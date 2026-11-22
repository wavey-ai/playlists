use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ResolvedStream {
    registry_id: u64,
    stream_id: u64,
    index: usize,
    generation: u64,
}

impl ResolvedStream {
    pub(crate) fn stream_id(self) -> u64 {
        self.stream_id
    }

    pub(crate) fn index(self) -> usize {
        self.index
    }

    pub(crate) fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug)]
struct Owner {
    stream_id: Option<u64>,
    generation: u64,
}

struct OwnerCell {
    accepting: AtomicBool,
    published_stream_id: AtomicU64,
    published_generation: AtomicU64,
    owner: RwLock<Owner>,
}

/// Index assignment state, guarded by the lifecycle lock.
///
/// A new stream takes a free index first. Only when every index is in use
/// does it replace the stream at `next_index`, in turn. A cache that adds and
/// releases a stream per reader must not evict a stream that still has
/// readers while indices are free. Both paths are O(1).
#[derive(Debug)]
struct Lifecycle {
    next_index: usize,
    free: VecDeque<usize>,
    is_free: Vec<bool>,
}

impl Lifecycle {
    fn new(capacity: usize) -> Self {
        Self {
            next_index: 0,
            free: (0..capacity).collect(),
            is_free: vec![true; capacity],
        }
    }

    fn take_index(&mut self, capacity: usize) -> usize {
        if let Some(index) = self.free.pop_front() {
            self.is_free[index] = false;
            return index;
        }
        let index = self.next_index;
        self.next_index = (index + 1) % capacity;
        index
    }

    fn release_index(&mut self, index: usize) {
        if !self.is_free[index] {
            self.is_free[index] = true;
            self.free.push_back(index);
        }
    }
}

/// Fixed-capacity logical-stream registry.
///
/// Map entries contain both the physical index and its generation. A resolve
/// operation therefore cannot combine an old stream ID with a new generation.
/// Repeated handle operations validate against per-index owner state and do
/// not acquire the global map lock.
pub(crate) struct StreamRegistry {
    registry_id: u64,
    mappings: RwLock<HashMap<u64, ResolvedStream>>,
    owners: Vec<OwnerCell>,
    lifecycle: Mutex<Lifecycle>,
}

impl StreamRegistry {
    pub(crate) fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        let mut registry_id = NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed);
        if registry_id == 0 {
            registry_id = NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed);
        }

        Self {
            registry_id,
            mappings: RwLock::new(HashMap::with_capacity(capacity)),
            owners: (0..capacity)
                .map(|_| OwnerCell {
                    accepting: AtomicBool::new(false),
                    published_stream_id: AtomicU64::new(0),
                    published_generation: AtomicU64::new(0),
                    owner: RwLock::new(Owner {
                        stream_id: None,
                        generation: 1,
                    }),
                })
                .collect(),
            lifecycle: Mutex::new(Lifecycle::new(capacity)),
        }
    }

    pub(crate) fn resolve(&self, stream_id: u64) -> Option<ResolvedStream> {
        let resolved = self
            .mappings
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&stream_id)
            .copied()?;
        self.is_current_fast(resolved).then_some(resolved)
    }

    pub(crate) fn resolve_or_create(
        &self,
        stream_id: u64,
        reset: impl FnOnce(usize, u64),
    ) -> ResolvedStream {
        if let Some(resolved) = self.resolve(stream_id) {
            return resolved;
        }

        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(resolved) = self.resolve(stream_id) {
            return resolved;
        }

        let index = lifecycle.take_index(self.owners.len());

        let cell = &self.owners[index];
        cell.accepting.store(false, Ordering::Release);
        cell.published_generation.store(0, Ordering::Release);
        // Seqlock write side: a reader that sees the new stream ID also sees
        // the zero generation stored above (see `is_current_fast`).
        fence(Ordering::Release);

        let previous_stream_id = cell
            .owner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stream_id;
        if let Some(previous_stream_id) = previous_stream_id {
            let mut mappings = self
                .mappings
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if mappings
                .get(&previous_stream_id)
                .is_some_and(|entry| entry.index == index)
            {
                mappings.remove(&previous_stream_id);
            }
        }

        let mut owner = cell
            .owner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owner.generation = next_generation(owner.generation);
        owner.stream_id = Some(stream_id);
        let resolved = ResolvedStream {
            registry_id: self.registry_id,
            stream_id,
            index,
            generation: owner.generation,
        };
        reset(index, owner.generation);

        // Publish per-index ownership before the forward mapping. A handle can
        // only become visible after all cache-specific state is reset.
        cell.published_stream_id.store(stream_id, Ordering::Relaxed);
        cell.published_generation
            .store(owner.generation, Ordering::Release);
        cell.accepting.store(true, Ordering::Release);
        self.mappings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(stream_id, resolved);
        resolved
    }

    /// Remove a stream and return the identity it had, so that a cache can
    /// release exactly that generation's state. The index becomes free.
    pub(crate) fn remove_stream(
        &self,
        stream_id: u64,
        reset: impl FnOnce(usize, u64),
    ) -> Option<ResolvedStream> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let resolved = self
            .mappings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&stream_id)?;

        let cell = &self.owners[resolved.index];
        // Ownership only changes under the lifecycle lock, so this check
        // cannot race with an assignment. Check before unpublishing so that a
        // mismatch never closes another stream's gate.
        {
            let owner = cell
                .owner
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if owner.stream_id != Some(stream_id) || owner.generation != resolved.generation {
                return None;
            }
        }
        cell.accepting.store(false, Ordering::Release);
        cell.published_generation.store(0, Ordering::Release);
        let mut owner = cell
            .owner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owner.stream_id = None;
        owner.generation = next_generation(owner.generation);
        reset(resolved.index, owner.generation);
        drop(owner);
        lifecycle.release_index(resolved.index);
        Some(resolved)
    }

    pub(crate) fn reset_index(&self, index: usize, reset: impl FnOnce(usize, u64)) -> bool {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(cell) = self.owners.get(index) else {
            return false;
        };
        cell.accepting.store(false, Ordering::Release);
        cell.published_generation.store(0, Ordering::Release);

        let previous_stream_id = cell
            .owner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stream_id;
        if let Some(previous_stream_id) = previous_stream_id {
            let mut mappings = self
                .mappings
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if mappings
                .get(&previous_stream_id)
                .is_some_and(|entry| entry.index == index)
            {
                mappings.remove(&previous_stream_id);
            }
        }

        let mut owner = cell
            .owner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owner.stream_id = None;
        owner.generation = next_generation(owner.generation);
        reset(index, owner.generation);
        drop(owner);
        lifecycle.release_index(index);
        true
    }

    pub(crate) fn with_validated<R>(
        &self,
        resolved: ResolvedStream,
        operation: impl FnOnce() -> R,
    ) -> Option<R> {
        if resolved.registry_id != self.registry_id {
            return None;
        }
        let cell = self.owners.get(resolved.index)?;
        if !cell.accepting.load(Ordering::Acquire) {
            return None;
        }
        let owner = cell
            .owner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !cell.accepting.load(Ordering::Acquire)
            || owner.stream_id != Some(resolved.stream_id)
            || owner.generation != resolved.generation
        {
            return None;
        }
        Some(operation())
    }

    pub(crate) fn with_generation<R>(
        &self,
        index: usize,
        generation: u64,
        operation: impl FnOnce() -> R,
    ) -> Option<R> {
        let cell = self.owners.get(index)?;
        let owner = cell
            .owner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (owner.stream_id.is_none() && owner.generation == generation).then(operation)
    }

    pub(crate) fn generation(&self, index: usize) -> Option<u64> {
        self.owners.get(index).map(|cell| {
            cell.owner
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .generation
        })
    }

    /// Validate a read-only operation without acquiring the owner lock.
    ///
    /// Assignment publishes generation zero before cache-specific reset and
    /// publishes the new stream ID before the new generation. Reading the
    /// generation twice therefore provides a small seqlock for read paths.
    pub(crate) fn is_current_fast(&self, resolved: ResolvedStream) -> bool {
        if resolved.registry_id != self.registry_id {
            return false;
        }
        let Some(cell) = self.owners.get(resolved.index) else {
            return false;
        };
        let first_generation = cell.published_generation.load(Ordering::Acquire);
        if first_generation != resolved.generation {
            return false;
        }
        let stream_id = cell.published_stream_id.load(Ordering::Relaxed);
        fence(Ordering::Acquire);
        let second_generation = cell.published_generation.load(Ordering::Relaxed);
        first_generation == second_generation && stream_id == resolved.stream_id
    }

    /// The identity that currently owns `index`, without taking a lock.
    ///
    /// Uses the same two-load check as [`Self::is_current_fast`]. Returns
    /// `None` for a free index and while an assignment or removal is not yet
    /// published.
    pub(crate) fn handle_at(&self, index: usize) -> Option<ResolvedStream> {
        let cell = self.owners.get(index)?;
        if !cell.accepting.load(Ordering::Acquire) {
            return None;
        }
        let generation = cell.published_generation.load(Ordering::Acquire);
        if generation == 0 {
            return None;
        }
        let stream_id = cell.published_stream_id.load(Ordering::Relaxed);
        fence(Ordering::Acquire);
        if cell.published_generation.load(Ordering::Relaxed) != generation {
            return None;
        }
        Some(ResolvedStream {
            registry_id: self.registry_id,
            stream_id,
            index,
            generation,
        })
    }

    pub(crate) fn streams(&self) -> Vec<ResolvedStream> {
        self.mappings
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .copied()
            .filter(|resolved| self.is_current_fast(*resolved))
            .collect()
    }
}

fn next_generation(generation: u64) -> u64 {
    let next = generation.wrapping_add(1);
    if next == 0 {
        1
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_returns_one_atomic_stream_identity() {
        let registry = StreamRegistry::new(1);
        let first = registry.resolve_or_create(11, |_, _| {});
        assert!(registry.is_current_fast(first));

        let second = registry.resolve_or_create(12, |_, _| {});
        assert!(!registry.is_current_fast(first));
        assert!(registry.is_current_fast(second));
        assert_eq!(registry.resolve(11), None);
        assert_eq!(registry.resolve(12), Some(second));
    }

    #[test]
    fn handles_are_bound_to_their_registry() {
        let first = StreamRegistry::new(1);
        let second = StreamRegistry::new(1);
        let handle = first.resolve_or_create(7, |_, _| {});
        let _ = second.resolve_or_create(7, |_, _| {});

        assert!(!second.is_current_fast(handle));
    }

    #[test]
    fn remove_and_recreate_advances_generation() {
        let registry = StreamRegistry::new(1);
        let first = registry.resolve_or_create(4, |_, _| {});
        assert_eq!(registry.remove_stream(4, |_, _| {}), Some(first));
        assert_eq!(registry.remove_stream(4, |_, _| {}), None);
        let second = registry.resolve_or_create(4, |_, _| {});

        assert_ne!(first.generation(), second.generation());
        assert!(!registry.is_current_fast(first));
        assert!(registry.is_current_fast(second));
    }

    #[test]
    fn resolve_rejects_an_identity_while_reassignment_is_unpublished() {
        let registry = StreamRegistry::new(1);
        let first = registry.resolve_or_create(4, |_, _| {});
        let cell = &registry.owners[first.index()];

        cell.accepting.store(false, Ordering::Release);
        cell.published_generation.store(0, Ordering::Release);

        assert_eq!(registry.resolve(4), None);
        assert!(registry.streams().is_empty());
    }

    #[test]
    fn a_free_index_is_taken_before_any_stream_is_evicted() {
        let registry = StreamRegistry::new(4);
        let kept = registry.resolve_or_create(7, |_, _| {});
        for stream_id in 100..140_u64 {
            let visitor = registry.resolve_or_create(stream_id, |_, _| {});
            assert_ne!(visitor.index(), kept.index());
            assert_eq!(registry.remove_stream(stream_id, |_, _| {}), Some(visitor));
            assert_eq!(
                registry.resolve(7),
                Some(kept),
                "stream 7 was evicted when stream {stream_id} was added"
            );
        }

        // Filling the free indices evicts nothing.
        let filled: Vec<_> = [8_u64, 9, 10]
            .into_iter()
            .map(|stream_id| registry.resolve_or_create(stream_id, |_, _| {}))
            .collect();
        let mut indices: Vec<_> = filled.iter().map(|resolved| resolved.index()).collect();
        indices.push(kept.index());
        indices.sort_unstable();
        assert_eq!(indices, vec![0, 1, 2, 3]);
        assert!(registry.is_current_fast(kept));
        assert!(filled
            .iter()
            .all(|resolved| registry.is_current_fast(*resolved)));

        // With every index in use a new stream replaces one in turn.
        let replacing = registry.resolve_or_create(11, |_, _| {});
        assert_eq!(registry.streams().len(), 4);
        assert!(registry.is_current_fast(replacing));

        // An index reset by number is free again, and taken before eviction.
        let reset = registry.handle_at(1).unwrap();
        assert!(registry.reset_index(1, |_, _| {}));
        assert!(registry.reset_index(1, |_, _| {}));
        assert!(!registry.is_current_fast(reset));
        let refilled = registry.resolve_or_create(12, |_, _| {});
        assert_eq!(refilled.index(), 1);
        assert_eq!(registry.streams().len(), 4);
    }

    #[test]
    fn handle_at_is_none_while_reassignment_is_unpublished() {
        let registry = StreamRegistry::new(2);
        assert_eq!(registry.handle_at(0), None);
        assert_eq!(registry.handle_at(2), None);

        let first = registry.resolve_or_create(4, |_, _| {});
        assert_eq!(registry.handle_at(first.index()), Some(first));

        let cell = &registry.owners[first.index()];
        cell.accepting.store(false, Ordering::Release);
        cell.published_generation.store(0, Ordering::Release);
        assert_eq!(registry.handle_at(first.index()), None);

        cell.published_generation
            .store(first.generation(), Ordering::Release);
        cell.accepting.store(true, Ordering::Release);
        assert_eq!(registry.handle_at(first.index()), Some(first));

        assert_eq!(registry.remove_stream(4, |_, _| {}), Some(first));
        assert_eq!(registry.handle_at(first.index()), None);
    }
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

#[derive(Debug)]
struct Lifecycle {
    next_index: usize,
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
            lifecycle: Mutex::new(Lifecycle { next_index: 0 }),
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

        let index = lifecycle.next_index;
        lifecycle.next_index = lifecycle.next_index.checked_add(1).unwrap_or(0) % self.owners.len();

        let cell = &self.owners[index];
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

    pub(crate) fn remove_stream(&self, stream_id: u64, reset: impl FnOnce(usize, u64)) -> bool {
        let _lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let resolved = self
            .mappings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&stream_id);
        let Some(resolved) = resolved else {
            return false;
        };

        let cell = &self.owners[resolved.index];
        cell.accepting.store(false, Ordering::Release);
        cell.published_generation.store(0, Ordering::Release);
        let mut owner = cell
            .owner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if owner.stream_id != Some(stream_id) || owner.generation != resolved.generation {
            return false;
        }
        owner.stream_id = None;
        owner.generation = next_generation(owner.generation);
        reset(resolved.index, owner.generation);
        true
    }

    pub(crate) fn reset_index(&self, index: usize, reset: impl FnOnce(usize, u64)) -> bool {
        let _lifecycle = self
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
        let second_generation = cell.published_generation.load(Ordering::Acquire);
        first_generation == second_generation && stream_id == resolved.stream_id
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
        assert!(registry.remove_stream(4, |_, _| {}));
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
}

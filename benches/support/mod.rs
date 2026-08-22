use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct CountingAllocator;

const COUNTER_SHARDS: usize = 64;

struct CounterShard {
    allocation_calls: AtomicU64,
    reallocation_calls: AtomicU64,
    deallocation_calls: AtomicU64,
    live_bytes: AtomicU64,
    peak_live_bytes: AtomicU64,
}

impl CounterShard {
    const fn new() -> Self {
        Self {
            allocation_calls: AtomicU64::new(0),
            reallocation_calls: AtomicU64::new(0),
            deallocation_calls: AtomicU64::new(0),
            live_bytes: AtomicU64::new(0),
            peak_live_bytes: AtomicU64::new(0),
        }
    }

    fn add_live_bytes(&self, bytes: usize) {
        let current = self
            .live_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed)
            .saturating_add(bytes as u64);
        let _ = self
            .peak_live_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |peak| {
                (current > peak).then_some(current)
            });
    }
}

static SHARDS: [CounterShard; COUNTER_SHARDS] = [const { CounterShard::new() }; COUNTER_SHARDS];

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies the layout required by GlobalAlloc.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let shard = counter_shard(pointer);
            shard.allocation_calls.fetch_add(1, Ordering::Relaxed);
            shard.add_live_bytes(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies the layout required by GlobalAlloc.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            let shard = counter_shard(pointer);
            shard.allocation_calls.fetch_add(1, Ordering::Relaxed);
            shard.add_live_bytes(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let shard = counter_shard(pointer);
        shard.deallocation_calls.fetch_add(1, Ordering::Relaxed);
        shard
            .live_bytes
            .fetch_sub(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: The caller supplies the pointer and layout required by
        // GlobalAlloc.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller supplies the pointer and layout required by
        // GlobalAlloc.
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            let old_shard = counter_shard(pointer);
            old_shard
                .live_bytes
                .fetch_sub(layout.size() as u64, Ordering::Relaxed);
            let new_shard = counter_shard(new_pointer);
            new_shard.reallocation_calls.fetch_add(1, Ordering::Relaxed);
            new_shard.add_live_bytes(new_size);
        }
        new_pointer
    }
}

fn counter_shard(pointer: *mut u8) -> &'static CounterShard {
    debug_assert!(COUNTER_SHARDS.is_power_of_two());
    let address = pointer as usize;
    let mixed = address ^ address.rotate_right(17) ^ address.rotate_right(31);
    &SHARDS[mixed & (COUNTER_SHARDS - 1)]
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocationSnapshot {
    pub allocation_calls: u64,
    pub reallocation_calls: u64,
    pub deallocation_calls: u64,
    pub live_bytes: u64,
    pub peak_live_bytes: u64,
}

impl AllocationSnapshot {
    pub fn since(self, before: Self) -> Self {
        Self {
            allocation_calls: self
                .allocation_calls
                .saturating_sub(before.allocation_calls),
            reallocation_calls: self
                .reallocation_calls
                .saturating_sub(before.reallocation_calls),
            deallocation_calls: self
                .deallocation_calls
                .saturating_sub(before.deallocation_calls),
            live_bytes: self.live_bytes,
            peak_live_bytes: self.peak_live_bytes,
        }
    }
}

pub fn allocation_snapshot() -> AllocationSnapshot {
    SHARDS
        .iter()
        .fold(AllocationSnapshot::default(), |mut snapshot, shard| {
            snapshot.allocation_calls = snapshot
                .allocation_calls
                .saturating_add(shard.allocation_calls.load(Ordering::Relaxed));
            snapshot.reallocation_calls = snapshot
                .reallocation_calls
                .saturating_add(shard.reallocation_calls.load(Ordering::Relaxed));
            snapshot.deallocation_calls = snapshot
                .deallocation_calls
                .saturating_add(shard.deallocation_calls.load(Ordering::Relaxed));
            snapshot.live_bytes = snapshot
                .live_bytes
                .saturating_add(shard.live_bytes.load(Ordering::Relaxed));
            snapshot.peak_live_bytes = snapshot
                .peak_live_bytes
                .saturating_add(shard.peak_live_bytes.load(Ordering::Relaxed));
            snapshot
        })
}

pub fn reset_peak_live_bytes() {
    for shard in &SHARDS {
        shard
            .peak_live_bytes
            .store(shard.live_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

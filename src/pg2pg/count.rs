use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct Counter {
    inner: AtomicU64,
}

impl Counter {
    pub fn inc(&self) {
        self.add(1)
    }

    pub fn add(&self, val: u64) {
        self.inner.fetch_add(val, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.inner.load(Ordering::Relaxed)
    }
}

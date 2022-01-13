use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

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

    pub(crate) fn time<T>(&self, func: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let response = func();
        self.add(u64::try_from(start.elapsed().as_micros()).expect("2^64 micros is 500,000 years"));
        response
    }
}

use std::panic::{catch_unwind, UnwindSafe};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use threadpool::ThreadPool;

pub struct JoinHandle {
    pool: ThreadPool,
    rx: Receiver<Result<()>>,
}

pub fn run_all<F>(jobs: impl IntoIterator<Item = F>) -> Result<JoinHandle>
where
    F: Send + 'static + UnwindSafe + FnOnce() -> Result<()>,
{
    let pool = ThreadPool::new(64);
    let (tx, rx) = channel();
    for job in jobs {
        let tx = tx.clone();
        pool.execute(move || {
            let maybe_panic = match catch_unwind(move || job()) {
                Ok(user) => user,
                Err(err) => Err(anyhow!("panic! {:?}", err)),
            };
            tx.send(maybe_panic).expect("main thread has gone away");
        });
    }

    Ok(JoinHandle { rx, pool })
}

impl JoinHandle {
    pub fn still_running(&self, timeout: Duration) -> Result<bool> {
        // drain any results
        while let Ok(result) = self.rx.try_recv() {
            result?;
        }

        // sleep, or get lucky
        match self.rx.recv_timeout(timeout) {
            Ok(result) => result?,

            // if it's disconnected, we're probably in an error state (all threads are gone?)
            // but we'll catch that with the next checks anyway
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => (),
        }

        if self.pool.panic_count() != 0 {
            bail!("some un-caught panic happened on a thread (this shouldn't be possible)");
        }

        let active = self.pool.active_count() > 0 || self.pool.queued_count() > 0;

        Ok(active)
    }
}

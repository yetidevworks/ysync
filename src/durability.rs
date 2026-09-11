//! Bounded disk flush concurrency, independent of CPU-bound scan workers.
use anyhow::{Result, anyhow};
use rayon::prelude::*;
use std::sync::OnceLock;

pub fn parallel<T: Sync>(
    items: &[T],
    flush: impl Fn(&T) -> Result<()> + Sync + Send,
) -> Result<()> {
    if items.len() <= 1 {
        return items.iter().try_for_each(flush);
    }
    static POOL: OnceLock<Result<rayon::ThreadPool, rayon::ThreadPoolBuildError>> = OnceLock::new();
    let pool = POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .thread_name(|n| format!("ysync-flush-{n}"))
            .build()
    });
    let pool = pool
        .as_ref()
        .map_err(|e| anyhow!("starting disk flush workers: {e}"))?;
    // install does not return until the parallel work has joined, including on failure.
    pool.install(|| items.par_iter().try_for_each(flush))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn failed_flush_joins_other_in_flight_flushes_before_returning() {
        let barrier = Barrier::new(8);
        let finished = AtomicUsize::new(0);
        let error = parallel(&(0..8).collect::<Vec<_>>(), |n| {
            barrier.wait();
            if *n == 0 {
                return Err(std::io::Error::other("injected disk flush failure").into());
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
            finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected disk flush failure"));
        assert_eq!(finished.load(Ordering::SeqCst), 7);
    }
}

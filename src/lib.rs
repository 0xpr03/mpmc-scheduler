//! # mpmc-scheduler
//!
//! A Fair, Per-Channel Cancellable, multi-mpmc task scheduler running on top of tokio.
//!
//! It bundles together multiple mpmc channels and schedules incoming work with fair rate limiting among the allowed maximum of workers.
//!
//! ## Example
//!
//! ```rust
//! use mpmc_scheduler;
//! use tokio::runtime::Runtime;
//! use futures::future::Future;
//!
//! let (controller, scheduler) = mpmc_scheduler::Scheduler::new(
//!     4,
//!     |v| {
//!         println!("Processing {}", v);
//!         v
//!     },
//!     Some(|r| println!("Finalizing {}", r)),
//!     true
//! );
//!
//! let mut runtime = Runtime::new().unwrap();
//!
//! let tx = controller.channel(1,4);
//!
//! runtime.spawn(scheduler);
//!
//! for i in 0..4 {
//!     tx.try_send(i);
//! }
//!
//! drop(tx); // drop tx so scheduler & runtime shut down
//!
//! runtime.shutdown_on_idle().wait().unwrap();
//! ```
//!
//! ## Details
//!
//! You can think of it as a round-robin scheduler for rate limited workers which always run the same function.
//!
//! ```text
//! o-                  -x
//!   \                /
//! o--|--Scheduler --|--x
//!   /                \
//! o-                  -x
//! ```
//!
//! In this image we have an n amount of Producers `o` and m amount of Workers `x`
//! We want to handle all incoming work from `o` in a fair manner. Such that if
//! one producers has 20 jobs and another 2, both are going to get handled equally in a round robin manner.
//!
//! Each channel queue can be cleared such that all to-be-scheduled jobs are droppped.  
//! To allow also stopping currently running (extensive) options, operation can be split into two functions.  
//! For example of http requests whose result is stored. If we abort before the store operation we can prevent all outstanding  
//! worker operations of one channel plus the remaining jobs.  
//!
//! Closed channels are detected and removed from the scheduler when iterating.
//! You can manually trigger a schedule tick by calling `gc` on the controller.
//!
//! ## Performance
//!
//! Arcane Magic benchmarks result in 56ms/job on a i7-6700HQ with 1 million jobs, 8 parallel producing channels & 8 Workers, 1024 bound per channel.
//! Note that at most two roundtrips per schedule interval are done (so at most 16 jobs scheduled per interval).
//! This means that above numbers include iteration & polling start-stop fees.
//!
//! ## Limitations
//! - mpmc-scheduler can only be used with its own Producer channels due to missing traits for other channels. futures mpsc also doesn't work as they are not waking up the scheduler.
//!
//! - The channel bound has to be a power of two!
//!
//! - You can only define one work-handler function per `Scheduler` and it cannot be changed afterwards.
//!
use bus::Bus;
use futures::task::Task;
use futures::{task, Async, Future, Poll};
use npnc::bounded::mpmc;
use npnc::{ConsumeError, ProduceError};

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::TrySendError;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

type TaskStore = Arc<RwLock<Option<Task>>>;
type FutItem = ();
type FutError = ();

macro_rules! lock_c {
    ($x:expr) => {
        $x.lock().expect("Can't access channels!")
    };
}

/// Sender/Producer for one channel
#[derive(Clone)]
pub struct Sender<V> {
    queue: Arc<mpmc::Producer<V>>,
    task: TaskStore,
}

unsafe impl<V> Send for Sender<V> where V: Send {}

unsafe impl<V> Sync for Sender<V> {}

impl<V> Sender<V> {
    /// Try sending a new job
    /// Doesn't block in best-normal case, but can block for a guaranteed short amount.
    pub fn try_send(&self, value: V) -> Result<(), TrySendError<V>> {
        match self.queue.produce(value) {
            Err(ProduceError::Disconnected(v)) => return Err(TrySendError::Disconnected(v)),
            Err(ProduceError::Full(v)) => return Err(TrySendError::Full(v)),
            _ => (),
        }
        let task_l = self.task.read().expect("Can't lock task!");
        if let Some(task) = task_l.as_ref() {
            task.notify();
        }
        Ok(())
    }
}

/// Inner scheduler, shared accross controller & worker
#[doc(hidden)]
struct SchedulerInner<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Send + Sync + 'static,
    R: 'static,
{
    position: AtomicUsize,
    // TODO: evaluate concurrent_hashmap
    channels: Arc<Mutex<HashMap<K, Channel<V>>>>,
    task: TaskStore,
    workers_active: Arc<AtomicUsize>,
    max_worker: usize,
    worker_fn: Arc<Box<dyn Fn(V) -> R + Send + Sync + 'static>>,
    worker_fn_finalize: Arc<Option<Box<dyn Fn(R) + Send + Sync + 'static>>>,
    exit_on_idle: bool,
}

/// One Channel conisting of the receiver site and the cancel bus to
/// stop running jobs
#[doc(hidden)]
struct Channel<V> {
    recv: mpmc::Consumer<V>,
    cancel_bus: Bus<()>,
}

impl<K, V, R> SchedulerInner<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + 'static,
    R: 'static,
{
    pub fn new(
        max_worker: usize,
        worker_fn: Box<dyn Fn(V) -> R + Send + Sync + 'static>,
        worker_fn_finalize: Option<Box<dyn Fn(R) + Send + Sync + 'static>>,
        exit_on_idle: bool,
    ) -> SchedulerInner<K, V, R> {
        SchedulerInner {
            position: AtomicUsize::new(0),
            channels: Arc::new(Mutex::new(HashMap::new())),
            workers_active: Arc::new(AtomicUsize::new(0)),
            max_worker,
            task: Arc::new(RwLock::new(None)),
            worker_fn: Arc::new(worker_fn),
            worker_fn_finalize: Arc::new(
                worker_fn_finalize as Option<Box<dyn Fn(R) + Send + Sync + 'static>>,
            ),
            exit_on_idle,
        }
    }

    /// Trigger polling wakeup, used by GC call
    fn schedule(&self) {
        let task_l = self.task.read().expect("Can't lock task!");
        if let Some(task) = task_l.as_ref() {
            task.notify();
        }
    }

    /// Clear queue for specific channel & cancel workers
    pub fn cancel_channel(&self, key: &K) -> Result<(), ()> {
        let mut map_l = lock_c!(self.channels);
        //TODO: handle missing queue
        if let Some(channel) = map_l.get_mut(key) {
            // if we're not able to send a cancel command then probably
            // no work is running and/or a stop command was already send
            let _ = channel.cancel_bus.try_broadcast(());
            loop {
                match channel.recv.consume() {
                    Ok(_) => (), // more messages
                    _ => return Ok(()),
                }
            }
        } else {
            Err(())
        }
    }

    /// Create channel
    pub fn create_channel(&self, key: K, bound: usize) -> Sender<V> {
        let mut map_l = lock_c!(self.channels);

        let (tx, rx) = mpmc::channel(bound);
        map_l.insert(
            key,
            Channel {
                recv: rx,
                cancel_bus: Bus::new(1),
            },
        );
        Sender {
            queue: Arc::new(tx),
            task: self.task.clone(),
        }
    }

    /// Inner poll method, only to be called by future handler
    fn poll(&self) -> Poll<FutItem, FutError> {
        let mut map_l = lock_c!(self.channels);
        if map_l.len() < self.position.load(Ordering::Relaxed) {
            self.position.store(0, Ordering::Relaxed);
        }

        let start_pos = self.position.load(Ordering::Relaxed);
        let mut pos = 0;

        let mut worker_counter = 0;
        let mut roundtrip = 0;
        let mut no_work = true;
        let mut idle = false;

        while self.workers_active.load(Ordering::Relaxed) < self.max_worker && !idle {
            map_l.retain(|_, channel| {
                // skip to postion from last poll
                if roundtrip == 0 && pos < start_pos {
                    return true;
                }
                let mut connected = true;
                match channel.recv.consume() {
                    Ok(w) => {
                        no_work = false;
                        self.workers_active.fetch_add(1, Ordering::SeqCst);
                        worker_counter += 1;
                        let worker_c = self.workers_active.clone();
                        let task = task::current();
                        let work_fn = self.worker_fn.clone();
                        let work_fn_final = self.worker_fn_finalize.clone();
                        let mut cancel_recv = channel.cancel_bus.add_rx();
                        thread::spawn(move || {
                            let result: R = work_fn(w);
                            if cancel_recv.try_recv().is_err() {
                                if let Some(finalizer) = work_fn_final.as_ref() {
                                    finalizer(result);
                                }
                            }
                            worker_c.fetch_sub(1, Ordering::SeqCst);
                            task.notify();
                        });
                    }
                    Err(ConsumeError::Empty) => (),
                    Err(ConsumeError::Disconnected) => connected = false,
                }
                pos += 1;
                connected
            });
            pos = 0;

            if no_work && roundtrip >= 1 {
                idle = true;
            }
            roundtrip += 1;

            no_work = true;
        }
        let mut task_l = self.task.write().expect("Can't lock task!");
        *task_l = Some(task::current());
        drop(task_l);
        self.position.store(pos, Ordering::Relaxed);
        if self.exit_on_idle && map_l.len() == 0 && self.workers_active.load(Ordering::Relaxed) == 0
        {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

/// The Controller is a non-producing handle to the scheduler.
/// It allows creation of new channels as well as clearing of queues.
#[derive(Clone)]
pub struct Controller<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + 'static,
    R: 'static,
{
    inner: Arc<SchedulerInner<K, V, R>>,
}

impl<K, V, R> Controller<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + 'static,
    R: 'static,
{
    /// Create a new channel, returns the producer site.
    /// See below for bound.
    /// May block if clearing or scheduling tick is currently running.
    ///
    /// ## Bound
    /// Is the next power of two for the handed value
    pub fn channel(&self, key: K, bound: usize) -> Sender<V> {
        self.inner.create_channel(key, bound.next_power_of_two())
    }

    /// Clear queue for specific channel & running jobs if supported.
    ///
    /// May block if `channel` is called or a schedule is running.
    /// Note that for a queue with bounds n, it has a O(n) worst case complexity.
    ///
    /// Returns Err if the specified channel is invalid.
    pub fn cancel_channel(&self, key: &K) -> Result<(), ()> {
        self.inner.cancel_channel(key)
    }

    /// Manually trigger schedule. Normaly not required but if you should drop a lot of channels and
    /// don't insert/complete a job in the next time, you may call this.
    pub fn gc(&self) {
        self.inner.schedule();
    }
}

// no clone, don't allow for things such as 2x spawn()
/// Scheduler
pub struct Scheduler<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + 'static,
    R: 'static,
{
    inner: Arc<SchedulerInner<K, V, R>>,
}

impl<K, V, R> Scheduler<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + 'static,
    R: 'static,
{
    /// Create a new scheduler with specified amount of max workers.
    /// max_worker: specifies the amount of workers to be used
    /// * `worker_fn` - the function to execute that handles the "main" work
    /// * `worker_fn_finialize` - the "finish" function which is not called on job cancel
    /// * `finish_on_idle` - on true if no channels are left on the next schedule the scheduler will drop from the tokio Runtime
    ///
    /// You should create at least one channel before spawning the scheduler on the runtime when set to true.
    pub fn new(
        max_worker: usize,
        worker_fn: impl Fn(V) -> R + Send + Sync + 'static,
        worker_fn_finalize: Option<impl Fn(R) + Send + Sync + 'static>,
        finish_on_idle: bool,
    ) -> (Controller<K, V, R>, Scheduler<K, V, R>) {
        let inner = Arc::new(SchedulerInner::new(
            max_worker,
            Box::new(worker_fn) as Box<dyn Fn(V) -> R + Send + Sync + 'static>,
            worker_fn_finalize.map(|v| Box::new(v) as Box<dyn Fn(R) + Send + Sync + 'static>),
            finish_on_idle,
        ));
        (
            Controller {
                inner: inner.clone(),
            },
            Scheduler { inner },
        )
    }
}

impl<K, V, R> Future for Scheduler<K, V, R>
where
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + 'static,
    R: 'static,
{
    // The stream will never yield an error
    type Error = FutError;
    type Item = FutItem;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

#[cfg(test)]
mod tests {

    use super::Scheduler;
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};
    use tokio::runtime::Runtime;

    fn run_mpmc(producers: usize, amount: usize, workers: usize, channel_size: usize) {
        let collector = Arc::new(Mutex::new(Vec::new()));
        let collectorc = collector.clone();
        let (controller, scheduler) = Scheduler::new(
            workers,
            |v| {
                //println!("Seen {}", v);
                v
            },
            Some(move |v| {
                let mut lock = collectorc.lock().unwrap();
                lock.push(v);
            }),
            true,
        );
        let mut runtime = Runtime::new().unwrap();
        let tx = controller.channel(1, channel_size);
        runtime.spawn(scheduler);
        for t in 0..producers {
            let txc = controller.channel(t, channel_size);
            let start = t * (amount / producers);
            let mut end = start + (amount / producers);
            if t == producers - 1 {
                end = amount;
            }
            println!("start: {} end {}", start, end);
            thread::spawn(move || {
                for i in start..end {
                    loop {
                        if txc.try_send(i).is_ok() {
                            break;
                        }
                        thread::sleep(Duration::from_micros(10))
                    }
                }
                drop(txc);
                println!("{} finished insertion", t);
            });
        }
        drop(tx);
        runtime.shutdown_on_idle().wait().unwrap();

        let lock = collector.lock().unwrap();
        println!(
            "Verifying for {} inserter {} workers {} amount",
            producers, workers, amount
        );
        assert_eq!(amount, lock.len());
        for i in 0..amount {
            assert!(lock.contains(&i));
        }
    }

    #[test]
    #[ignore]
    fn bench_mpsw() {
        let producers = 8;
        let amount = 1_000_000;
        let workers = 8;
        let channel_size = 1024;
        let (controller, scheduler) = Scheduler::new(
            workers,
            |v| {
                //println!("Seen {}", v);
                v
            },
            Some(move |v| {
                //assert!(v >= 0);
            }),
            true,
        );
        let mut runtime = Runtime::new().unwrap();
        let tx = controller.channel(1, channel_size);
        runtime.spawn(scheduler);
        let start = Instant::now();
        for t in 0..producers {
            let txc = controller.channel(t, channel_size);
            let start = t * (amount / producers);
            let mut end = start + (amount / producers);
            if t == producers - 1 {
                end = amount;
            }
            println!("start: {} end {}", start, end);
            thread::spawn(move || {
                for i in start..end {
                    loop {
                        if txc.try_send(i).is_ok() {
                            break;
                        }
                        thread::sleep(Duration::from_nanos(1));
                    }
                }
                drop(txc);
                println!("{} finished insertion", t);
            });
        }
        drop(tx);
        runtime.shutdown_on_idle().wait().unwrap();
        let end_d = start.elapsed();
        let end = end_d.subsec_millis() as u64 + (end_d.as_secs() * 1_000);
        println!(
            "Took {} ms for {} entries: {}ms per job",
            end,
            amount,
            amount / end
        );
    }

    #[test]
    fn verify_mpsw() {
        run_mpmc(8, 10_000, 4, 32);
    }

    #[test]
    fn verify_spsw() {
        run_mpmc(1, 1_000, 1, 32);
    }

    #[test]
    fn verify_spmw_overload() {
        for i in 2..30 {
            run_mpmc(1, 1_000, i, 2);
        }
    }

    #[test]
    fn verify_spmw() {
        for i in 2..30 {
            run_mpmc(1, 1_000, i, 1024);
        }
    }

    #[test]
    fn verify_spmw_underload() {
        for i in 2..30 {
            run_mpmc(1, 30, i, 1024);
        }
    }
}

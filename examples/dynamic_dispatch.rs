use tokio;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use mpmc_scheduler::Scheduler;

// demonstrate dynamic function dispatch of jobs

type WrappedJob = Box<dyn Job + 'static + Send + Sync>;

/// Trait for dynamic jobs
trait Job {
    fn blocking(&mut self) -> FinalizerData;
    fn finalize(&mut self, r: FinalizerData);
    /// Turn Job into wrapped to send to scheduler
    fn wrap(self) -> WrappedJob
    where
        Self: std::marker::Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }
}

/// Limitation in rust, we can't use a generic, the type here to be known at compile time.
/// We could use a type-erasure but this would make it hard to work pass to the finalizer.
enum FinalizerData {
    String(String),
    i32(i32),
}

/// First job type implementation
struct JobA;

impl Job for JobA {
    fn blocking(&mut self) -> FinalizerData {
        println!("Performing job A");
        FinalizerData::String(String::from("something to pass"))
    }

    fn finalize(&mut self, data: FinalizerData) {
        println!("Finalizing of job A");
        match data {
            FinalizerData::String(s) => println!("Finalizing {}", s),
            _ => unreachable!(),
        }
    }
}

/// Second job type implementation
struct JobB;

impl Job for JobB {
    fn blocking(&mut self) -> FinalizerData {
        println!("Performing job B");
        FinalizerData::i32(42)
    }

    fn finalize(&mut self, data: FinalizerData) {
        println!("Finalizing of job B");
        match data {
            FinalizerData::i32(v) => println!("Finalizing {}", v),
            _ => unreachable!(),
        }
    }
}

fn main() {
    let workers = 2;

    let mut rt = Runtime::new().unwrap();

    let (controller, scheduler) = Scheduler::new(
        workers,
        move |mut v: WrappedJob| {
            let result = v.blocking();
            (v, result)
        },
        Some(move |(mut v, r): (WrappedJob, FinalizerData)| {
            v.finalize(r);
        }),
        true,
    );

    let sender = controller.channel(1, 32);

    rt.spawn(scheduler);
    sender.try_send(JobA.wrap()).unwrap();
    sender.try_send(JobB.wrap()).unwrap();
    drop(sender);
    rt.shutdown_on_idle().wait().unwrap();
}

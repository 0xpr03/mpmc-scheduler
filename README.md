# mpmc-scheduler

[![crates.io](https://img.shields.io/crates/v/mpmc-scheduler.svg)](https://crates.io/crates/mpmc-scheduler)
[![docs.rs](https://docs.rs/mpmc-scheduler/badge.svg)](https://docs.rs/mpmc-scheduler)

A Fair, Per-Channel Cancellable, multi-mpmc task scheduler running on top of tokio.

It bundles together multiple mpmc channels and schedules incoming work with fair rate limiting among the allowed maximum of workers.

## Example

```rust
use mpmc_scheduler;
use tokio::runtime::Runtime;
use futures::future::Future;

let (controller, scheduler) = mpmc_scheduler::Scheduler::new(
    4,
    |v| {
        println!("Processing {}", v);
        v
    },
    Some(|r| println!("Finalizing {}", r)),
    true
);

let mut runtime = Runtime::new().unwrap();

let tx = controller.channel(1,4);

runtime.spawn(scheduler);

for i in 0..4 {
    tx.try_send(i);
}

drop(tx); // drop tx so scheduler & runtime shut down

runtime.shutdown_on_idle().wait().unwrap();
```

## Details

You can think of it as a round-robin scheduler for rate limited workers which always run the same function.

```text
o-                  -x
  \                /
o--|--Scheduler --|--x
  /                \
o-                  -x
```

In this image we have an n amount of Producers `o` and m amount of Workers `x`
We want to handle all incoming work from `o` in a fair manner. Such that if
one producers has 20 jobs and another 2, both are going to get handled equally in a round robin fashion.

Each channel queue can be cleared such that all to-be-scheduled jobs are droppped.  
To allow also stopping currently running (expensive) operations, these can be split into two sections (functions).
The `worker_fn` which can't be canceled and `worker_fn_finalize` which is not called if a job is marked as canceled.  
For example http requests whose result is stored into a database. If we abort before the store operation we can prevent all outstanding
worker operations of one channel plus the remaining jobs. We create fetch-http as the blocking and the db storing as the optional part.

Closed channels are detected and removed from the scheduler when iterating.
You can manually trigger a schedule tick by calling `gc` on the controller.

## Performance

If you have idle workers it takes ~ 1ms or less to process a job. Depending on your worker/producer ratio your mileage may vary.
For example with Arcane Magic benchmarks it results in 56ms/job on a i7-6700HQ with 1 million jobs, 8 parallel producing channels & 8 Workers, 1024 bound per channel.
Note that at most two roundtrips per schedule interval are done (so at most 16 jobs scheduled per interval) and we constantly have to re-send.
This means that above numbers include iteration & polling start-stop fees.

## Limitations
- mpmc-scheduler can only be used with its own Producer channels due to missing traits for other channels. futures mpsc also doesn't work as they are not waking up the scheduler.

- The channel bound has to be a power of two. 

- You can only define one work-handler function per `Scheduler` and it cannot be changed afterwards.

## Thanks

Thanks to udoprog for the help with reducing the amount of generics & making it possible to store Controller & Scheduler in a non-generic way.

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in mpmc-scheduler by you, shall be licensed as MIT, without any additional
terms or conditions.

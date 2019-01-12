# mpmc-scheduler

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
one producers has 20 jobs and another 2, both are going to get handled equally in a round robin manner.

Each channel queue can be cleared such that all to-be-scheduled jobs are droppped.  
To allow also stopping currently running (extensive) options, operation can be split into two functions.  
For example of http requests whose result is stored. If we abort before the store operation we can prevent all outstanding  
worker operations of one channel plus the remaining jobs.  

Closed channels are detected and removed from the scheduler when iterating.
You can manually trigger a schedule tick by calling `gc` on the controller.

## Limitations
- mpmc-scheduler can only be used with its own Producer channels due to missing traits for other channels. futures mpsc also doesn't work as they are not waking up the scheduler.

- The channel bound has to be a power of two. 

- You can only define one work-handler function per `Scheduler` and it cannot be changed afterwards.


## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in mpmc-scheduler by you, shall be licensed as MIT, without any additional
terms or conditions.

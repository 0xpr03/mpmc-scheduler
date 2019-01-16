use tokio;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use mpmc_scheduler::Scheduler;

use std::thread;
use std::time::Duration;

fn main() {
    println!("Hello, world!");
    let delay = 1;
    let workers = 4;
    println!(
        "Creating controller with delay of {}secs, {} parallel workers",
        delay, workers
    );
    // create scheduler
    let (controller, scheduler) = Scheduler::new(
        workers,
        move |v| {
            thread::sleep(Duration::from_secs(delay));
            println!("Processing {}", v);
            v
        },
        Some(|r| println!("Finalizing {}", r)),
        true,
    );

    // create two channels
    let tx_1 = controller.channel(1, 32);
    let tx_2 = controller.channel(2, 32);

    // create a manual tokio runtime, we don't want to block our thread
    let mut rt = Runtime::new().expect("Can't create tokio runtime!");
    rt.spawn(scheduler);
    let tx_1c = tx_1.clone();
    let tx_2c = tx_1.clone();
    let controllerc = controller.clone();
    thread::spawn(move || {
        // create some jobs
        #[allow(unused_must_use)]
        for i in 0..100 {
            println!("Inserting {}", i);
            tx_1c.try_send(format!("Msg1A{}", i));
            tx_2c.try_send(format!("Msg1B{}", i));
            // clear queue every 100th insert to stress it a little
            if i % 100 == 0 {
                println!("Clearing queue {}", i);
                controllerc.cancel_channel(&1);
                println!("Cleared {}", i);
            }
        }

        {
            // new scope, we drop tx_3 (and thus that channel) afterwards
            let tx_3 = controllerc.channel(3, 8);

            #[allow(unused_must_use)]
            for i in 0..100 {
                println!("Inserting {}", i);
                tx_3.try_send(format!("Msg3A{}", i));
                tx_3.try_send(format!("Msg3B{}", i));
                if i % 10 == 0 {
                    println!("Clearing queue {}", i);
                    controllerc.cancel_channel(&1);
                    println!("Cleared {}", i);
                }
            }
            drop(tx_3);
        }
    });

    let mut i = 0;
    let mut run = true;
    println!(
        "Waiting for input..\
    All Input expects a newline.
    e: Exit
    c: Cancel queue+jobs in 1
    1: Send on channel 1
    2: Send on Channel 2
    g: Manually trigger schedule
    t: Re-Create channel1 and drop it instantly
    "
    );
    while run {
        let mut a = String::from("");
        let reader = ::std::io::stdin();
        reader.read_line(&mut a).unwrap();
        match a.trim() {
            "c" => {
                println!("Cancelling 1: {:?}", controller.cancel_channel(&1));
            }
            "e" => run = false,
            "1" => {
                println!(
                    "Sending on 1: {:?}",
                    tx_1.try_send(format!("ManualA message{}", i))
                );
            }
            "t" => {
                let _ = controller.channel(1, 2);
                println!("Re-Created and dropped channel 1");
            }
            "g" => {
                controller.gc();
                println!("Triggered manual GC");
            }
            "2" => {
                println!(
                    "Sending on 2: {:?}",
                    tx_2.try_send(format!("ManualB message{}", i))
                );
            }
            _ => println!("Unknown command"),
        }
        i += 1;
    }

    // manually drop + gc, so we get a clean shutdown
    drop(tx_1);
    drop(tx_2);
    controller.gc();

    println!("End of example!");
    rt.shutdown_on_idle().wait().unwrap(); // clean shutdown of the runtime
}

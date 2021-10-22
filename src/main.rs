use std::thread;
use std::sync::mpsc;
// originally used hashmap but the reallocations will not scale
// very well with an unknown number of messages coming in
//use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use rand::thread_rng;
use rand::seq::SliceRandom;

const BATCH_SIZE: usize = 5;
const NUM_MESSAGES: usize = 20000;

fn main() {
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    let processor = thread::spawn(move || {
        // Processor receiving batches from consumer and printing accordingly
        for batch in rx2 {
            print!("Batch Received: ");
            for message in batch {
                let message: usize = message;
                print!(" {} ", message.to_string());
            }
            println!();
        }
    });

    let consumer = thread::spawn(move || {
        // consumer waiting for messages, loading them into batches in order and sending them to the processor
        let mut batches = BTreeMap::new();
        let mut batch_to_send = 0;

        for message in rx1 {
            let mut len;
            {
                let batch = batches.entry(message/BATCH_SIZE).or_insert_with(BTreeSet::new);
                batch.insert(message);
                len = batch.len();
            }
            
            // send next batch if it is ready
            if len == BATCH_SIZE && message/BATCH_SIZE == batch_to_send {
                // do while loop
                while {
                    let to_send = batches.remove(&(batch_to_send)).unwrap().into_iter().collect();
                    tx2.send(to_send).unwrap();
                    batch_to_send += 1;
                    
                    // reset len to the length of the next batch to check it
                    if batches.contains_key(&batch_to_send) {
                        len = batches.get(&batch_to_send).unwrap().len();

                        len == BATCH_SIZE
                    } else {
                        false
                    }
                    
                } {}
            }
        }
        
        // send all batches that are not sent already
        while batches.contains_key(&batch_to_send) {
            let to_send = batches.remove(&batch_to_send).unwrap();
            tx2.send(to_send).unwrap();
            batch_to_send += 1;
        }
        drop(tx2);
    });

    let producer = thread::spawn(move || {
        let mut messages: Vec<usize> = (0..NUM_MESSAGES).collect();
        messages.shuffle(&mut thread_rng());
        for i in messages {
            tx1.send(i).unwrap();
        }
        drop(tx1);
    });

    // wait for all threads to finish
    producer.join().unwrap();
    consumer.join().unwrap();
    processor.join().unwrap();
}

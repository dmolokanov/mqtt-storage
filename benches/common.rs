use std::{collections::HashMap, collections::VecDeque, rc::Rc};

use criterion::black_box;
use rand::{distributions::Standard, Rng};

use mqtt_storage::{Key, Payload, Storage};

pub enum Op {
    Write(String, Payload),
    Read(String),
    Remove(String),
}

pub fn run(storage: &mut impl Storage, ops: impl IntoIterator<Item = Op>) {
    let mut batches = HashMap::new();
    let mut inflights: HashMap<Rc<String>, Vec<Key>> = HashMap::new();

    let mut iter = |op| match op {
        Op::Write(name, payload) => {
            storage.push(&name, payload);
        }
        Op::Read(name) => {
            let name = Rc::new(name);

            let batch = batches
                .entry(name.clone())
                .or_insert_with(|| storage.batch(&name, 100));

            let inflight = inflights.entry(name.clone()).or_default();

            if let Some((key, _)) = batch.pop_front() {
                let len = inflight.len();
                let index = rand::thread_rng().gen_range(0, len + 1);

                inflight.insert(index, key);
            }

            if batch.is_empty() {
                batches.remove(&name);
            }
        }
        Op::Remove(name) => {
            let name = Rc::new(name);

            let inflight = inflights.entry(name.clone()).or_default();
            if let Some(key) = inflight.pop() {
                storage.remove(&name, key);
            }
        }
    };

    for op in ops {
        iter(black_box(op));
    }
}

pub fn ops(n: u64, prefix: &str, queues: u16) -> VecDeque<Op> {
    let mut rng = rand::thread_rng();

    let op = |_| {
        let name = format!("{}{}", prefix, rng.gen_range(0, queues));

        if rng.gen_ratio(1, 3) {
            let size = rng.gen_range(0, 1_000);
            let payload = rng.sample_iter(&Standard).take(size).collect();

            Op::Write(name, payload)
        } else if rng.gen_ratio(1, 3) {
            Op::Read(name)
        } else {
            Op::Remove(name)
        }
    };

    (0..n).map(op).collect()
}

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Display,
    sync::Arc,
};

use dashmap::DashMap;

use crate::{Key, Payload, Storage};

#[derive(Default)]
pub struct Memory {
    queues: Arc<DashMap<String, Queue>>,
}

impl Memory {
    pub fn new(prefix: impl Display, count: u16) -> Self {
        let queues = (0..count)
            .map(|i| (format!("{}{}", prefix, i), Queue::default()))
            .collect();

        Self {
            queues: Arc::new(queues),
        }
    }
}

impl Storage for Memory {
    fn names(&self) -> Vec<String> {
        self.queues.iter().map(|i| i.key().clone()).collect()
    }

    fn push(&self, name: &str, payload: Payload) -> Key {
        if let Some(mut queue) = self.queues.get_mut(name) {
            queue.push(payload)
        } else {
            panic!("no queue: {}", name)
        }
    }

    fn remove(&self, name: &str, key: Key) {
        if let Some(mut queue) = self.queues.get_mut(name) {
            queue.remove(key)
        } else {
            panic!("no queue: {}", name)
        }
    }

    fn batch(&self, name: &str, size: usize) -> VecDeque<(Key, Payload)> {
        if let Some(queue) = self.queues.get(name) {
            queue.batch(size)
        } else {
            panic!("no queue: {}", name)
        }
    }
}

#[derive(Debug, Default)]
struct Queue {
    last_key: Key,
    items: BTreeMap<Key, Payload>,
}

impl Queue {
    fn push(&mut self, item: Payload) -> Key {
        let current_key = self.last_key;
        self.items.insert(current_key, item);
        self.last_key = current_key.next();

        current_key
    }

    fn remove(&mut self, key: Key) {
        self.items.remove(&key);
    }

    fn batch(&self, count: usize) -> VecDeque<(Key, Payload)> {
        self.items
            .iter()
            .take(count)
            .map(|(k, v)| (*k, v.to_vec()))
            .collect()
    }
}

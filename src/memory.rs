use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Display,
    sync::Arc,
};

use dashmap::DashMap;

use crate::{Key, Payload, Storage};

#[derive(Default)]
pub struct Memory<Q> {
    queues: Arc<DashMap<String, Q>>,
}

impl Memory<BTreeQueue> {
    pub fn tree(prefix: impl Display, count: u16) -> Self {
        let queues = (0..count)
            .map(|i| (format!("{}{}", prefix, i), BTreeQueue::default()))
            .collect();

        Self {
            queues: Arc::new(queues),
        }
    }
}

impl Memory<VecQueue> {
    pub fn vec(prefix: impl Display, count: u16) -> Self {
        let queues = (0..count)
            .map(|i| (format!("{}{}", prefix, i), VecQueue::default()))
            .collect();

        Self {
            queues: Arc::new(queues),
        }
    }
}

impl<Q: Queue> Storage for Memory<Q> {
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
pub struct BTreeQueue {
    last_key: Key,
    items: BTreeMap<Key, Payload>,
}

impl Queue for BTreeQueue {
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

pub trait Queue {
    fn push(&mut self, item: Payload) -> Key;

    fn remove(&mut self, key: Key);

    fn batch(&self, count: usize) -> VecDeque<(Key, Payload)>;
}

#[derive(Debug, Default)]
pub struct VecQueue {
    last_key: Key,
    items: VecDeque<(Key, Payload)>,
}

impl Queue for VecQueue {
    fn push(&mut self, item: Payload) -> Key {
        let current_key = self.last_key;
        self.items.push_front((current_key, item));
        self.last_key = current_key.next();

        current_key
    }

    fn remove(&mut self, key: Key) {
        let index = self.last_key.offset(key);
        self.items.remove(index as usize);
    }

    fn batch(&self, count: usize) -> VecDeque<(Key, Payload)> {
        self.items
            .iter()
            .take(count)
            .map(|(k, v)| (*k, v.to_vec()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_queue_works() {
        let mut queue = VecQueue::default();
        let key = queue.push(vec![1, 2, 3]);
        queue.remove(key);

        assert!(queue.items.is_empty());
    }

    #[test]
    fn vec_queue_batch_works() {
        let mut queue = VecQueue::default();
        let key = queue.push(vec![1, 2, 3]);
        queue.remove(key);

        assert!(queue.items.is_empty());
    }
}

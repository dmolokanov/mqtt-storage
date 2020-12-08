use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    fmt::Display,
    path::Path,
};

use dashmap::DashMap;

use crate::{Key, Payload, Storage};

pub struct QueueFile {
    _path: Box<dyn AsRef<Path> + Send + Sync>,
    queues: DashMap<String, Queue>,
}

impl QueueFile {
    pub fn new(
        path: impl AsRef<Path> + Send + Sync + 'static,
        prefix: impl Display,
        count: u16,
    ) -> Self {
        if path.as_ref().exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        std::fs::create_dir(&path).unwrap();

        let queues = (0..count)
            .map(|i| {
                let path = path.as_ref().join(format!("{}{}.qf", prefix, i));
                let db = queue_file::QueueFile::open(&path).unwrap();

                let name = format!("{}{}", prefix, i);
                (name, Queue::new(db))
            })
            .collect();

        Self {
            _path: Box::new(path),
            queues,
        }
    }
}

impl Storage for QueueFile {
    fn names(&self) -> Vec<String> {
        self.queues.iter().map(|i| i.key().clone()).collect()
    }

    fn push(&self, name: &str, payload: Payload) -> Key {
        if let Some(mut queue) = self.queues.get_mut(name) {
            queue.push(payload)
        } else {
            panic!("no tree: {}", name)
        }
    }

    fn batch(&self, name: &str, size: usize) -> VecDeque<(Key, Payload)> {
        if let Some(mut queue) = self.queues.get_mut(name) {
            queue.batch(size)
        } else {
            panic!("no tree: {}", name)
        }
    }

    fn remove(&self, name: &str, key: Key) {
        if let Some(mut queue) = self.queues.get_mut(name) {
            queue.remove(key);
        } else {
            panic!("no tree: {}", name)
        }
    }
}

struct Queue {
    file: queue_file::QueueFile,
    last_key: Key,
    oldest: Key,
    to_remove: BinaryHeap<Reverse<Key>>,
}

impl Queue {
    fn new(file: queue_file::QueueFile) -> Self {
        Self {
            file,
            last_key: Key::default(),
            oldest: Key::default(),
            to_remove: BinaryHeap::default(),
        }
    }

    fn push(&mut self, item: Vec<u8>) -> Key {
        let current_key = self.last_key;

        self.file.add(&item).unwrap();
        self.last_key = current_key.next();

        current_key
    }

    fn remove(&mut self, key: Key) {
        self.to_remove.push(Reverse(key));

        while matches!(self.to_remove.peek(), Some(smallest) if *smallest == Reverse(self.oldest)) {
            self.oldest = self.oldest.next();
            self.file.remove().unwrap();
            self.to_remove.pop();
        }
    }

    fn batch(&mut self, count: usize) -> VecDeque<(Key, Vec<u8>)> {
        let oldest = self.oldest.1;
        self.file
            .iter()
            .take(count)
            .enumerate()
            .map(|(i, v)| (Key::with_offset(oldest + i as u64), v.to_vec()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_iters_without_delete() {
        let path = tempfile::TempDir::new().unwrap();

        let mut file = queue_file::QueueFile::open(&path.as_ref().join("foo.qf")).unwrap();
        file.add(b"1").unwrap();
        file.add(b"2").unwrap();

        assert!(matches!(file.iter().next(), Some(p) if p.to_vec() == b"1"));
        assert!(matches!(file.iter().next(), Some(p) if p.to_vec() == b"1"));

        file.remove().unwrap();
        assert!(matches!(file.iter().next(), Some(p) if p.to_vec() == b"2"));

        file.remove().unwrap();
        assert!(matches!(file.iter().next(), None));
    }
}

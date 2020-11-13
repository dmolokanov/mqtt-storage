use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    path::Path,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use sled::{Db, Tree};

use crate::{Key, Payload, Storage};

pub struct Sled {
    _path: Box<dyn AsRef<Path> + Send + Sync>,
    _db: Db,
    queues: HashMap<String, Queue>,
}

impl Sled {
    pub fn new(
        path: impl AsRef<Path> + Send + Sync + 'static,
        prefix: impl Display,
        count: u16,
    ) -> Self {
        if path.as_ref().exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        let db = sled::open(&path).unwrap();
        let mut queues = HashMap::new();

        for i in 0..count {
            let name = format!("{}{}", prefix, i);
            let tree = db.open_tree(&name).unwrap();
            queues.insert(name, Queue::new(tree));
        }

        Self {
            _path: Box::new(path),
            _db: db,
            queues,
        }
    }
}

impl Storage for Sled {
    fn names(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }

    fn push(&self, name: &str, payload: Payload) -> Key {
        if let Some(queue) = self.queues.get(name) {
            queue.push(payload)
        } else {
            panic!("no tree: {}", name)
        }
    }

    fn batch(&self, name: &str, size: usize) -> VecDeque<(Key, Payload)> {
        if let Some(queue) = self.queues.get(name) {
            queue.batch(size)
        } else {
            panic!("no tree: {}", name)
        }
    }

    fn remove(&self, name: &str, key: Key) {
        if let Some(queue) = self.queues.get(name) {
            queue.remove(key);
        } else {
            panic!("no tree: {}", name)
        }
    }
}

struct Queue {
    writes: AtomicUsize,
    tree: Tree,
    offset: AtomicU64,
}

impl Queue {
    fn new(tree: Tree) -> Self {
        Self {
            writes: AtomicUsize::default(),
            tree,
            offset: AtomicU64::default(),
        }
    }

    fn push(&self, item: Vec<u8>) -> Key {
        let offset = self.offset.fetch_add(1, Ordering::SeqCst);
        let current_key = Key::with_offset(offset);

        self.with_flush(move |tree| {
            tree.insert(current_key.to_string(), item).unwrap();
        });

        current_key
    }

    fn remove(&self, key: Key) {
        let key = key.to_string();
        self.with_flush(|tree| {
            tree.remove(key).unwrap();
        });
    }

    fn batch(&self, count: usize) -> VecDeque<(Key, Vec<u8>)> {
        self.tree
            .iter()
            .take(count)
            .filter_map(|i| i.map_or_else(|_| None, |(k, v)| Some((k.into(), v.to_vec()))))
            .collect()
    }

    fn with_flush(&self, f: impl FnOnce(&Tree)) {
        f(&self.tree);

        if self.writes.fetch_add(1, Ordering::SeqCst) > 100 {
            self.tree.flush().unwrap();
            self.writes.store(0, Ordering::SeqCst);
        }
    }
}

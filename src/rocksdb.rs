use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    path::Path,
    sync::atomic::AtomicU64,
    sync::atomic::Ordering,
};

use rocksdb::{IteratorMode, Options, DB};

use crate::{Key, Payload, Storage};

pub struct Rocksdb {
    _path: Box<dyn AsRef<Path> + Send + Sync>,
    db: DB,
    offsets: HashMap<String, AtomicU64>,
}

impl Rocksdb {
    pub fn new(
        path: impl AsRef<Path> + Send + Sync + 'static,
        prefix: impl Display,
        count: u16,
    ) -> Self {
        if path.as_ref().exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let mut db = DB::open(&db_opts, &path).unwrap();
        let mut offsets = HashMap::new();

        for i in 0..count {
            let name = format!("{}{}", prefix, i);
            let opts = Options::default();
            db.create_cf(&name, &opts).unwrap();
            offsets.insert(name, AtomicU64::default());
        }

        Self {
            _path: Box::new(path),
            db,
            offsets,
        }
    }
}

impl Storage for Rocksdb {
    fn names(&self) -> Vec<String> {
        self.offsets.keys().cloned().collect()
    }

    fn push(&self, name: &str, payload: Payload) -> Key {
        if let Some(cf) = self.db.cf_handle(name) {
            let offset = self.offsets.get(name).unwrap();
            let offset = offset.fetch_add(1, Ordering::SeqCst);

            let current_key = Key::with_offset(offset);

            self.db
                .put_cf(cf, current_key.to_string(), payload)
                .unwrap();

            current_key
        } else {
            panic!("no cf: {}", name)
        }
    }

    fn batch(&self, name: &str, size: usize) -> VecDeque<(Key, Payload)> {
        if let Some(cf) = self.db.cf_handle(name) {
            self.db
                .iterator_cf(cf, IteratorMode::Start)
                .take(size)
                .map(|(k, v)| (k.into(), v.into()))
                .collect()
        } else {
            panic!("no cf: {}", name)
        }
    }

    fn remove(&self, name: &str, key: Key) {
        if let Some(cf) = self.db.cf_handle(name) {
            let key = key.to_string();
            self.db.delete_cf(cf, key).unwrap();
        } else {
            panic!("no cf: {}", name)
        }
    }
}

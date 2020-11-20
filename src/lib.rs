use std::{collections::VecDeque, fmt::Display, ops::Deref};

pub mod app;
mod memory;
#[cfg(feature = "rocksdb")]
mod rocksdb;
mod sled;

pub use crate::memory::Memory;
#[cfg(feature = "rocksdb")]
pub use crate::rocksdb::Rocksdb;
pub use crate::sled::Sled;

pub trait Storage {
    fn names(&self) -> Vec<String>;
    fn push(&self, name: &str, payload: Payload) -> Key;
    fn batch(&self, name: &str, size: usize) -> VecDeque<(Key, Payload)>;
    fn remove(&self, name: &str, key: Key);
}

pub type Payload = Vec<u8>;

#[derive(Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Key(u16, u64);

impl Key {
    pub fn next(&self) -> Key {
        Key(self.0, self.1 + 1)
    }

    pub fn with_offset(offset: u64) -> Self {
        Self(0, offset)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}_{:012}", self.0, self.1)
    }
}

impl<T: Deref<Target = [u8]>> From<T> for Key {
    fn from(bytes: T) -> Self {
        let (priority, offset) = bytes.split_at(4);

        let priority = std::str::from_utf8(priority).unwrap();
        let priority = priority.parse().unwrap();

        let offset = std::str::from_utf8(&offset[1..]).unwrap();
        let offset = offset.parse().unwrap();

        Self(priority, offset)
    }
}

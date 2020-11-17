use std::{collections::BTreeMap, num::NonZeroU16, str::FromStr};

use anyhow::Result;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use prettytable::{cell, row, Table};
use structopt::StructOpt;

use mqtt_storage::{
    app::{self, EgressStats, IngressStats},
    Memory, Rocksdb, Sled,
};

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(200);
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("/|\\- ")
            .template("{spinner:.dim.bold} storage: {wide_msg}"),
    );
    pb.tick();

    let mut results = BTreeMap::new();

    if opt.storage.contains(&StorageOpt::Memory) {
        pb.set_message("memory");

        let storage = Memory::new("q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert(StorageOpt::Memory, res);
    }

    if opt.storage.contains(&StorageOpt::Sled) {
        pb.set_message("sled");

        let storage = Sled::new("sled", "q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert(StorageOpt::Sled, res);
    }

    if opt.storage.contains(&StorageOpt::Rocksdb) {
        pb.set_message("rocksdb");

        let storage = Rocksdb::new("rocksdb", "q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert(StorageOpt::Rocksdb, res);
    }

    pb.finish_and_clear();

    print(results);

    Ok(())
}

fn print(results: BTreeMap<StorageOpt, (IngressStats, EgressStats)>) {
    let mut table = Table::new();
    table.add_row(row![
        "storage",
        "writes",
        "total write",
        "empty iter",
        "loop iter",
        "reads",
        "total read"
    ]);
    for (mode, (i, e)) in results {
        table.add_row(row![
            mode,
            i.total_items,
            HumanBytes(i.total_bytes),
            e.empty,
            e.loop_iter,
            e.total_items,
            HumanBytes(e.total_bytes)
        ]);
    }

    table.printstd();
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Opt {
    #[structopt(default_value = "2", long, short)]
    duration: u64,

    #[structopt(default_value = "10", long, short)]
    queues: u16,

    #[structopt(default_value = "memory", long, short)]
    storage: Vec<StorageOpt>,

    #[structopt(default_value = "1", long, short)]
    parallel: NonZeroU16,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
enum StorageOpt {
    Memory,
    Sled,
    Rocksdb,
}

impl FromStr for StorageOpt {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sled" => Ok(Self::Sled),
            "memory" => Ok(Self::Memory),
            "rocksdb" => Ok(Self::Rocksdb),
            x => Err(format!("unknown storage :{}", x)),
        }
    }
}

impl std::fmt::Display for StorageOpt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            StorageOpt::Memory => "memory",
            StorageOpt::Sled => "sled",
            StorageOpt::Rocksdb => "rocksdb",
        };
        write!(f, "{}", label)
    }
}

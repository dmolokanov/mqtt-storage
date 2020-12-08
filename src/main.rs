use std::{collections::BTreeMap, num::NonZeroU16};

use anyhow::Result;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use prettytable::{cell, row, Table};
use structopt::StructOpt;

use mqtt_storage::{
    app::{self, EgressStats, IngressStats},
    Memory, QueueFile, Sled,
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

    if opt.memory {
        pb.set_message("tree backed memory");

        let storage = Memory::tree("q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert("BTreeMap", res);

        pb.set_message("vec backed memory");

        let storage = Memory::vec("q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert("VecDeque", res);
    }

    if opt.sled {
        pb.set_message("sled");

        let storage = Sled::new("sled", "q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert("sled", res);
    }

    if opt.queue_file {
        pb.set_message("queue file");

        let storage = QueueFile::new("qf", "q", opt.queues);
        let res = app::run(storage, opt.duration, opt.parallel).await?;
        results.insert("queue file", res);
    }

    #[cfg(feature = "rocksdb")]
    {
        use mqtt_storage::Rocksdb;
        if opt.rocksdb {
            pb.set_message("rocksdb");

            let storage = Rocksdb::new("rocksdb", "q", opt.queues);
            let res = app::run(storage, opt.duration, opt.parallel).await?;
            results.insert("rocksdb", res);
        }
    }

    pb.finish_and_clear();

    print(results);

    Ok(())
}

fn print(results: BTreeMap<&str, (IngressStats, EgressStats)>) {
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

    #[structopt(help = "Examine in-memory storage", long)]
    memory: bool,

    #[structopt(help = "Examine sled-rs storage", long)]
    sled: bool,

    #[cfg(feature = "rocksdb")]
    #[structopt(help = "Examine rocksdb-rs storage", long)]
    rocksdb: bool,

    #[structopt(help = "Examine queue-file based storage", long)]
    queue_file: bool,

    #[structopt(default_value = "1", long, short)]
    parallel: NonZeroU16,
}

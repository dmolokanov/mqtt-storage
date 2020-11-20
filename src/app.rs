use std::{collections::HashMap, num::NonZeroU16, ops::Add, sync::Arc, time::Duration};

use anyhow::Result;
use futures::{future, try_join};
use rand::{distributions::Standard, prelude::ThreadRng, Rng};
use tokio::{
    sync::oneshot::{self, error::TryRecvError, Receiver},
    time,
};

use crate::Storage;

thread_local! {
    static RNG : std::cell::RefCell<ThreadRng> = std::cell::RefCell::new(rand::thread_rng());
}

pub async fn run<S>(
    storage: S,
    secs: u64,
    parallel: NonZeroU16,
) -> Result<(IngressStats, EgressStats)>
where
    S: Storage + Send + Sync + 'static,
{
    let storage = Arc::new(storage);

    let (ingress_send, ingress): (Vec<_>, Vec<_>) = (0..parallel.get())
        .map(|_| {
            let (tx, rx) = oneshot::channel();
            let join = tokio::spawn(ingress(storage.clone(), rx));
            (tx, join)
        })
        .unzip();

    let (egress_send, egress): (Vec<_>, Vec<_>) = (0..parallel.get())
        .map(|_| {
            let (tx, rx) = oneshot::channel();
            let join = tokio::spawn(egress(storage.clone(), rx));
            (tx, join)
        })
        .unzip();

    time::sleep(Duration::from_secs(secs)).await;

    ingress_send.into_iter().for_each(|tx| tx.send(()).unwrap());
    egress_send.into_iter().for_each(|tx| tx.send(()).unwrap());

    let (ingress, egress) = try_join!(future::try_join_all(ingress), future::try_join_all(egress))?;

    Ok((
        ingress.into_iter().fold(IngressStats::default(), Add::add),
        egress.into_iter().fold(EgressStats::default(), Add::add),
    ))
}

async fn ingress<S>(storage: Arc<S>, mut ingress_recv: Receiver<()>) -> IngressStats
where
    S: Storage + Send,
{
    let names = storage.names();

    let mut stats = IngressStats::default();

    while let Err(TryRecvError::Empty) = ingress_recv.try_recv() {
        let size = RNG.with(|rng| rng.borrow_mut().gen_range(0, 100));
        let payload = RNG.with(|rng| {
            rng.borrow_mut()
                .sample_iter::<u8, Standard>(Standard)
                .take(size)
                .collect()
        });

        let name = RNG.with(|rng| rng.borrow_mut().gen_range(0, names.len()));
        let name = &names[name];
        storage.push(name, payload);

        stats.total_items += 1;
        stats.total_bytes += size as u64;

        if stats.total_bytes % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    stats
}

async fn egress<S>(storage: Arc<S>, mut egress_recv: Receiver<()>) -> EgressStats
where
    S: Storage + Send,
{
    let names = storage.names();

    let mut batches = HashMap::new();
    let mut inflights = HashMap::new();

    let mut stats = EgressStats::default();

    while let Err(TryRecvError::Empty) = egress_recv.try_recv() {
        stats.loop_iter += 1;

        let name = RNG.with(|rng| rng.borrow_mut().gen_range(0, names.len()));
        let name = Arc::new(format!("q{}", name));

        let batch = batches
            .entry(name.clone())
            .or_insert_with(|| storage.batch(&name, 100));

        if batch.is_empty() {
            stats.empty += 1;
        }

        let inflight = inflights.entry(name.clone()).or_insert_with(Vec::default);

        if let Some((k, v)) = batch.pop_front() {
            stats.total_items += 1;
            stats.total_bytes += v.len() as u64;

            let index = RNG.with(|rng| rng.borrow_mut().gen_range(0, inflight.len() + 1));
            inflight.insert(index, k);
        }

        if batch.is_empty() {
            batches.remove(&name);
        }

        if let Some(key) = inflight.pop() {
            storage.remove(&name, key);
        }

        if stats.total_bytes % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    stats
}

#[derive(Debug, Default)]
pub struct EgressStats {
    pub empty: u64,
    pub total_bytes: u64,
    pub total_items: u64,
    pub loop_iter: u64,
}

impl Add<Self> for EgressStats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            empty: self.empty + rhs.empty,
            total_bytes: self.total_bytes + rhs.total_bytes,
            total_items: self.total_items + rhs.total_items,
            loop_iter: self.loop_iter + rhs.loop_iter,
        }
    }
}

#[derive(Debug, Default)]
pub struct IngressStats {
    pub total_bytes: u64,
    pub total_items: u64,
}

impl Add<Self> for IngressStats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            total_bytes: self.total_bytes + rhs.total_bytes,
            total_items: self.total_items + rhs.total_items,
        }
    }
}

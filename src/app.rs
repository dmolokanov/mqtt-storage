use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use anyhow::Result;
use rand::{distributions::Standard, Rng};
use tokio::{
    sync::oneshot::{self, error::TryRecvError, Receiver},
    time,
};

use crate::Storage;

pub async fn run<S>(storage: S, secs: u64) -> Result<(IngressStats, EgressStats)>
where
    S: Storage + Send + Sync + 'static,
{
    let storage = Arc::new(storage);

    let (ingress_send, ingress_recv) = oneshot::channel();
    let ingress = tokio::spawn(ingress(storage.clone(), ingress_recv));

    let (egress_send, egress_recv) = oneshot::channel();
    let egress = tokio::spawn(egress(storage, egress_recv));

    time::sleep(Duration::from_secs(secs)).await;

    ingress_send.send(()).unwrap();
    egress_send.send(()).unwrap();

    Ok(tokio::try_join!(ingress, egress)?)
}

async fn ingress<S>(storage: Arc<S>, mut ingress_recv: Receiver<()>) -> IngressStats
where
    S: Storage + Send,
{
    let names = storage.names();

    let mut rng = rand::thread_rng();

    let mut stats = IngressStats::default();

    while let Err(TryRecvError::Empty) = ingress_recv.try_recv() {
        let size = rng.gen_range(0, 1_00);
        let payload = rng
            .sample_iter::<u8, Standard>(Standard)
            .take(size)
            .collect();

        let name = &names[rng.gen_range(0, names.len())];
        storage.push(name, payload);

        stats.total_items += 1;
        stats.total_bytes += size as u64;
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

    let mut rng = rand::thread_rng();

    let mut stats = EgressStats::default();

    while let Err(TryRecvError::Empty) = egress_recv.try_recv() {
        stats.loop_iter += 1;

        let name = rng.gen_range(0, names.len());
        let name = Rc::new(format!("q{}", name));

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

            let index = rng.gen_range(0, inflight.len() + 1);
            inflight.insert(index, k);
        }

        if batch.is_empty() {
            batches.remove(&name);
        }

        if let Some(key) = inflight.pop() {
            storage.remove(&name, key);
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

#[derive(Debug, Default)]
pub struct IngressStats {
    pub total_bytes: u64,
    pub total_items: u64,
}

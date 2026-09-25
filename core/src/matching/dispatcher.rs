use super::{snapshot::CandidateSnapshot, Candidate};
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock, Mutex},
};

static POOLS: LazyLock<Mutex<BTreeMap<usize, Arc<rayon::ThreadPool>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn pool(workers: usize) -> Arc<rayon::ThreadPool> {
    let workers = workers.max(1);
    let mut pools = POOLS.lock().expect("Rayon pool cache lock is not poisoned");
    pools
        .entry(workers)
        .or_insert_with(|| {
            Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .thread_name(move |index| format!("erps-match-{workers}-{index}"))
                    .build()
                    .expect("validated worker count creates a Rayon pool"),
            )
        })
        .clone()
}
pub fn generate<F>(
    snapshot: &CandidateSnapshot,
    workers: usize,
    budget: usize,
    builder: F,
) -> Vec<Candidate>
where
    F: Fn(&[super::PartyTicket]) -> Vec<Candidate> + Sync + Send,
{
    let mut owners = BTreeSet::new();
    for ticket in &snapshot.tickets {
        owners.insert((
            ticket.region.clone(),
            ticket.mode,
            ticket.effective_rating.div_euclid(100),
        ));
    }
    let partitions = owners
        .into_iter()
        .enumerate()
        .map(|(owner_shard, (region, mode, bucket))| {
            let tickets = snapshot
                .tickets
                .iter()
                .filter(|ticket| ticket.region == region && ticket.mode == mode)
                .filter(|ticket| {
                    let ticket_bucket = ticket.effective_rating.div_euclid(100);
                    let halo = (ticket.search_delta.max(0) + 99) / 100;
                    (ticket_bucket - bucket).abs() <= halo
                })
                .cloned()
                .collect::<Vec<_>>();
            (owner_shard as u64, tickets)
        })
        .collect::<Vec<_>>();
    let pool = pool(workers);
    let mut items: Vec<_> = pool.install(|| {
        partitions
            .into_par_iter()
            .flat_map(|(owner_shard, part)| {
                let mut remaining = part;
                let mut local = Vec::new();
                while local.len() < budget {
                    let mut candidates = builder(&remaining);
                    for candidate in &mut candidates {
                        candidate.owner_shard = owner_shard;
                    }
                    sort_candidates(&mut candidates);
                    let mut claimed = BTreeSet::new();
                    let mut selected = 0;
                    for candidate in candidates {
                        if candidate
                            .tickets
                            .iter()
                            .any(|ticket| claimed.contains(ticket))
                        {
                            continue;
                        }
                        claimed.extend(candidate.tickets.iter().copied());
                        local.push(candidate);
                        selected += 1;
                        if local.len() == budget {
                            break;
                        }
                    }
                    if selected == 0 {
                        break;
                    }
                    remaining.retain(|ticket| !claimed.contains(&ticket.id));
                }
                local.into_par_iter()
            })
            .collect()
    });
    sort_candidates(&mut items);
    items
}
pub fn sort_candidates(items: &mut [Candidate]) {
    items.sort_by(|a, b| {
        (
            a.oldest_enqueued_at,
            a.quality_key,
            a.owner_shard,
            a.stable_ticket_ids(),
        )
            .cmp(&(
                b.oldest_enqueued_at,
                b.quality_key,
                b.owner_shard,
                b.stable_ticket_ids(),
            ))
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{id::TicketId, matching::Candidate};
    #[test]
    fn arrival_order_does_not_matter() {
        let a = Candidate {
            tickets: vec![TicketId::new()],
            teams: vec![],
            oldest_enqueued_at: 1,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let b = Candidate {
            tickets: vec![TicketId::new()],
            teams: vec![],
            oldest_enqueued_at: 1,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut x = vec![a.clone(), b.clone()];
        let mut y = vec![b, a];
        sort_candidates(&mut x);
        sort_candidates(&mut y);
        assert_eq!(x, y);
    }
    #[test]
    fn older_candidate_can_outrank_quality_penalty() {
        let older = Candidate {
            tickets: vec![TicketId::new()],
            teams: vec![],
            oldest_enqueued_at: 1,
            quality_key: (0, 0, 5),
            owner_shard: 0,
        };
        let newer = Candidate {
            tickets: vec![TicketId::new()],
            teams: vec![],
            oldest_enqueued_at: 2,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut candidates = vec![newer, older.clone()];
        sort_candidates(&mut candidates);
        assert_eq!(candidates[0], older);
    }
}

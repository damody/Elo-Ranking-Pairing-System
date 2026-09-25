//! Read-only sharded candidate generation and deterministic commit.
pub mod bucket;
pub mod claim;
pub mod dispatcher;
pub mod five_v_five;
pub mod free_for_all;
pub mod one_v_one;
pub mod snapshot;

use crate::{
    components::QueueMode,
    id::{PartyId, PlayerId, TicketId},
};

pub const FIVE_V_FIVE_MIRROR_SECONDS: u64 = 60;
pub const FIVE_V_FIVE_PARTY_BONUSES: [i32; 4] = [5, 10, 20, 30];
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartyTicket {
    pub id: TicketId,
    pub party: PartyId,
    pub members: Vec<PlayerId>,
    pub ratings: Vec<i32>,
    pub effective_rating: i32,
    pub enqueued_at: u64,
    pub revision: u64,
    pub region: String,
    pub mode: QueueMode,
    pub search_delta: i32,
    pub wait_seconds: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub tickets: Vec<TicketId>,
    pub teams: Vec<Vec<PlayerId>>,
    pub oldest_enqueued_at: u64,
    pub quality_key: (i32, i32, i32),
    pub owner_shard: u64,
}
impl Candidate {
    pub fn stable_ticket_ids(&self) -> Vec<TicketId> {
        let mut ids = self.tickets.clone();
        ids.sort();
        ids
    }
}
pub fn effective_rating(
    ratings: &[i32],
    size_adjustment: i32,
    spread_adjustment: i32,
    max_spread: i32,
) -> Option<i32> {
    if ratings.is_empty() {
        return None;
    }
    let min = *ratings.iter().min()?;
    let max = *ratings.iter().max()?;
    let spread = i64::from(max) - i64::from(min);
    if spread > i64::from(max_spread) {
        return None;
    }
    let average =
        ratings.iter().map(|rating| i64::from(*rating)).sum::<i64>() / ratings.len() as i64;
    let adjusted = average
        + i64::from(size_adjustment) * (ratings.len() as i64 - 1)
        + i64::from(spread_adjustment) * spread / 100;
    Some(adjusted.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32)
}

pub fn five_v_five_party_bonus(size: usize) -> i32 {
    match size {
        1 => 0,
        2..=5 => FIVE_V_FIVE_PARTY_BONUSES[size - 2],
        _ => 0,
    }
}

pub fn effective_rating_for_mode(
    mode: QueueMode,
    ratings: &[i32],
    size_adjustment: i32,
    spread_adjustment: i32,
    max_spread: i32,
) -> Option<i32> {
    let size_adjustment = if mode == QueueMode::FiveVsFive {
        0
    } else {
        size_adjustment
    };
    let rating = effective_rating(ratings, size_adjustment, spread_adjustment, max_spread)?;
    Some(if mode == QueueMode::FiveVsFive {
        rating.saturating_add(five_v_five_party_bonus(ratings.len()))
    } else {
        rating
    })
}

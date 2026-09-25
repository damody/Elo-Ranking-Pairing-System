use super::{five_v_five_party_bonus, Candidate, PartyTicket, FIVE_V_FIVE_MIRROR_SECONDS};

fn structure(group: &[usize], tickets: &[PartyTicket]) -> [u8; 5] {
    let mut counts = [0; 5];
    for index in group {
        counts[tickets[*index].members.len() - 1] += 1;
    }
    counts
}

fn raw_rating_total(group: &[usize], tickets: &[PartyTicket]) -> i64 {
    group
        .iter()
        .flat_map(|index| &tickets[*index].ratings)
        .map(|rating| i64::from(*rating))
        .sum()
}

fn advantage_total(group: &[usize], tickets: &[PartyTicket]) -> i64 {
    group
        .iter()
        .map(|index| {
            let size = tickets[*index].members.len();
            i64::from(five_v_five_party_bonus(size)) * size as i64
        })
        .sum()
}
fn subsets(t: &[PartyTicket], target: usize, budget: usize) -> Vec<Vec<usize>> {
    fn go(
        t: &[PartyTicket],
        at: usize,
        left: usize,
        c: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
        budget: usize,
    ) {
        if out.len() >= budget {
            return;
        }
        if left == 0 {
            out.push(c.clone());
            return;
        }
        for i in at..t.len() {
            let n = t[i].members.len();
            if (1..=5).contains(&n) && n <= left {
                c.push(i);
                go(t, i + 1, left - n, c, out, budget);
                c.pop();
            }
        }
    }
    let mut out = Vec::new();
    go(t, 0, target, &mut Vec::new(), &mut out, budget);
    out
}
pub fn build(t: &[PartyTicket], budget: usize) -> Vec<Candidate> {
    let groups = subsets(t, 5, budget);
    let mut out = Vec::new();
    for a in &groups {
        for b in &groups {
            if a.iter().any(|i| b.contains(i)) {
                continue;
            }
            let selected: Vec<_> = a.iter().chain(b).copied().collect();
            let left_structure = structure(a, t);
            let right_structure = structure(b, t);
            if left_structure != right_structure {
                if !selected
                    .iter()
                    .any(|index| t[*index].wait_seconds >= FIVE_V_FIVE_MIRROR_SECONDS)
                {
                    continue;
                }
                let raw_difference = raw_rating_total(a, t) - raw_rating_total(b, t);
                let advantage_difference = advantage_total(a, t) - advantage_total(b, t);
                if advantage_difference > 0 && -raw_difference < advantage_difference
                    || advantage_difference < 0 && raw_difference < -advantage_difference
                {
                    continue;
                }
            }
            let minimum = selected
                .iter()
                .map(|i| t[*i].effective_rating)
                .min()
                .unwrap_or_default();
            let maximum = selected
                .iter()
                .map(|i| t[*i].effective_rating)
                .max()
                .unwrap_or_default();
            let allowed = selected
                .iter()
                .map(|i| t[*i].search_delta)
                .min()
                .unwrap_or_default();
            if i64::from(maximum) - i64::from(minimum) > i64::from(allowed) {
                continue;
            }
            let ta = a.iter().flat_map(|i| t[*i].members.clone()).collect();
            let tb = b.iter().flat_map(|i| t[*i].members.clone()).collect();
            let ar = a
                .iter()
                .map(|i| i64::from(t[*i].effective_rating) * t[*i].members.len() as i64)
                .sum::<i64>()
                / 5;
            let br = b
                .iter()
                .map(|i| i64::from(t[*i].effective_rating) * t[*i].members.len() as i64)
                .sum::<i64>()
                / 5;
            let dispersion = a
                .iter()
                .chain(b)
                .map(|i| {
                    let party = &t[*i];
                    party
                        .ratings
                        .iter()
                        .map(|rating| {
                            (i64::from(*rating) - i64::from(party.effective_rating)).abs()
                        })
                        .sum::<i64>()
                })
                .sum::<i64>()
                .min(i64::from(i32::MAX)) as i32;
            out.push(Candidate {
                tickets: selected.iter().map(|i| t[*i].id).collect(),
                teams: vec![ta, tb],
                oldest_enqueued_at: a
                    .iter()
                    .chain(b)
                    .map(|i| t[*i].enqueued_at)
                    .min()
                    .unwrap_or(0),
                quality_key: (
                    (ar - br).abs().min(i64::from(i32::MAX)) as i32,
                    dispersion,
                    left_structure
                        .iter()
                        .zip(right_structure)
                        .map(|(left, right)| (i32::from(*left) - i32::from(right)).abs())
                        .sum(),
                ),
                owner_shard: 0,
            });
            if out.len() >= budget {
                return out;
            }
        }
    }
    out
}

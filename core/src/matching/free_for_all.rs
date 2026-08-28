use super::{Candidate, PartyTicket};
pub fn build(t: &[PartyTicket], budget: usize) -> Vec<Candidate> {
    fn go(
        t: &[PartyTicket],
        at: usize,
        left: usize,
        c: &mut Vec<usize>,
        out: &mut Vec<Candidate>,
        budget: usize,
    ) {
        if out.len() >= budget {
            return;
        }
        if left == 0 {
            let players: Vec<_> = c.iter().flat_map(|i| t[*i].members.clone()).collect();
            let min = c.iter().map(|i| t[*i].effective_rating).min().unwrap();
            let max = c.iter().map(|i| t[*i].effective_rating).max().unwrap();
            let allowed = c.iter().map(|i| t[*i].search_delta).min().unwrap();
            if max - min > allowed {
                return;
            }
            let mean = c
                .iter()
                .map(|i| t[*i].effective_rating as i64 * t[*i].members.len() as i64)
                .sum::<i64>()
                / 8;
            let dispersion = c
                .iter()
                .map(|i| {
                    (t[*i].effective_rating as i64 - mean).unsigned_abs()
                        * t[*i].members.len() as u64
                })
                .sum::<u64>()
                .min(i32::MAX as u64) as i32;
            out.push(Candidate {
                tickets: c.iter().map(|i| t[*i].id).collect(),
                teams: players.into_iter().map(|p| vec![p]).collect(),
                oldest_enqueued_at: c.iter().map(|i| t[*i].enqueued_at).min().unwrap(),
                quality_key: (max - min, dispersion, 0),
                owner_shard: 0,
            });
            return;
        }
        for i in at..t.len() {
            let n = t[i].members.len();
            if n <= left && n <= 4 {
                c.push(i);
                go(t, i + 1, left - n, c, out, budget);
                c.pop();
            }
        }
    }
    let mut out = Vec::new();
    go(t, 0, 8, &mut Vec::new(), &mut out, budget);
    out
}

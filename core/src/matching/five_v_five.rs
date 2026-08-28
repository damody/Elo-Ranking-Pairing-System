use super::{Candidate, PartyTicket};
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
            if n <= left {
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
            if maximum - minimum > allowed {
                continue;
            }
            let ta = a.iter().flat_map(|i| t[*i].members.clone()).collect();
            let tb = b.iter().flat_map(|i| t[*i].members.clone()).collect();
            let ar = a
                .iter()
                .map(|i| t[*i].effective_rating * t[*i].members.len() as i32)
                .sum::<i32>()
                / 5;
            let br = b
                .iter()
                .map(|i| t[*i].effective_rating * t[*i].members.len() as i32)
                .sum::<i32>()
                / 5;
            let dispersion = a
                .iter()
                .chain(b)
                .map(|i| {
                    let party = &t[*i];
                    party
                        .ratings
                        .iter()
                        .map(|rating| (*rating - party.effective_rating).abs())
                        .sum::<i32>()
                })
                .sum();
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
                    (ar - br).abs(),
                    dispersion,
                    (a.len() as i32 - b.len() as i32).abs(),
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

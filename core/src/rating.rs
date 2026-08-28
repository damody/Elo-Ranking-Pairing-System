use crate::components::QueueMode;

#[derive(Clone, Copy, Debug)]
pub struct RatingPolicy {
    pub established_k: f64,
    pub provisional_k: f64,
    pub provisional_matches: u32,
    pub maximum_delta: i32,
}
impl Default for RatingPolicy {
    fn default() -> Self {
        Self {
            established_k: 20.0,
            provisional_k: 40.0,
            provisional_matches: 10,
            maximum_delta: 40,
        }
    }
}
pub fn expected(a: i32, b: i32) -> f64 {
    1.0 / (1.0 + 10f64.powf((b - a) as f64 / 400.0))
}
pub fn delta(
    current: i32,
    opponent: i32,
    actual: f64,
    completed: u32,
    policy: RatingPolicy,
) -> i32 {
    let k = if completed < policy.provisional_matches {
        policy.provisional_k
    } else {
        policy.established_k
    };
    (k * (actual - expected(current, opponent)))
        .round()
        .clamp(-(policy.maximum_delta as f64), policy.maximum_delta as f64) as i32
}
pub fn one_vs_one(
    a: i32,
    b: i32,
    a_score: f64,
    a_completed: u32,
    b_completed: u32,
    p: RatingPolicy,
) -> (i32, i32) {
    (
        a + delta(a, b, a_score, a_completed, p),
        b + delta(b, a, 1.0 - a_score, b_completed, p),
    )
}
pub fn team_update(winners: &[i32], losers: &[i32], p: RatingPolicy) -> (Vec<i32>, Vec<i32>) {
    let wa = mean(winners);
    let la = mean(losers);
    (
        winners
            .iter()
            .map(|r| *r + delta(*r, la, 1.0, p.provisional_matches, p))
            .collect(),
        losers
            .iter()
            .map(|r| *r + delta(*r, wa, 0.0, p.provisional_matches, p))
            .collect(),
    )
}
pub fn free_for_all(ratings: &[i32], ranks: &[u8], p: RatingPolicy) -> Vec<i32> {
    assert_eq!(ratings.len(), ranks.len());
    ratings
        .iter()
        .enumerate()
        .map(|(i, rating)| {
            let mut sum = 0.0;
            for (j, opponent) in ratings.iter().enumerate() {
                if i == j {
                    continue;
                }
                let actual = if ranks[i] < ranks[j] {
                    1.0
                } else if ranks[i] == ranks[j] {
                    0.5
                } else {
                    0.0
                };
                sum += actual - expected(*rating, *opponent);
            }
            let raw = (p.established_k * sum / ((ratings.len() - 1).max(1) as f64)).round() as i32;
            rating + raw.clamp(-p.maximum_delta, p.maximum_delta)
        })
        .collect()
}
pub fn default_rating(_: QueueMode) -> i32 {
    1000
}
fn mean(values: &[i32]) -> i32 {
    if values.is_empty() {
        0
    } else {
        values.iter().map(|x| *x as i64).sum::<i64>() as i32 / values.len() as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equal_1v1_moves_symmetrically() {
        let p = RatingPolicy {
            provisional_matches: 0,
            ..Default::default()
        };
        assert_eq!(one_vs_one(1000, 1000, 1.0, 20, 20, p), (1010, 990));
    }
    #[test]
    fn team_winners_gain_and_losers_drop() {
        let (w, l) = team_update(&[1000; 5], &[1000; 5], Default::default());
        assert!(w.iter().all(|r| *r > 1000));
        assert!(l.iter().all(|r| *r < 1000));
    }
    #[test]
    fn ffa_ties_are_draws_and_clamped() {
        let p = RatingPolicy {
            maximum_delta: 8,
            ..Default::default()
        };
        let result = free_for_all(&[1000; 8], &[1, 1, 3, 4, 5, 6, 7, 8], p);
        assert_eq!(result[0], result[1]);
        assert!(result.iter().all(|r| (r - 1000).abs() <= 8));
    }
}

//! Credit score policy.

#[derive(Clone, Copy, Debug)]
pub struct CreditPolicy {
    pub reject_penalty: u8,
    pub timeout_penalty: u8,
    pub minimum: u8,
    pub recovery_matches: u32,
}
impl Default for CreditPolicy {
    fn default() -> Self {
        Self {
            reject_penalty: 2,
            timeout_penalty: 5,
            minimum: 60,
            recovery_matches: 3,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreditCause {
    Rejected,
    TimedOut,
    InfrastructureFailure,
    CompletedMatch,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreditOutcome {
    pub score: u8,
    pub eligible: bool,
    pub suspension_steps: u32,
}
pub fn apply(
    score: u8,
    recent_violations: u32,
    completed_since_recovery: u32,
    cause: CreditCause,
    p: CreditPolicy,
) -> CreditOutcome {
    let score = score.min(100);
    let score = match cause {
        CreditCause::Rejected => score.saturating_sub(p.reject_penalty),
        CreditCause::TimedOut => score.saturating_sub(p.timeout_penalty),
        CreditCause::InfrastructureFailure => score,
        CreditCause::CompletedMatch if completed_since_recovery + 1 >= p.recovery_matches => {
            score.saturating_add(1).min(100)
        }
        CreditCause::CompletedMatch => score,
    };
    let violation = matches!(cause, CreditCause::Rejected | CreditCause::TimedOut);
    CreditOutcome {
        score,
        eligible: score >= p.minimum,
        suspension_steps: if violation {
            recent_violations.saturating_add(1)
        } else {
            0
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn infrastructure_never_penalizes() {
        assert_eq!(
            apply(
                61,
                9,
                0,
                CreditCause::InfrastructureFailure,
                Default::default()
            )
            .score,
            61
        );
    }
    #[test]
    fn timeout_can_suspend() {
        let o = apply(62, 1, 0, CreditCause::TimedOut, Default::default());
        assert_eq!(o.score, 57);
        assert!(!o.eligible);
        assert_eq!(o.suspension_steps, 2);
    }
    #[test]
    fn recovery_is_clamped() {
        assert_eq!(
            apply(100, 0, 2, CreditCause::CompletedMatch, Default::default()).score,
            100
        );
    }
}

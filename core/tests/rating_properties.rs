use erps::{
    credit::{apply, CreditCause, CreditPolicy},
    rating::{delta, RatingPolicy},
};
use proptest::prelude::*;

proptest! {
 #[test] fn rating_delta_is_finite_and_bounded(a in -100_000i32..100_000,b in -100_000i32..100_000,actual in 0.0f64..1.0){let p=RatingPolicy::default();let d=delta(a,b,actual,0,p);prop_assert!(d.abs()<=p.maximum_delta);}
 #[test] fn credit_stays_in_range(score in any::<u8>(),violations in any::<u32>()){let o=apply(score,violations,0,CreditCause::TimedOut,CreditPolicy::default());prop_assert!(o.score<=100);}
}

//! Stable opaque domain identifiers.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid {kind}: {value}")]
pub struct ParseIdError {
    pub kind: &'static str,
    pub value: String,
}

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);
        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
        impl FromStr for $name {
            type Err = ParseIdError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self).map_err(|_| ParseIdError {
                    kind: stringify!($name),
                    value: value.into(),
                })
            }
        }
    };
}

uuid_id!(PlayerId);
uuid_id!(PartyId);
uuid_id!(TicketId);
uuid_id!(ProposalId);
uuid_id!(MatchId);
uuid_id!(ServerId);
uuid_id!(SessionId);

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(transparent)]
pub struct ServerGeneration(pub u64);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ids_round_trip_and_order() {
        let a = PlayerId::new();
        let text = a.to_string();
        assert_eq!(text.parse::<PlayerId>().unwrap(), a);
        let encoded = serde_json::to_string(&a).unwrap();
        assert_eq!(serde_json::from_str::<PlayerId>(&encoded).unwrap(), a);
        let mut ids = [PlayerId::new(), a];
        ids.sort();
        assert!(ids[0] <= ids[1]);
    }
    #[test]
    fn malformed_id_reports_type() {
        let e = "bad".parse::<MatchId>().unwrap_err();
        assert_eq!(e.kind, "MatchId");
    }
}

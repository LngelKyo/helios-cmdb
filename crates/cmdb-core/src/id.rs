//! Strongly-typed identifiers.
//!
//! Stored as `UUID` in Postgres (native type, smaller index). Generated in
//! Rust from `ulid::Ulid` so IDs are time-sortable as strings.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum IdParseError {
    #[error("not a valid ulid or uuid: {0}")]
    Invalid(String),
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(into = "Uuid", from = "Uuid")]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(ulid::Ulid::new().into())
            }

            pub fn from_uuid(u: Uuid) -> Self {
                Self(u)
            }

            pub fn as_uuid(&self) -> Uuid {
                self.0
            }

            pub fn as_ulid(&self) -> ulid::Ulid {
                ulid::Ulid::from(self.0.as_u128())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.as_ulid())
            }
        }

        impl From<Uuid> for $name {
            fn from(u: Uuid) -> Self {
                Self(u)
            }
        }

        impl From<$name> for Uuid {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl std::str::FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                if let Ok(u) = Uuid::parse_str(s) {
                    return Ok(Self(u));
                }
                match s.parse::<ulid::Ulid>() {
                    Ok(u) => Ok(Self(Uuid::from_u128(u.into()))),
                    Err(_) => Err(IdParseError::Invalid(s.to_string())),
                }
            }
        }
    };
}

id_type!(EntityId);
id_type!(RelationId);
id_type!(FactId);
id_type!(ChangeId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_roundtrip() {
        let id = EntityId::new();
        let s = id.to_string();
        let parsed: EntityId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn ulid_time_prefix_close() {
        // ULID's high bits encode millisecond time; same-ms generations may
        // have any relative order in the random tail. What we care about is
        // the time prefix being close.
        let a = EntityId::new();
        let b = EntityId::new();
        let a_ms = a
            .as_ulid()
            .datetime()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let b_ms = b
            .as_ulid()
            .datetime()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!((a_ms - b_ms).abs() <= 1, "ulid time prefix should be close");
    }

    #[test]
    fn uuid_string_also_accepted() {
        let id = EntityId::from_uuid(Uuid::new_v4());
        let s = id.as_uuid().to_string();
        let parsed: EntityId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }
}

//! Opaque identifiers and authority counters.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

/// An identifier or authority counter failed validation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IdError {
    /// An opaque identifier was empty or contained leading/trailing whitespace.
    #[error("{kind} must be non-empty and have no surrounding whitespace")]
    InvalidOpaqueId {
        /// The logical identifier type.
        kind: &'static str,
    },
    /// An owner generation or fence token was zero.
    #[error("{kind} must be greater than zero")]
    ZeroAuthorityCounter {
        /// The logical counter type.
        kind: &'static str,
    },
}

macro_rules! opaque_id {
    ($name:ident, $doc:literal, $kind:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        #[schemars(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Create a validated `", stringify!($name), "`.")]
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if value.is_empty() || value.trim() != value {
                    return Err(IdError::InvalidOpaqueId { kind: $kind });
                }
                Ok(Self(value))
            }

            /// Return the wire representation without exposing mutable access.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the identifier and return its wire representation.
            #[must_use]
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

opaque_id!(ProfileId, "A profile-scoped authority identifier.", "profile id");
opaque_id!(SessionId, "A runtime session identifier.", "session id");
opaque_id!(LineageId, "An immutable conversation lineage identifier.", "lineage id");
opaque_id!(RunId, "A single agent or task execution identifier.", "run id");
opaque_id!(TaskId, "A durable coordination task identifier.", "task id");
opaque_id!(WorkerId, "A supervised worker identity.", "worker id");
opaque_id!(BoardId, "A shared board authority identifier.", "board id");
opaque_id!(EventId, "An idempotent inbox or outbox event identifier.", "event id");
opaque_id!(ToolCallId, "A model-issued tool invocation identifier.", "tool call id");

macro_rules! authority_counter {
    ($name:ident, $doc:literal, $kind:literal) => {
        #[doc = $doc]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        #[schemars(transparent)]
        pub struct $name(u64);

        impl $name {
            #[doc = concat!("Create a nonzero `", stringify!($name), "`.")]
            pub fn new(value: u64) -> Result<Self, IdError> {
                if value == 0 {
                    return Err(IdError::ZeroAuthorityCounter { kind: $kind });
                }
                Ok(Self(value))
            }

            /// Return the numeric counter value.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = u64::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

authority_counter!(
    OwnerGeneration,
    "A monotonically increasing write-authority generation.",
    "owner generation"
);
authority_counter!(FenceToken, "A run-scoped token fencing stale task workers.", "fence token");

#[cfg(test)]
mod tests {
    use super::{FenceToken, SessionId};

    #[test]
    fn opaque_ids_reject_empty_and_padded_values() {
        assert!(SessionId::new("").is_err());
        assert!(SessionId::new(" session-1").is_err());
        assert!(SessionId::new("session-1 ").is_err());
        assert_eq!(SessionId::new("session-1").map(|id| id.into_inner()), Ok("session-1".into()));
    }

    #[test]
    fn authority_counters_are_nonzero() {
        assert!(FenceToken::new(0).is_err());
        assert_eq!(FenceToken::new(7).map(FenceToken::get), Ok(7));
    }

    #[test]
    fn deserialization_cannot_bypass_validation() {
        assert!(serde_json::from_str::<SessionId>(r#"""#).is_err());
        assert!(serde_json::from_str::<FenceToken>("0").is_err());
    }
}

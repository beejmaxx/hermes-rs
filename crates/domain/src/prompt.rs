//! Immutable prompt and tool-catalog identity.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

/// An immutable engine implementation stamp, such as `rust-v1`.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct EngineId(String);

/// A lowercase SHA-256 digest used in prompt identity.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ManifestDigest(String);

/// Prompt manifest construction failed validation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PromptManifestError {
    /// Manifest revisions are one-based.
    #[error("prompt manifest revision must be greater than zero")]
    ZeroRevision,
    /// An engine stamp was empty or padded.
    #[error("engine id must be non-empty and have no surrounding whitespace")]
    InvalidEngineId,
    /// A manifest digest was not 64 lowercase hexadecimal characters.
    #[error("manifest digest must be a lowercase SHA-256 hex string")]
    InvalidDigest,
}

impl EngineId {
    /// Create a validated engine stamp.
    pub fn new(value: impl Into<String>) -> Result<Self, PromptManifestError> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(PromptManifestError::InvalidEngineId);
        }
        Ok(Self(value))
    }

    /// Return the wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for EngineId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

impl ManifestDigest {
    /// Parse a lowercase SHA-256 digest.
    pub fn new(value: impl Into<String>) -> Result<Self, PromptManifestError> {
        let value = value.into();
        if value.len() != 64
            || !value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(PromptManifestError::InvalidDigest);
        }
        Ok(Self(value))
    }

    /// Return the lowercase hexadecimal representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ManifestDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ManifestDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Byte identity of the frozen prompt inputs for one conversation revision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PromptManifest {
    /// Monotonic revision within a lineage.
    revision: u64,
    /// Immutable engine selected when the lineage was created.
    engine: EngineId,
    /// SHA-256 of the exact rendered system prompt bytes.
    system_prompt: ManifestDigest,
    /// SHA-256 of the exact ordered tool catalog bytes.
    tool_catalog: ManifestDigest,
}

impl PromptManifest {
    /// Construct a validated immutable prompt manifest.
    pub fn new(
        revision: u64,
        engine: EngineId,
        system_prompt: ManifestDigest,
        tool_catalog: ManifestDigest,
    ) -> Result<Self, PromptManifestError> {
        if revision == 0 {
            return Err(PromptManifestError::ZeroRevision);
        }
        Ok(Self { revision, engine, system_prompt, tool_catalog })
    }

    /// Monotonic one-based revision within a lineage.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Immutable engine identity.
    #[must_use]
    pub const fn engine(&self) -> &EngineId {
        &self.engine
    }

    /// Digest of exact system-prompt bytes.
    #[must_use]
    pub const fn system_prompt(&self) -> &ManifestDigest {
        &self.system_prompt
    }

    /// Digest of canonical ordered tool-catalog bytes.
    #[must_use]
    pub const fn tool_catalog(&self) -> &ManifestDigest {
        &self.tool_catalog
    }
}

impl<'de> Deserialize<'de> for PromptManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Repr {
            revision: u64,
            engine: EngineId,
            system_prompt: ManifestDigest,
            tool_catalog: ManifestDigest,
        }

        let repr = Repr::deserialize(deserializer)?;
        Self::new(repr.revision, repr.engine, repr.system_prompt, repr.tool_catalog)
            .map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{EngineId, ManifestDigest, PromptManifest};

    #[test]
    fn digest_requires_canonical_lowercase_sha256() {
        assert!(ManifestDigest::new("a".repeat(64)).is_ok());
        assert!(ManifestDigest::new("A".repeat(64)).is_err());
        assert!(ManifestDigest::new("a".repeat(63)).is_err());
    }

    #[test]
    fn engine_id_is_not_ambient_or_empty() {
        assert_eq!(EngineId::new("rust-v1").map(|id| id.as_str().to_owned()), Ok("rust-v1".into()));
        assert!(EngineId::new(" rust-v1").is_err());
        assert!(serde_json::from_str::<EngineId>(r#"""#).is_err());
    }

    #[test]
    fn manifest_revisions_are_one_based_even_during_deserialization() {
        let digest = "a".repeat(64);
        assert!(
            PromptManifest::new(
                1,
                EngineId::new("rust-v1").unwrap_or_else(|error| unreachable!("valid id: {error}")),
                ManifestDigest::new(digest.clone())
                    .unwrap_or_else(|error| unreachable!("valid digest: {error}")),
                ManifestDigest::new(digest.clone())
                    .unwrap_or_else(|error| unreachable!("valid digest: {error}")),
            )
            .is_ok()
        );
        let encoded = format!(
            r#"{{"revision":0,"engine":"rust-v1","system_prompt":"{digest}","tool_catalog":"{digest}"}}"#
        );
        assert!(serde_json::from_str::<PromptManifest>(&encoded).is_err());
    }
}

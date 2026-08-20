//! Shared CLI state-path resolution.

use std::path::{Path, PathBuf};

use anyhow::Context;

pub(super) fn state_path(override_path: Option<&Path>) -> anyhow::Result<PathBuf> {
    if let Some(path) = override_path {
        return Ok(path.to_path_buf());
    }
    let home = dirs::home_dir().context("could not determine home directory for session state")?;
    Ok(home.join(".hermes-rs/state.db"))
}

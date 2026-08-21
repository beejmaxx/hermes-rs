//! Operator inspection of durable tool-effect records.

use std::path::Path;

use anyhow::Context;
use domain::ToolEffect;
use ports::EffectLedger;

use super::state::state_path;
use crate::adapters::SqliteEffectLedger;

/// Print invocations left without a terminal disposition after interruption.
pub fn list_pending_effects(state_override: Option<&Path>) -> anyhow::Result<()> {
    let state = state_path(state_override)?;
    let mut ledger = SqliteEffectLedger::open(&state)
        .with_context(|| format!("could not open effect ledger {}", state.display()))?;
    let pending = ledger.pending()?;
    if pending.is_empty() {
        println!("No pending effects.");
        return Ok(());
    }
    for effect in pending {
        println!(
            "{}\t{}\t{}\t{}",
            effect.plan.invocation_id,
            effect.execution_scope,
            effect.plan.name,
            effect_name(effect.plan.effect)
        );
    }
    Ok(())
}

const fn effect_name(effect: ToolEffect) -> &'static str {
    match effect {
        ToolEffect::ReadOnly => "read_only",
        ToolEffect::ModelInference => "model_inference",
        ToolEffect::LocalMutation => "local_mutation",
        ToolEffect::ExternalMutation => "external_mutation",
        ToolEffect::ProcessControl => "process_control",
        ToolEffect::CredentialUse => "credential_use",
    }
}

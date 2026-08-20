//! Live model-provider adapters below the kernel-owned provider port.

mod openai;

pub use openai::{OpenAiCompatibleProvider, OpenAiProviderConfigError};

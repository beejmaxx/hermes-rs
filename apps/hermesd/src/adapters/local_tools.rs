//! Root-confined, read-only local tools.
//!
//! This adapter deliberately exposes no process execution or mutation. Paths
//! are canonicalized before use and must remain under one immutable root.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use domain::{
    PlannedToolCall, ToolArguments, ToolCall, ToolEffect, ToolResultStatus, ToolTerminal,
};
use futures_util::{FutureExt, future::BoxFuture};
use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use ports::{ToolBroker, ToolBrokerError};
use regex::Regex;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use thiserror::Error;

const DEFAULT_READ_LINES: usize = 2_000;
const MAX_READ_LINES: usize = 2_000;
const MAX_READ_BYTES: u64 = 10 * 1024 * 1024;
const MAX_RESULT_CHARS: usize = 100_000;
const MAX_RENDERED_LINE_BYTES: usize = 4_000;
const DEFAULT_SEARCH_RESULTS: usize = 50;
const MAX_SEARCH_RESULTS: usize = 500;
const MAX_SEARCH_OFFSET: usize = 10_000;
const MAX_SEARCH_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_WALK_FILES: usize = 100_000;
const PROTECTED_FILE_NAMES: &[&str] = &[
    ".anthropic_oauth.json",
    ".env",
    ".env.development",
    ".env.local",
    ".env.production",
    ".env.staging",
    ".env.test",
    ".envrc",
    ".git-credentials",
    ".netrc",
    ".npmrc",
    ".pgpass",
    ".pypirc",
    "auth.json",
    "auth.lock",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
];
const PROTECTED_DIRECTORY_NAMES: &[&str] =
    &[".aws", ".docker", ".git", ".gnupg", ".hermes-rs", ".kube", ".ssh", "mcp-tokens"];

/// Configuration failure for the read-only local tool broker.
#[derive(Debug, Error)]
pub enum LocalToolsConfigError {
    /// The configured root could not be canonicalized.
    #[error("could not resolve tool root {path}: {source}")]
    ResolveRoot {
        /// Configured root path.
        path: PathBuf,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// The configured root is not a directory.
    #[error("tool root is not a directory: {0}")]
    RootNotDirectory(PathBuf),
    /// The selected root is itself a protected credential directory.
    #[error("tool root is a protected credential directory: {0}")]
    ProtectedRoot(PathBuf),
    /// Execution scope must be stable and non-empty.
    #[error("execution scope must be non-empty")]
    EmptyExecutionScope,
}

/// Broker exposing only root-confined `read_file` and `search_files`.
pub struct ReadOnlyLocalTools {
    root: PathBuf,
    execution_scope: String,
}

impl ReadOnlyLocalTools {
    /// Construct a broker rooted at one canonical directory.
    pub fn new(
        root: impl AsRef<Path>,
        execution_scope: impl Into<String>,
    ) -> Result<Self, LocalToolsConfigError> {
        let supplied = root.as_ref();
        let root = fs::canonicalize(supplied).map_err(|source| {
            LocalToolsConfigError::ResolveRoot { path: supplied.to_path_buf(), source }
        })?;
        if !root.is_dir() {
            return Err(LocalToolsConfigError::RootNotDirectory(root));
        }
        if is_protected_path(&root) {
            return Err(LocalToolsConfigError::ProtectedRoot(root));
        }
        let execution_scope = execution_scope.into();
        if execution_scope.is_empty() {
            return Err(LocalToolsConfigError::EmptyExecutionScope);
        }
        Ok(Self { root, execution_scope })
    }

    /// Canonical filesystem root visible to this broker.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Frozen OpenAI-compatible schemas for the tools this broker exposes.
    #[must_use]
    pub fn catalog() -> Vec<Value> {
        vec![read_file_schema(), search_files_schema()]
    }

    fn execute_one(&self, call: &PlannedToolCall) -> ToolTerminal {
        let result = match call.name.as_str() {
            "read_file" => decode_arguments::<ReadFileArgs>(&call.arguments)
                .and_then(|arguments| self.read_file(&arguments)),
            "search_files" => decode_arguments::<SearchFilesArgs>(&call.arguments)
                .and_then(|arguments| self.search_files(&arguments)),
            name => Err(format!("unknown read-only tool {name:?}")),
        };
        let (status, content) = match result {
            Ok(content) => (ToolResultStatus::Succeeded, content),
            Err(error) => (ToolResultStatus::Failed, error),
        };
        ToolTerminal {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            status,
            content,
            execution_key: call.execution_key.clone(),
            effect: ToolEffect::ReadOnly,
            receipt: None,
        }
    }

    fn resolve_existing(&self, supplied: &str) -> Result<PathBuf, String> {
        if supplied.is_empty() {
            return Err("path must be non-empty".into());
        }
        let supplied = Path::new(supplied);
        let candidate =
            if supplied.is_absolute() { supplied.to_path_buf() } else { self.root.join(supplied) };
        let resolved = fs::canonicalize(&candidate)
            .map_err(|error| format!("could not resolve {}: {error}", candidate.display()))?;
        if !resolved.starts_with(&self.root) {
            return Err(format!(
                "path {} escapes tool root {}",
                candidate.display(),
                self.root.display()
            ));
        }
        if is_protected_path(&resolved) {
            return Err(format!(
                "access denied: {} is a protected credential or repository-internal path",
                candidate.display()
            ));
        }
        Ok(resolved)
    }

    fn read_file(&self, arguments: &ReadFileArgs) -> Result<String, String> {
        if arguments.offset == 0 {
            return Err("offset must be at least 1".into());
        }
        if arguments.limit == 0 || arguments.limit > MAX_READ_LINES {
            return Err(format!("limit must be between 1 and {MAX_READ_LINES}"));
        }
        let path = self.resolve_existing(&arguments.path)?;
        let metadata = fs::metadata(&path)
            .map_err(|error| format!("could not inspect {}: {error}", path.display()))?;
        if !metadata.is_file() {
            return Err(format!("path is not a regular file: {}", path.display()));
        }
        if metadata.len() > MAX_READ_BYTES {
            return Err(format!(
                "file is too large for read_file ({} bytes; maximum {MAX_READ_BYTES})",
                metadata.len()
            ));
        }
        let content = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {} as UTF-8 text: {error}", path.display()))?;
        let lines = content.lines().collect::<Vec<_>>();
        let start = arguments.offset - 1;
        if start >= lines.len() {
            return Ok(format!(
                "Offset {} is beyond the end of {} ({} lines).",
                arguments.offset,
                display_path(&self.root, &path),
                lines.len()
            ));
        }

        let mut output = String::new();
        let end = start.saturating_add(arguments.limit).min(lines.len());
        let mut next_offset = None;
        for (index, line) in lines[start..end].iter().enumerate() {
            let line_number = start + index + 1;
            let rendered_line = truncate_line(line);
            let rendered = format!("{line_number}|{rendered_line}\n");
            if output.len().saturating_add(rendered.len()) > MAX_RESULT_CHARS {
                next_offset = Some(line_number);
                break;
            }
            output.push_str(&rendered);
        }
        if next_offset.is_none() && end < lines.len() {
            next_offset = Some(end + 1);
        }
        if let Some(offset) = next_offset {
            output.push_str(&format!("[More content available; continue with offset={offset}]"));
        }
        Ok(output)
    }

    fn search_files(&self, arguments: &SearchFilesArgs) -> Result<String, String> {
        if arguments.pattern.is_empty() {
            return Err("pattern must be non-empty".into());
        }
        if arguments.limit == 0 || arguments.limit > MAX_SEARCH_RESULTS {
            return Err(format!("limit must be between 1 and {MAX_SEARCH_RESULTS}"));
        }
        if arguments.offset > MAX_SEARCH_OFFSET {
            return Err(format!("offset must not exceed {MAX_SEARCH_OFFSET}"));
        }
        let start = self.resolve_existing(&arguments.path)?;
        let file_filter = arguments.file_glob.as_deref().map(compile_glob).transpose()?;
        let mut candidates = collect_files(&start)?;
        candidates.sort();

        let match_cap = arguments.offset.saturating_add(arguments.limit).saturating_add(1);
        let matches = match arguments.target {
            SearchTarget::Files => {
                let matcher = compile_glob(&arguments.pattern)?;
                candidates
                    .into_iter()
                    .filter_map(|path| {
                        let relative = display_path(&self.root, &path);
                        glob_matches(&matcher, &relative, &path).then_some(relative)
                    })
                    .take(match_cap)
                    .collect::<Vec<_>>()
            }
            SearchTarget::Content => {
                let expression = Regex::new(&arguments.pattern)
                    .map_err(|error| format!("invalid search regex: {error}"))?;
                let mut found = Vec::new();
                for path in candidates {
                    if !file_filter.as_ref().is_none_or(|matcher| {
                        glob_matches(matcher, &display_path(&self.root, &path), &path)
                    }) {
                        continue;
                    }
                    let Ok(metadata) = fs::metadata(&path) else {
                        continue;
                    };
                    if metadata.len() > MAX_SEARCH_FILE_BYTES {
                        continue;
                    }
                    let Ok(content) = fs::read_to_string(&path) else {
                        continue;
                    };
                    let relative = display_path(&self.root, &path);
                    for (index, line) in
                        content.lines().enumerate().filter(|(_, line)| expression.is_match(line))
                    {
                        found.push(format!("{relative}:{}:{}", index + 1, truncate_line(line)));
                        if found.len() >= match_cap {
                            break;
                        }
                    }
                    if found.len() >= match_cap {
                        break;
                    }
                }
                found
            }
        };
        let available = matches.len().saturating_sub(arguments.offset);
        let selected =
            matches.into_iter().skip(arguments.offset).take(arguments.limit).collect::<Vec<_>>();
        if selected.is_empty() {
            return Ok("No matches found.".into());
        }
        let mut output = selected.join("\n");
        if available > selected.len() {
            output.push_str(&format!(
                "\n[More matches available; continue with offset={}]",
                arguments.offset + selected.len()
            ));
        }
        if output.len() > MAX_RESULT_CHARS {
            output = truncate_owned(output, MAX_RESULT_CHARS);
            output.push_str("\n[Results truncated at the output budget]");
        }
        Ok(output)
    }
}

impl ToolBroker for ReadOnlyLocalTools {
    fn plan(&mut self, calls: &[ToolCall]) -> Result<Vec<PlannedToolCall>, ToolBrokerError> {
        let mut seen = HashSet::with_capacity(calls.len());
        calls
            .iter()
            .map(|call| {
                if !seen.insert(&call.id) {
                    return Err(ToolBrokerError::new(format!(
                        "duplicate tool call id {}",
                        call.id
                    )));
                }
                Ok(PlannedToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                    execution_key: format!("{}:{}", self.execution_scope, call.id),
                    effect: ToolEffect::ReadOnly,
                    approval: None,
                })
            })
            .collect()
    }

    fn execute<'a>(
        &'a mut self,
        calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<ToolTerminal>, ToolBrokerError>> {
        async move { Ok(calls.iter().map(|call| self.execute_one(call)).collect()) }.boxed()
    }
}

fn decode_arguments<T: DeserializeOwned>(arguments: &ToolArguments) -> Result<T, String> {
    let object = Map::from_iter(arguments.0.clone());
    serde_json::from_value(Value::Object(object))
        .map_err(|error| format!("invalid tool arguments: {error}"))
}

fn collect_files(start: &Path) -> Result<Vec<PathBuf>, String> {
    if start.is_file() {
        return Ok(vec![start.to_path_buf()]);
    }
    if !start.is_dir() {
        return Err(format!("search path is not a file or directory: {}", start.display()));
    }
    let mut files = Vec::new();
    for entry in WalkBuilder::new(start).follow_links(false).standard_filters(true).build() {
        let entry =
            entry.map_err(|error| format!("could not walk {}: {error}", start.display()))?;
        if entry.file_type().is_some_and(|kind| kind.is_file()) {
            let path = entry.into_path();
            if is_protected_path(&path) {
                continue;
            }
            files.push(path);
            if files.len() > MAX_WALK_FILES {
                return Err(format!(
                    "search path contains more than {MAX_WALK_FILES} files; narrow the path"
                ));
            }
        }
    }
    Ok(files)
}

fn compile_glob(pattern: &str) -> Result<GlobMatcher, String> {
    Glob::new(pattern)
        .map(|glob| glob.compile_matcher())
        .map_err(|error| format!("invalid file glob: {error}"))
}

fn glob_matches(matcher: &GlobMatcher, relative: &str, path: &Path) -> bool {
    matcher.is_match(relative)
        || path.file_name().is_some_and(|name| matcher.is_match(Path::new(name)))
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).to_string_lossy().into_owned()
}

fn is_protected_path(path: &Path) -> bool {
    if path
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .is_some_and(|name| PROTECTED_FILE_NAMES.contains(&name.as_str()))
    {
        return true;
    }
    path.components().any(|component| {
        let name = component.as_os_str().to_string_lossy().to_ascii_lowercase();
        PROTECTED_DIRECTORY_NAMES.contains(&name.as_str())
    })
}

fn truncate_owned(mut value: String, max_bytes: usize) -> String {
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

fn truncate_line(value: &str) -> String {
    if value.len() <= MAX_RENDERED_LINE_BYTES {
        return value.into();
    }
    let mut end = MAX_RENDERED_LINE_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [line truncated]", &value[..end])
}

fn read_file_schema() -> Value {
    json!({
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a UTF-8 text file under the configured workspace root with line numbers and pagination.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path relative to the workspace root, or an absolute path within it."},
                    "offset": {"type": "integer", "minimum": 1, "default": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_READ_LINES, "default": DEFAULT_READ_LINES}
                },
                "required": ["path"],
                "additionalProperties": false
            }
        }
    })
}

fn search_files_schema() -> Value {
    json!({
        "type": "function",
        "function": {
            "name": "search_files",
            "description": "Search file contents with a regular expression or find files by glob under the configured workspace root.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {"type": "string"},
                    "target": {"type": "string", "enum": ["content", "files"], "default": "content"},
                    "path": {"type": "string", "default": "."},
                    "file_glob": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_SEARCH_RESULTS, "default": DEFAULT_SEARCH_RESULTS},
                    "offset": {"type": "integer", "minimum": 0, "default": 0}
                },
                "required": ["pattern"],
                "additionalProperties": false
            }
        }
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileArgs {
    path: String,
    #[serde(default = "one")]
    offset: usize,
    #[serde(default = "default_read_lines")]
    limit: usize,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SearchTarget {
    #[default]
    Content,
    Files,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchFilesArgs {
    pattern: String,
    #[serde(default)]
    target: SearchTarget,
    #[serde(default = "current_directory")]
    path: String,
    #[serde(default)]
    file_glob: Option<String>,
    #[serde(default = "default_search_results")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

const fn one() -> usize {
    1
}

const fn default_read_lines() -> usize {
    DEFAULT_READ_LINES
}

fn current_directory() -> String {
    ".".into()
}

const fn default_search_results() -> usize {
    DEFAULT_SEARCH_RESULTS
}

#[cfg(test)]
mod tests {
    use std::fs;

    use domain::{ToolArguments, ToolCall, ToolCallId, ToolResultStatus};
    use futures_executor::block_on;
    use ports::ToolBroker;
    use serde_json::json;
    use tempfile::tempdir;

    use super::ReadOnlyLocalTools;

    fn call(id: &str, name: &str, arguments: &[(&str, serde_json::Value)]) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id).unwrap_or_else(|error| unreachable!("valid test id: {error}")),
            name: name.into(),
            arguments: ToolArguments(
                arguments.iter().map(|(key, value)| ((*key).into(), value.clone())).collect(),
            ),
        }
    }

    fn execute(
        tools: &mut ReadOnlyLocalTools,
        call: ToolCall,
    ) -> Result<domain::ToolTerminal, Box<dyn std::error::Error>> {
        let plans = tools.plan(&[call])?;
        let mut terminals = block_on(tools.execute(&plans))?;
        terminals.pop().ok_or_else(|| "missing terminal".into())
    }

    #[test]
    fn reads_with_line_numbers_and_pagination() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        fs::write(root.path().join("notes.txt"), "one\ntwo\nthree\n")?;
        let mut tools = ReadOnlyLocalTools::new(root.path(), "test")?;
        let terminal = execute(
            &mut tools,
            call(
                "call-read",
                "read_file",
                &[("path", json!("notes.txt")), ("offset", json!(2)), ("limit", json!(1))],
            ),
        )?;
        assert_eq!(terminal.status, ToolResultStatus::Succeeded);
        assert_eq!(terminal.content, "2|two\n[More content available; continue with offset=3]");
        Ok(())
    }

    #[test]
    fn truncates_a_giant_line_without_stalling_pagination() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempdir()?;
        fs::write(root.path().join("giant.txt"), format!("{}\ntail\n", "x".repeat(8_000)))?;
        let mut tools = ReadOnlyLocalTools::new(root.path(), "test")?;
        let terminal = execute(
            &mut tools,
            call("call-giant", "read_file", &[("path", json!("giant.txt")), ("limit", json!(1))]),
        )?;
        assert_eq!(terminal.status, ToolResultStatus::Succeeded);
        assert!(terminal.content.contains("[line truncated]"));
        assert!(terminal.content.ends_with("[More content available; continue with offset=2]"));
        Ok(())
    }

    #[test]
    fn searches_content_and_file_names() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        fs::create_dir(root.path().join("src"))?;
        fs::write(root.path().join("src/lib.rs"), "pub fn answer() -> u8 { 42 }\n")?;
        fs::write(root.path().join("README.md"), "nothing here\n")?;
        let mut tools = ReadOnlyLocalTools::new(root.path(), "test")?;

        let content = execute(
            &mut tools,
            call(
                "call-search",
                "search_files",
                &[("pattern", json!("answer")), ("file_glob", json!("*.rs"))],
            ),
        )?;
        assert_eq!(content.status, ToolResultStatus::Succeeded);
        assert_eq!(content.content, "src/lib.rs:1:pub fn answer() -> u8 { 42 }");

        let files = execute(
            &mut tools,
            call(
                "call-files",
                "search_files",
                &[("pattern", json!("*.md")), ("target", json!("files"))],
            ),
        )?;
        assert_eq!(files.content, "README.md");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlink_that_escapes_the_root() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let root = tempdir()?;
        let outside = tempdir()?;
        fs::write(outside.path().join("secret.txt"), "outside")?;
        symlink(outside.path().join("secret.txt"), root.path().join("escape.txt"))?;
        let mut tools = ReadOnlyLocalTools::new(root.path(), "test")?;
        let terminal = execute(
            &mut tools,
            call("call-escape", "read_file", &[("path", json!("escape.txt"))]),
        )?;
        assert_eq!(terminal.status, ToolResultStatus::Failed);
        assert!(terminal.content.contains("escapes tool root"));
        Ok(())
    }

    #[test]
    fn denies_project_credentials_but_allows_documented_examples()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        fs::write(root.path().join(".env"), "API_KEY=secret\n")?;
        fs::write(root.path().join(".env.example"), "API_KEY=\n")?;
        let mut tools = ReadOnlyLocalTools::new(root.path(), "test")?;

        let denied =
            execute(&mut tools, call("call-env", "read_file", &[("path", json!(".env"))]))?;
        assert_eq!(denied.status, ToolResultStatus::Failed);
        assert!(denied.content.contains("protected credential"));

        let example = execute(
            &mut tools,
            call("call-example", "read_file", &[("path", json!(".env.example"))]),
        )?;
        assert_eq!(example.status, ToolResultStatus::Succeeded);
        assert_eq!(example.content, "1|API_KEY=\n");
        Ok(())
    }

    #[test]
    fn unknown_tools_fail_without_dispatching_an_effect() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempdir()?;
        let mut tools = ReadOnlyLocalTools::new(root.path(), "test")?;
        let terminal = execute(&mut tools, call("call-unknown", "delete_everything", &[]))?;
        assert_eq!(terminal.status, ToolResultStatus::Failed);
        assert_eq!(terminal.content, "unknown read-only tool \"delete_everything\"");
        Ok(())
    }
}

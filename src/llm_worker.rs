use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use reqwest::blocking::Client;
use serde_json::Value as JsonValue;
use triblespace::core::blob::Bytes;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::import::json::JsonObjectImporter;
use triblespace::core::repo::Workspace;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle, NsTAIInterval, ShortString};
use triblespace::prelude::*;

use crate::blob_refs::{PromptChunk, split_blob_refs, unknown_blob_handle_from_hex};
use crate::chat_prompt::{ChatMessage, ChatRole};
use crate::config::Config;
use crate::repo_util::{
    close_repo, current_branch_head, ensure_worker_name, init_repo, load_text, pull_workspace,
    push_workspace, refresh_cached_checkout, seed_metadata,
};
use crate::schema::llm_chat;
use crate::time_util::{epoch_interval, interval_key, now_epoch};

#[derive(Debug, Clone)]
struct LlmRequest {
    id: Id,
    prompt: Value<Handle<Blake3, LongString>>,
    model: Option<Value<ShortString>>,
    requested_at: Option<Value<NsTAIInterval>>,
}

#[derive(Default)]
struct LlmRequestIndex {
    requests: HashMap<Id, LlmRequest>,
    in_progress_by_worker: HashSet<Id>,
    done: HashSet<Id>,
}

#[derive(Debug)]
struct OpenAIResult {
    output_text: String,
    reasoning_text: Option<String>,
    raw: String,
    response_id: Option<String>,
}

struct ChatCompletionsClient {
    client: Client,
    endpoint_url: String,
    api_key: Option<String>,
    stream: bool,
}

const SEND_MAX_ATTEMPTS: usize = 5;
const SEND_RETRY_BASE_MS: u64 = 500;
const MAX_INLINE_IMAGES_PER_PROMPT: usize = 4;
const MAX_INLINE_IMAGE_BYTES: usize = 5 * 1024 * 1024;

fn chat_completions_url(api_base_url: &str) -> String {
    let base = api_base_url.trim().trim_end_matches('/');
    if base.ends_with("/chat/completions") || base.ends_with("/completions") {
        return base.to_string();
    }
    if let Some(base) = base.strip_suffix("/responses") {
        return format!("{base}/chat/completions");
    }
    format!("{base}/chat/completions")
}

impl ChatCompletionsClient {
    fn new(api_base_url: &str, api_key: Option<String>, stream: bool) -> Result<Self> {
        let client = Client::builder().build().context("build http client")?;
        let endpoint_url = chat_completions_url(api_base_url);
        Ok(Self {
            client,
            endpoint_url,
            api_key,
            stream,
        })
    }

    fn send_payload(&self, payload: &JsonValue) -> Result<OpenAIResult> {
        let mut last_error = None;
        for attempt in 1..=SEND_MAX_ATTEMPTS {
            match self.send_payload_once(payload) {
                Ok(result) => return Ok(result),
                Err(err) => {
                    eprintln!(
                        "warning: llm send attempt {attempt}/{SEND_MAX_ATTEMPTS} to {} failed: {err:#}",
                        self.endpoint_url
                    );
                    last_error = Some(err);
                    if attempt < SEND_MAX_ATTEMPTS {
                        let backoff_ms = SEND_RETRY_BASE_MS * (1_u64 << (attempt - 1));
                        sleep(Duration::from_millis(backoff_ms));
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("request failed without error detail")))
    }

    fn send_payload_once(&self, payload: &JsonValue) -> Result<OpenAIResult> {
        let response = self.send_request(payload).context("send request")?;
        if response.status().is_success() {
            return self.parse_response(response);
        }

        let status = response.status();
        // Best-effort body capture for debugging; don't assume it's JSON.
        let body = response
            .text()
            .unwrap_or_else(|_| "<failed to read error body>".to_string());

        bail!(
            "request failed: HTTP {} for url ({}){}",
            status,
            self.endpoint_url,
            if body.trim().is_empty() {
                "".to_string()
            } else {
                format!(": {}", body.trim())
            }
        );
    }

    fn send_request(&self, payload: &JsonValue) -> Result<reqwest::blocking::Response> {
        let mut request = self.client.post(&self.endpoint_url);
        if let Some(api_key) = self.api_key.as_ref() {
            request = request.bearer_auth(api_key);
        }
        request.json(payload).send().context("send http request")
    }

    fn parse_response(&self, response: reqwest::blocking::Response) -> Result<OpenAIResult> {
        if self.stream {
            parse_stream(response)
        } else {
            let json: JsonValue = response.json().context("read response json")?;
            let output_text = extract_output_text(&json);
            let reasoning_text = extract_reasoning_text(&json);
            let raw = serde_json::to_string(&json).context("serialize response")?;
            let response_id = extract_response_id(&json);
            Ok(OpenAIResult {
                output_text,
                reasoning_text,
                raw,
                response_id,
            })
        }
    }
}

pub(crate) fn run_llm_loop(
    config: Config,
    worker_id: Id,
    poll_ms: u64,
    stop: Option<Arc<AtomicBool>>,
) -> Result<()> {
    let (mut repo, branch_id) = init_repo(&config).context("open triblespace repo")?;
    let result = (|| -> Result<()> {
        seed_metadata(&mut repo)?;
        let label = format!("llm-{}", id_prefix(worker_id));
        ensure_worker_name(&mut repo, branch_id, worker_id, &label)?;
        let mut cached_head = None;
        let mut cached_catalog = TribleSet::new();
        let mut request_index = LlmRequestIndex::default();

        let client = ChatCompletionsClient::new(
            config.llm.base_url.as_str(),
            config.llm.api_key.clone(),
            config.llm.stream,
        )?;

        loop {
            if stop_requested(&stop) {
                break;
            }

            let branch_head = current_branch_head(&mut repo, branch_id)?;
            if branch_head == cached_head {
                sleep(Duration::from_millis(poll_ms));
                continue;
            }

            let mut ws = pull_workspace(&mut repo, branch_id, "pull workspace")?;
            let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
            request_index.apply_delta(&cached_catalog, &delta, worker_id);
            let Some(request) = request_index.next_pending() else {
                sleep(Duration::from_millis(poll_ms));
                continue;
            };

            if stop_requested(&stop) {
                break;
            }

            let prompt = load_text(&mut ws, request.prompt).context("load prompt")?;
            let model = request
                .model
                .map(|value| String::from_value(&value))
                .unwrap_or_else(|| config.llm.model.clone());

            let attempt: u64 = 1;
            let messages: Vec<ChatMessage> = match serde_json::from_str(prompt.as_str()) {
                Ok(messages) => messages,
                Err(err) => {
                    let finished_at = epoch_interval(now_epoch());
                    let result_id = ufoid();
                    let handle = ws.put(format!("parse chat prompt: {err}"));
                    let mut change = TribleSet::new();
                    change += entity! { &result_id @
                        llm_chat::kind: llm_chat::kind_result,
                        llm_chat::about_request: request.id,
                        llm_chat::finished_at: finished_at,
                        llm_chat::attempt: attempt,
                        llm_chat::error: handle,
                    };
                    ws.commit(change, None, Some("llm_chat result (prompt parse error)"));
                    push_workspace(&mut repo, &mut ws).context("push prompt parse error")?;
                    sleep(Duration::from_millis(poll_ms));
                    continue;
                }
            };
            let payload_messages = build_payload_messages(&mut ws, model.as_str(), &messages);
            let payload = build_payload(
                &model,
                config.llm.stream,
                config.llm.max_output_tokens,
                payload_messages,
            );
            let request_raw =
                serde_json::to_string(&payload).context("serialize request payload")?;

            let started_at = epoch_interval(now_epoch());
            let in_progress_id = ufoid();
            let request_raw_handle = ws.put(request_raw);

            let mut change = TribleSet::new();
            change += entity! { ExclusiveId::force_ref(&request.id) @
                llm_chat::request_raw: request_raw_handle,
            };
            change += entity! { &in_progress_id @
                llm_chat::kind: llm_chat::kind_in_progress,
                llm_chat::about_request: request.id,
                llm_chat::started_at: started_at,
                llm_chat::worker: worker_id,
                llm_chat::attempt: attempt,
            };
            ws.commit(change, None, Some("llm_chat in_progress"));
            push_workspace(&mut repo, &mut ws).context("push in_progress")?;

            let result = client.send_payload(&payload);

            let finished_at = epoch_interval(now_epoch());
            let result_id = ufoid();
            let mut change = TribleSet::new();
            change += entity! { &result_id @
                llm_chat::kind: llm_chat::kind_result,
                llm_chat::about_request: request.id,
                llm_chat::finished_at: finished_at,
                llm_chat::attempt: attempt,
            };

            let mut import_data = None;
            let mut import_metadata = None;

            match result {
                Ok(result) => {
                    let response_id = result.response_id.clone();
                    let raw_blob = result.raw.clone().to_blob();
                    let output_handle = ws.put(result.output_text);
                    let raw_handle = ws.put(result.raw);
                    change += entity! { &result_id @
                        llm_chat::output_text: output_handle,
                        llm_chat::response_raw: raw_handle,
                    };
                    if let Some(reasoning_text) = result.reasoning_text {
                        let handle = ws.put(reasoning_text);
                        change += entity! { &result_id @
                            llm_chat::reasoning_text: handle,
                        };
                    }
                    if let Some(response_id) = response_id {
                        let response_id_handle = ws.put(response_id);
                        change += entity! { &result_id @
                            llm_chat::response_id: response_id_handle,
                        };
                    }

                    let mut import_blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer =
                        JsonObjectImporter::<_, Blake3>::new(&mut import_blobs, None);
                    match importer.import_blob(raw_blob) {
                        Ok(fragment) => {
                            let metadata = importer
                                .metadata()
                                .context("build response import metadata")?
                                .into_facts();
                            let import_reader = import_blobs
                                .reader()
                                .context("read response import blobs")?;
                            for (_, blob) in import_reader.iter() {
                                ws.put::<UnknownBlob, _>(blob.bytes.clone());
                            }

                            for root in fragment.exports() {
                                change += entity! { &result_id @
                                    llm_chat::response_json_root: root,
                                };
                            }

                            import_data = Some(fragment);
                            import_metadata = Some(metadata);
                        }
                        Err(err) => {
                            eprintln!("Failed to import response JSON: {err}");
                        }
                    }
                }
                Err(err) => {
                    let handle = ws.put(format!("{err:#}"));
                    change += entity! { &result_id @
                        llm_chat::error: handle,
                    };
                }
            }

            if let (Some(data), Some(metadata)) = (import_data, import_metadata) {
                ws.commit(data, Some(metadata), Some("import response json"));
            }
            ws.commit(change, None, Some("llm_chat result"));
            push_workspace(&mut repo, &mut ws).context("push result")?;
        }

        Ok(())
    })();

    if let Err(err) = close_repo(repo) {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    result
}

fn stop_requested(stop: &Option<Arc<AtomicBool>>) -> bool {
    stop.as_ref()
        .map(|flag| flag.load(Ordering::Relaxed))
        .unwrap_or(false)
}

impl LlmRequestIndex {
    fn apply_delta(&mut self, updated: &TribleSet, delta: &TribleSet, worker_id: Id) {
        if delta.is_empty() {
            return;
        }

        for (request_id, prompt) in find!(
            (request_id: Id, prompt: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?request_id @
                llm_chat::kind: llm_chat::kind_request,
                llm_chat::prompt: ?prompt,
            }])
        ) {
            self.requests.insert(
                request_id,
                LlmRequest {
                    id: request_id,
                    prompt,
                    model: None,
                    requested_at: None,
                },
            );
        }

        for (request_id, model) in find!(
            (request_id: Id, model: Value<ShortString>),
            pattern_changes!(updated, delta, [{
                ?request_id @ llm_chat::model: ?model
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.model = Some(model);
            }
        }

        for (request_id, requested_at) in find!(
            (request_id: Id, requested_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?request_id @ llm_chat::requested_at: ?requested_at
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.requested_at = Some(requested_at);
            }
        }

        for (request_id, in_progress_worker_id) in find!(
            (
                request_id: Id,
                in_progress_worker_id: Id
            ),
            pattern_changes!(updated, delta, [{
                _?event @
                llm_chat::kind: llm_chat::kind_in_progress,
                llm_chat::about_request: ?request_id,
                llm_chat::worker: ?in_progress_worker_id,
            }])
        ) {
            if in_progress_worker_id == worker_id {
                self.in_progress_by_worker.insert(request_id);
            }
        }

        for (request_id,) in find!(
            (request_id: Id),
            pattern_changes!(updated, delta, [{
                _?event @
                llm_chat::kind: llm_chat::kind_result,
                llm_chat::about_request: ?request_id,
            }])
        ) {
            self.done.insert(request_id);
        }
    }

    fn next_pending(&self) -> Option<LlmRequest> {
        let mut candidates: Vec<LlmRequest> = self
            .requests
            .values()
            .filter(|req| {
                !self.in_progress_by_worker.contains(&req.id) && !self.done.contains(&req.id)
            })
            .cloned()
            .collect();
        candidates.sort_by_key(|req| req.requested_at.map(interval_key).unwrap_or(i128::MIN));
        candidates.into_iter().next()
    }
}

fn build_payload_messages(
    ws: &mut Workspace<Pile>,
    model: &str,
    messages: &[ChatMessage],
) -> Vec<JsonValue> {
    let supports_images = model_supports_images(model);
    let mut out = Vec::new();

    for message in messages {
        let role = match message.role {
            ChatRole::System => "system",
            ChatRole::User => "user",
            ChatRole::Assistant => "assistant",
        };

        if message.role == ChatRole::User && supports_images {
            let chunks = split_blob_refs(message.content.as_str());
            let has_blob = chunks.iter().any(|chunk| {
                if let PromptChunk::Blob(_) = chunk {
                    true
                } else {
                    false
                }
            });
            if has_blob {
                let content = build_prompt_input_content(ws, model, message.content.as_str());
                out.push(serde_json::json!({ "role": role, "content": content }));
                continue;
            }
        }

        out.push(serde_json::json!({ "role": role, "content": message.content.as_str() }));
    }

    out
}

fn build_payload(
    model: &str,
    stream: bool,
    max_tokens: u64,
    messages: Vec<JsonValue>,
) -> JsonValue {
    let max_tokens = max_tokens.max(1);
    serde_json::json!({
        "model": model,
        "messages": messages,
        "stream": stream,
        "max_tokens": max_tokens,
    })
}

fn build_prompt_input_content(
    ws: &mut Workspace<Pile>,
    model: &str,
    prompt: &str,
) -> Vec<JsonValue> {
    let supports_images = model_supports_images(model);
    let mut content = Vec::new();
    let mut images_added = 0usize;

    for chunk in split_blob_refs(prompt) {
        match chunk {
            PromptChunk::Text(text) => {
                if !text.is_empty() {
                    content.push(serde_json::json!({
                        "type": "text",
                        "text": text,
                    }));
                }
            }
            PromptChunk::Blob(blob_ref) => {
                if !supports_images {
                    content.push(serde_json::json!({
                        "type": "text",
                        "text": format_blob_fallback(blob_ref.raw.as_str(), "vision unavailable for current model"),
                    }));
                    continue;
                }
                if images_added >= MAX_INLINE_IMAGES_PER_PROMPT {
                    content.push(serde_json::json!({
                        "type": "text",
                        "text": format_blob_fallback(blob_ref.raw.as_str(), "image limit reached"),
                    }));
                    continue;
                }
                match resolve_blob_image_data_url(
                    ws,
                    &blob_ref.digest_hex,
                    blob_ref.mime.as_deref(),
                ) {
                    Ok(data_url) => {
                        content.push(serde_json::json!({
                            "type": "image_url",
                            "image_url": {"url": data_url},
                        }));
                        images_added += 1;
                    }
                    Err(reason) => {
                        content.push(serde_json::json!({
                            "type": "text",
                            "text": format_blob_fallback(blob_ref.raw.as_str(), reason.as_str()),
                        }));
                    }
                }
            }
        }
    }

    if content.is_empty() {
        content.push(serde_json::json!({
            "type": "text",
            "text": prompt,
        }));
    }
    content
}

fn resolve_blob_image_data_url(
    ws: &mut Workspace<Pile>,
    digest_hex: &str,
    mime_hint: Option<&str>,
) -> std::result::Result<String, String> {
    let handle =
        unknown_blob_handle_from_hex(digest_hex).ok_or_else(|| "bad blob digest".to_string())?;
    let bytes: Bytes = ws
        .get(handle)
        .map_err(|_| "blob not found in pile".to_string())?;
    if bytes.len() > MAX_INLINE_IMAGE_BYTES {
        return Err(format!(
            "image too large ({} bytes > {} bytes)",
            bytes.len(),
            MAX_INLINE_IMAGE_BYTES
        ));
    }
    let mime = match mime_hint.filter(|mime| mime.starts_with("image/")) {
        Some(mime) => mime.to_owned(),
        None => sniff_image_mime(bytes.as_ref())
            .map(str::to_owned)
            .ok_or_else(|| "blob is not a supported image format".to_string())?,
    };
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes.as_ref());
    Ok(format!("data:{mime};base64,{encoded}"))
}

fn sniff_image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 8 && bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some("image/png");
    }
    if bytes.len() >= 3 && bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image/jpeg");
    }
    if bytes.len() >= 6 && (bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) {
        return Some("image/gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    None
}

fn model_supports_images(model: &str) -> bool {
    let model = model.to_ascii_lowercase();
    if model.contains("mistral") {
        return true;
    }
    !(model.contains("codex")
        || model.contains("gpt-oss")
        || model.contains("llama")
        || model.contains("qwen")
        || model.contains("deepseek"))
}

fn format_blob_fallback(raw_marker: &str, reason: &str) -> String {
    format!("[blob omitted: {reason}] {raw_marker}")
}

fn parse_stream(response: reqwest::blocking::Response) -> Result<OpenAIResult> {
    let mut output_text = String::new();
    let mut raw_events = Vec::new();
    let mut response_id = None;
    let mut reasoning_parts = Vec::new();

    let reader = BufReader::new(response);
    for line in reader.lines() {
        let line = line.context("read stream")?;
        let Some(data) = line.strip_prefix("data: ") else {
            continue;
        };

        if data == "[DONE]" {
            break;
        }

        raw_events.push(data.to_owned());
        let Ok(event) = serde_json::from_str::<JsonValue>(data) else {
            continue;
        };

        if response_id.is_none() {
            response_id = extract_response_id(&event);
        }

        if let Some(choices) = event.get("choices").and_then(JsonValue::as_array) {
            for choice in choices {
                if let Some(delta) = choice.get("delta") {
                    if let Some(content) = delta.get("content").and_then(JsonValue::as_str) {
                        output_text.push_str(content);
                    }
                    collect_chat_reasoning_chunks(delta, &mut reasoning_parts);
                }
            }
        }
    }

    let raw = raw_events.join("\n");
    let reasoning_text = normalized_join(reasoning_parts);
    Ok(OpenAIResult {
        output_text,
        reasoning_text,
        raw,
        response_id,
    })
}

fn extract_response_id(response: &JsonValue) -> Option<String> {
    response
        .get("id")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

fn extract_output_text(response: &JsonValue) -> String {
    let Some(choices) = response.get("choices").and_then(JsonValue::as_array) else {
        return String::new();
    };

    let Some(first) = choices.first() else {
        return String::new();
    };

    // OpenAI-compatible chat completions: choices[0].message.content
    if let Some(message) = first.get("message") {
        if let Some(content) = message.get("content") {
            if let Some(text) = content.as_str() {
                return text.to_string();
            }
            if let Some(parts) = content.as_array() {
                let mut out = String::new();
                for part in parts {
                    if part.get("type").and_then(JsonValue::as_str) == Some("text")
                        && let Some(text) = part.get("text").and_then(JsonValue::as_str)
                    {
                        out.push_str(text);
                    }
                }
                return out;
            }
        }
    }

    // Legacy completions-style fallback: choices[0].text
    first
        .get("text")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn extract_reasoning_text(response: &JsonValue) -> Option<String> {
    let mut out = Vec::new();

    // OpenAI responses-style reasoning summaries.
    if let Some(output) = response.get("output").and_then(JsonValue::as_array) {
        for item in output {
            if item.get("type").and_then(JsonValue::as_str) != Some("reasoning") {
                continue;
            }
            if let Some(summary_items) = item.get("summary").and_then(JsonValue::as_array) {
                for entry in summary_items {
                    if entry.get("type").and_then(JsonValue::as_str) == Some("summary_text")
                        && let Some(text) = entry.get("text").and_then(JsonValue::as_str)
                    {
                        push_clean(&mut out, text);
                    }
                }
            }
        }
    }

    // Chat-completions style (including Mistral reasoning/thinking chunks where present).
    if let Some(choices) = response.get("choices").and_then(JsonValue::as_array) {
        for choice in choices {
            if let Some(message) = choice.get("message") {
                collect_chat_reasoning_chunks(message, &mut out);
            }
            if let Some(delta) = choice.get("delta") {
                collect_chat_reasoning_chunks(delta, &mut out);
            }
        }
    }

    normalized_join(out)
}

fn collect_chat_reasoning_chunks(node: &JsonValue, out: &mut Vec<String>) {
    for key in ["thinking", "reasoning", "reasoning_content"] {
        if let Some(text) = node.get(key).and_then(JsonValue::as_str) {
            push_clean(out, text);
        }
    }

    if let Some(content) = node.get("content") {
        if let Some(parts) = content.as_array() {
            for part in parts {
                let kind = part
                    .get("type")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default();
                if (kind == "thinking"
                    || kind == "reasoning"
                    || kind == "reasoning_content"
                    || kind == "summary_text")
                    && let Some(text) = part
                        .get("text")
                        .and_then(JsonValue::as_str)
                        .or_else(|| part.get("content").and_then(JsonValue::as_str))
                {
                    push_clean(out, text);
                }
            }
        }
    }

    if let Some(summary_items) = node.get("summary").and_then(JsonValue::as_array) {
        for entry in summary_items {
            if entry.get("type").and_then(JsonValue::as_str) == Some("summary_text")
                && let Some(text) = entry.get("text").and_then(JsonValue::as_str)
            {
                push_clean(out, text);
            }
        }
    }
}

fn push_clean(out: &mut Vec<String>, text: &str) {
    let trimmed = text.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
}

fn normalized_join(chunks: Vec<String>) -> Option<String> {
    if chunks.is_empty() {
        return None;
    }
    let joined = chunks.join("\n\n");
    if joined.trim().is_empty() {
        None
    } else {
        Some(joined)
    }
}

fn id_prefix(id: Id) -> String {
    let raw: [u8; 16] = id.into();
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use ed25519_dalek::SigningKey;
    use serde_json::json;
    use triblespace::core::blob::Blob;
    use triblespace::core::repo::Repository;
    use triblespace::core::repo::Workspace;
    use triblespace::core::repo::pile::Pile;
    use triblespace::prelude::valueschemas::{Blake3, Handle};
    use triblespace::prelude::*;

    use super::{
        Bytes, JsonValue, UnknownBlob, build_prompt_input_content, extract_reasoning_text,
    };

    fn test_repo_path() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "playground-llm-worker-test-{}-{ts}.pile",
            std::process::id()
        ))
    }

    fn put_test_png(ws: &mut Workspace<Pile<Blake3>>) -> String {
        // 1x1 PNG (black).
        let png_bytes: [u8; 68] = [
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, b'I', b'H',
            b'D', b'R', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x04, 0x00, 0x00,
            0x00, 0xB5, 0x1C, 0x0C, 0x02, 0x00, 0x00, 0x00, 0x0B, b'I', b'D', b'A', b'T', 0x78,
            0xDA, 0x63, 0xFC, 0xFF, 0x1F, 0x00, 0x03, 0x03, 0x02, 0x00, 0xED, 0x29, 0xEB, 0x14,
            0x00, 0x00, 0x00, 0x00, b'I', b'E', b'N', b'D', 0xAE, 0x42, 0x60, 0x82,
        ];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from(png_bytes.to_vec()));
        let handle: Value<Handle<Blake3, UnknownBlob>> = ws.put(blob);
        handle
            .raw
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect()
    }

    fn with_test_workspace<T>(f: impl FnOnce(&mut Workspace<Pile<Blake3>>) -> T) -> T {
        let path = test_repo_path();
        let mut pile = Pile::<Blake3>::open(path.as_path()).expect("open test pile");
        pile.restore().expect("restore test pile");
        let mut repo = Repository::new(pile, SigningKey::from_bytes(&[7u8; 32]));
        let branch_id = repo
            .create_branch("test", None)
            .expect("create test branch")
            .release();
        let mut ws = repo.pull(branch_id).expect("pull test workspace");
        let output = f(&mut ws);
        let _ = repo.close();
        let _ = std::fs::remove_file(path);
        output
    }

    #[test]
    fn extracts_openai_reasoning_summary() {
        let response = json!({
            "output": [
                {
                    "type": "reasoning",
                    "summary": [
                        {"type": "summary_text", "text": "Investigating branch mismatch"}
                    ]
                }
            ]
        });
        let reasoning = extract_reasoning_text(&response).expect("reasoning");
        assert!(reasoning.contains("Investigating branch mismatch"));
    }

    #[test]
    fn extracts_chat_thinking_chunks() {
        let response = json!({
            "choices": [
                {
                    "message": {
                        "content": [
                            {"type": "thinking", "text": "Need to inspect config first"},
                            {"type": "text", "text": "/workspace/faculties/orient.rs show"}
                        ]
                    }
                }
            ]
        });
        let reasoning = extract_reasoning_text(&response).expect("reasoning");
        assert_eq!(reasoning, "Need to inspect config first");
    }

    #[test]
    fn ignores_plain_assistant_text_without_thinking_fields() {
        let response = json!({
            "choices": [
                {
                    "message": {
                        "content": "echo hello"
                    }
                }
            ]
        });
        assert!(extract_reasoning_text(&response).is_none());
    }

    #[test]
    fn blob_marker_becomes_image_part_for_vision_models() {
        with_test_workspace(|ws| {
            let digest_hex = put_test_png(ws);
            let prompt = format!(
                "inspect this image ![sample](blob:blake3:{digest_hex}?mime=image%2Fpng&name=sample.png)"
            );
            let content = build_prompt_input_content(ws, "gpt-4.1", prompt.as_str());
            assert!(
                content
                    .iter()
                    .any(|part| part.get("type").and_then(JsonValue::as_str) == Some("image_url")),
                "expected one image_url part in prompt content"
            );
        });
    }

    #[test]
    fn blob_marker_falls_back_to_text_for_non_vision_models() {
        with_test_workspace(|ws| {
            let digest_hex = put_test_png(ws);
            let prompt = format!("![sample](blob:blake3:{digest_hex}?mime=image%2Fpng)");
            let content = build_prompt_input_content(ws, "gpt-oss-120b", prompt.as_str());
            assert_eq!(content.len(), 1);
            assert_eq!(
                content[0].get("type").and_then(JsonValue::as_str),
                Some("text")
            );
            let text = content[0]
                .get("text")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            assert!(text.contains("vision unavailable"));
        });
    }
}

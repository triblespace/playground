use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde_json::Value as JsonValue;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::import::json::JsonObjectImporter;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle, NsTAIInterval, ShortString};
use triblespace::prelude::*;

use crate::config::Config;
use crate::repo_util::{
    close_repo, current_branch_head, ensure_worker_name, init_repo, load_text, push_workspace,
    refresh_cached_checkout, seed_metadata,
};
use crate::schema::openai_responses;
use crate::time_util::{epoch_interval, interval_key, now_epoch};

#[derive(Debug, Clone)]
struct LlmRequest {
    id: Id,
    prompt: Value<Handle<Blake3, LongString>>,
    model: Option<Value<ShortString>>,
    previous_response_id: Option<Value<Handle<Blake3, LongString>>>,
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
    raw: String,
    response_id: Option<String>,
}

struct ResponsesClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    stream: bool,
}

const SEND_MAX_ATTEMPTS: usize = 3;
const SEND_RETRY_BASE_MS: u64 = 250;

impl ResponsesClient {
    fn new(base_url: &str, api_key: Option<String>, stream: bool) -> Result<Self> {
        let client = Client::builder().build().context("build http client")?;
        Ok(Self {
            client,
            base_url: base_url.to_owned(),
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
                    last_error = Some(err);
                    if attempt < SEND_MAX_ATTEMPTS {
                        sleep(Duration::from_millis(SEND_RETRY_BASE_MS * attempt as u64));
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("request failed without error detail")))
    }

    fn send_payload_once(&self, payload: &JsonValue) -> Result<OpenAIResult> {
        let mut request = self.client.post(&self.base_url);
        if let Some(api_key) = self.api_key.as_ref() {
            request = request.bearer_auth(api_key);
        }

        let response = request
            .json(payload)
            .send()
            .context("send request")?
            .error_for_status()
            .context("request failed")?;

        if self.stream {
            parse_stream(response)
        } else {
            let json: JsonValue = response.json().context("read response json")?;
            let output_text = extract_output_text(&json);
            let raw = serde_json::to_string(&json).context("serialize response")?;
            let response_id = extract_response_id(&json);
            Ok(OpenAIResult {
                output_text,
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

        let client = ResponsesClient::new(
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

            let mut ws = repo
                .pull(branch_id)
                .map_err(|err| anyhow::anyhow!("pull workspace: {err:?}"))?;
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
            let previous_response_id = request
                .previous_response_id
                .map(|value| load_text(&mut ws, value))
                .transpose()
                .context("load previous_response_id")?;

            let payload = build_payload(
                &model,
                &prompt,
                config.llm.stream,
                config.llm.reasoning_effort.as_deref(),
                previous_response_id.as_deref(),
            );
            let request_raw =
                serde_json::to_string(&payload).context("serialize request payload")?;

            let started_at = epoch_interval(now_epoch());
            let in_progress_id = ufoid();
            let attempt: u64 = 1;
            let request_raw_handle = ws.put(request_raw);

            let mut change = TribleSet::new();
            change += entity! { ExclusiveId::force_ref(&request.id) @
                openai_responses::request_raw: request_raw_handle,
            };
            change += entity! { &in_progress_id @
                openai_responses::kind: openai_responses::kind_in_progress,
                openai_responses::about_request: request.id,
                openai_responses::started_at: started_at,
                openai_responses::worker: worker_id,
                openai_responses::attempt: attempt,
            };
            ws.commit(change, None, Some("openai_responses in_progress"));
            push_workspace(&mut repo, &mut ws).context("push in_progress")?;

            let result = client.send_payload(&payload);

            let finished_at = epoch_interval(now_epoch());
            let result_id = ufoid();
            let mut change = TribleSet::new();
            change += entity! { &result_id @
                openai_responses::kind: openai_responses::kind_result,
                openai_responses::about_request: request.id,
                openai_responses::finished_at: finished_at,
                openai_responses::attempt: attempt,
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
                        openai_responses::output_text: output_handle,
                        openai_responses::response_raw: raw_handle,
                    };
                    if let Some(response_id) = response_id {
                        let response_id_handle = ws.put(response_id);
                        change += entity! { &result_id @
                            openai_responses::response_id: response_id_handle,
                        };
                    }

                    let mut import_blobs = MemoryBlobStore::<Blake3>::new();
                    let mut importer =
                        JsonObjectImporter::<_, Blake3>::new(&mut import_blobs, None);
                    match importer.import_blob(raw_blob) {
                        Ok(fragment) => {
                            let metadata = importer
                                .metadata()
                                .context("build response import metadata")?;
                            let import_reader = import_blobs
                                .reader()
                                .context("read response import blobs")?;
                            for (_, blob) in import_reader.iter() {
                                ws.put::<UnknownBlob, _>(blob.bytes.clone());
                            }

                            for root in fragment.exports() {
                                change += entity! { &result_id @
                                    openai_responses::response_json_root: root,
                                };
                            }

                            import_data = Some(fragment.into_facts());
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
                        openai_responses::error: handle,
                    };
                }
            }

            if let (Some(data), Some(metadata)) = (import_data, import_metadata) {
                ws.commit(data, Some(metadata), Some("import response json"));
            }
            ws.commit(change, None, Some("openai_responses result"));
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
                openai_responses::kind: openai_responses::kind_request,
                openai_responses::prompt: ?prompt,
            }])
        ) {
            self.requests.insert(
                request_id,
                LlmRequest {
                    id: request_id,
                    prompt,
                    model: None,
                    previous_response_id: None,
                    requested_at: None,
                },
            );
        }

        for (request_id, model) in find!(
            (request_id: Id, model: Value<ShortString>),
            pattern_changes!(updated, delta, [{
                ?request_id @ openai_responses::model: ?model
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.model = Some(model);
            }
        }

        for (request_id, requested_at) in find!(
            (request_id: Id, requested_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?request_id @ openai_responses::requested_at: ?requested_at
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.requested_at = Some(requested_at);
            }
        }

        for (request_id, previous_response_id) in find!(
            (
                request_id: Id,
                previous_response_id: Value<Handle<Blake3, LongString>>
            ),
            pattern_changes!(updated, delta, [{
                ?request_id @ openai_responses::previous_response_id: ?previous_response_id
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.previous_response_id = Some(previous_response_id);
            }
        }

        for (request_id, in_progress_worker_id) in find!(
            (
                request_id: Id,
                in_progress_worker_id: Id
            ),
            pattern_changes!(updated, delta, [{
                _?event @
                openai_responses::kind: openai_responses::kind_in_progress,
                openai_responses::about_request: ?request_id,
                openai_responses::worker: ?in_progress_worker_id,
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
                openai_responses::kind: openai_responses::kind_result,
                openai_responses::about_request: ?request_id,
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

fn build_payload(
    model: &str,
    prompt: &str,
    stream: bool,
    reasoning_effort: Option<&str>,
    previous_response_id: Option<&str>,
) -> JsonValue {
    let mut reasoning = serde_json::json!({
        "summary": "detailed",
    });
    if let Some(effort) = reasoning_effort {
        reasoning["effort"] = JsonValue::String(effort.to_string());
    }
    let mut payload = serde_json::json!({
        "model": model,
        "input": [{"role": "user", "content": prompt}],
        "reasoning": reasoning,
        "include": ["reasoning.encrypted_content"],
        "stream": stream,
        "store": true,
    });
    if let Some(previous_response_id) = previous_response_id {
        payload["previous_response_id"] = JsonValue::String(previous_response_id.to_string());
    }
    payload
}

fn parse_stream(response: reqwest::blocking::Response) -> Result<OpenAIResult> {
    let mut output_text = String::new();
    let mut raw_events = Vec::new();
    let mut completed = None;

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

        let Some(kind) = event.get("type").and_then(JsonValue::as_str) else {
            continue;
        };

        match kind {
            "response.output_text.delta" => {
                if let Some(delta) = event.get("delta").and_then(JsonValue::as_str) {
                    output_text.push_str(delta);
                }
            }
            "response.completed" => {
                completed = event.get("response").cloned();
            }
            "response.failed" => {
                let message = event
                    .get("response")
                    .and_then(|resp| resp.get("error"))
                    .and_then(|err| err.get("message"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("response failed");
                bail!("{message}");
            }
            _ => {}
        }
    }

    if output_text.is_empty() {
        if let Some(response) = completed.as_ref() {
            output_text = extract_output_text(response);
        }
    }

    let raw = if let Some(response) = completed {
        serde_json::to_string(&response).context("serialize response")?
    } else {
        raw_events.join("\n")
    };
    let response_id = if let Some(response) = serde_json::from_str::<JsonValue>(&raw).ok() {
        extract_response_id(&response)
    } else {
        None
    };
    Ok(OpenAIResult {
        output_text,
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
    let mut text = String::new();

    if let Some(output) = response.get("output").and_then(JsonValue::as_array) {
        for item in output {
            if let Some(item_type) = item.get("type").and_then(JsonValue::as_str)
                && item_type == "output_text"
                && let Some(content) = item.get("text").and_then(JsonValue::as_str)
            {
                text.push_str(content);
            }

            if let Some(content) = item.get("content").and_then(JsonValue::as_array) {
                for part in content {
                    if let Some(part_type) = part.get("type").and_then(JsonValue::as_str)
                        && part_type == "output_text"
                        && let Some(content) = part.get("text").and_then(JsonValue::as_str)
                    {
                        text.push_str(content);
                    }
                }
            }
        }
    }

    if text.is_empty()
        && let Some(output_text) = response.get("output_text").and_then(JsonValue::as_str)
    {
        text.push_str(output_text);
    }

    text
}

fn id_prefix(id: Id) -> String {
    let raw: [u8; 16] = id.into();
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

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
use crate::repo_util::{ensure_worker_shortname, init_repo, load_text, push_workspace, seed_metadata};
use crate::time_util::{epoch_interval, interval_key, now_epoch};
use crate::schema::openai_responses;

#[derive(Debug, Clone)]
struct LlmRequest {
    id: Id,
    prompt: Value<Handle<Blake3, LongString>>,
    model: Option<Value<ShortString>>,
    requested_at: Option<Value<NsTAIInterval>>,
}

#[derive(Debug)]
struct OpenAIResult {
    output_text: String,
    raw: String,
}

struct ResponsesClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    stream: bool,
}

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
            Ok(OpenAIResult { output_text, raw })
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
    seed_metadata(&mut repo)?;
    let label = format!("llm-{}", id_prefix(worker_id));
    ensure_worker_shortname(&mut repo, branch_id, worker_id, &label)?;

    let client = ResponsesClient::new(
        config.llm.base_url.as_str(),
        config.llm.api_key.clone(),
        config.llm.stream,
    )?;

    loop {
        if stop_requested(&stop) {
            break;
        }

        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow::anyhow!("pull workspace: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        let Some(request) = next_request(&catalog) else {
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

        let payload = build_payload(&model, &prompt, config.llm.stream);
        let request_raw = serde_json::to_string(&payload).context("serialize request payload")?;

        let started_at = epoch_interval(now_epoch());
        let in_progress_id = ufoid();
        let attempt: u64 = 1;
        let request_raw_handle = ws.put::<LongString, _>(request_raw.clone());

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

        let result = match client.send_payload(&payload) {
            Ok(result) => Ok(result),
            Err(err) => Err(err),
        };

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
                let output_handle = ws.put::<LongString, _>(result.output_text);
                let raw_handle = ws.put::<LongString, _>(result.raw.clone());
                change += entity! { &result_id @
                    openai_responses::output_text: output_handle,
                    openai_responses::response_raw: raw_handle,
                };

                let mut import_blobs = MemoryBlobStore::<Blake3>::new();
                let mut importer = JsonObjectImporter::<_, Blake3>::new(&mut import_blobs, None);
                match importer.import_blob(result.raw.to_blob()) {
                    Ok(roots) => {
                        let data = importer.data().clone();
                        let metadata = importer.metadata().unwrap_or_else(|err| match err {});
                        let reader = import_blobs
                            .reader()
                            .context("read response import blobs")?;
                        for (_, blob) in reader.iter() {
                            ws.put::<UnknownBlob, _>(blob.bytes.clone());
                        }

                        for root in roots {
                            change += entity! { &result_id @
                                openai_responses::response_json_root: root,
                            };
                        }

                        import_data = Some(data);
                        import_metadata = Some(metadata);
                    }
                    Err(err) => {
                        eprintln!("Failed to import response JSON: {err}");
                    }
                }
            }
            Err(err) => {
                let handle = ws.put::<LongString, _>(err.to_string());
                change += entity! { &result_id @ openai_responses::error: handle };
            }
        }

        if let (Some(data), Some(metadata)) = (import_data, import_metadata) {
            ws.commit(data, Some(metadata), Some("import response json"));
        }
        ws.commit(change, None, Some("openai_responses result"));
        push_workspace(&mut repo, &mut ws).context("push result")?;
    }

    Ok(())
}

fn stop_requested(stop: &Option<Arc<AtomicBool>>) -> bool {
    stop.as_ref()
        .map(|flag| flag.load(Ordering::Relaxed))
        .unwrap_or(false)
}

fn next_request(catalog: &TribleSet) -> Option<LlmRequest> {
    let mut requests: HashMap<Id, LlmRequest> = HashMap::new();
    for (request_id, prompt) in find!(
        (request_id: Id, prompt: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?request_id @
            openai_responses::kind: openai_responses::kind_request,
            openai_responses::prompt: ?prompt,
        }])
    ) {
        requests.insert(
            request_id,
            LlmRequest {
                id: request_id,
                prompt,
                model: None,
                requested_at: None,
            },
        );
    }

    if requests.is_empty() {
        return None;
    }

    for (request_id, model) in find!(
        (request_id: Id, model: Value<ShortString>),
        pattern!(catalog, [{
            ?request_id @ openai_responses::model: ?model
        }])
    ) {
        if let Some(entry) = requests.get_mut(&request_id) {
            entry.model = Some(model);
        }
    }

    for (request_id, requested_at) in find!(
        (request_id: Id, requested_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?request_id @ openai_responses::requested_at: ?requested_at
        }])
    ) {
        if let Some(entry) = requests.get_mut(&request_id) {
            entry.requested_at = Some(requested_at);
        }
    }

    let mut in_progress = HashSet::new();
    for (request_id,) in find!(
        (request_id: Id),
        pattern!(catalog, [{
            _?event @
            openai_responses::kind: openai_responses::kind_in_progress,
            openai_responses::about_request: ?request_id,
        }])
    ) {
        in_progress.insert(request_id);
    }

    let mut done = HashSet::new();
    for (request_id,) in find!(
        (request_id: Id),
        pattern!(catalog, [{
            _?event @
            openai_responses::kind: openai_responses::kind_result,
            openai_responses::about_request: ?request_id,
        }])
    ) {
        done.insert(request_id);
    }

    let mut candidates: Vec<LlmRequest> = requests
        .into_values()
        .filter(|req| !in_progress.contains(&req.id) && !done.contains(&req.id))
        .collect();
    candidates.sort_by_key(|req| req.requested_at.map(interval_key).unwrap_or(i128::MIN));
    candidates.into_iter().next()
}

fn build_payload(model: &str, prompt: &str, stream: bool) -> JsonValue {
    serde_json::json!({
        "model": model,
        "input": [{"role": "user", "content": prompt}],
        "reasoning": {
            "summary": "detailed",
        },
        "include": ["reasoning.encrypted_content"],
        "stream": stream,
    })
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
    Ok(OpenAIResult { output_text, raw })
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

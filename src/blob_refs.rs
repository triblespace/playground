use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::value::Value;
use triblespace::core::value::schemas::hash::{Blake3, Handle, Hash};

const BLOB_SCHEME_PREFIX: &str = "blob:blake3:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    pub alt: String,
    pub digest_hex: String,
    pub mime: Option<String>,
    pub name: Option<String>,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptChunk {
    Text(String),
    Blob(BlobRef),
}

pub fn split_blob_refs(input: &str) -> Vec<PromptChunk> {
    let mut chunks = Vec::new();
    let mut cursor = 0usize;

    while let Some(start_rel) = input[cursor..].find("![") {
        let start = cursor + start_rel;
        if start > cursor {
            chunks.push(PromptChunk::Text(input[cursor..start].to_owned()));
        }

        let alt_start = start + 2;
        let Some(alt_sep_rel) = input[alt_start..].find("](") else {
            chunks.push(PromptChunk::Text(input[start..].to_owned()));
            cursor = input.len();
            break;
        };
        let alt_sep = alt_start + alt_sep_rel;
        let url_start = alt_sep + 2;
        let Some(url_end_rel) = input[url_start..].find(')') else {
            chunks.push(PromptChunk::Text(input[start..].to_owned()));
            cursor = input.len();
            break;
        };
        let url_end = url_start + url_end_rel;

        let alt = &input[alt_start..alt_sep];
        let url = &input[url_start..url_end];
        let raw = &input[start..=url_end];
        if let Some(blob_ref) = parse_blob_ref(alt, url, raw) {
            chunks.push(PromptChunk::Blob(blob_ref));
        } else {
            chunks.push(PromptChunk::Text(raw.to_owned()));
        }
        cursor = url_end + 1;
    }

    if cursor < input.len() {
        chunks.push(PromptChunk::Text(input[cursor..].to_owned()));
    }

    if chunks.is_empty() {
        chunks.push(PromptChunk::Text(String::new()));
    }
    merge_adjacent_text_chunks(chunks)
}

pub fn unknown_blob_handle_from_hex(hex: &str) -> Option<Value<Handle<Blake3, UnknownBlob>>> {
    let hash = Hash::<Blake3>::from_hex(hex).ok()?;
    Some(hash.into())
}

fn parse_blob_ref(alt: &str, url: &str, raw: &str) -> Option<BlobRef> {
    let rest = url.strip_prefix(BLOB_SCHEME_PREFIX)?;
    let (digest_hex, query) = match rest.split_once('?') {
        Some((digest, q)) => (digest, Some(q)),
        None => (rest, None),
    };
    if digest_hex.len() != 64 || !digest_hex.bytes().all(is_hex) {
        return None;
    }

    let mut mime = None;
    let mut name = None;
    if let Some(query) = query {
        for pair in query.split('&') {
            let (key, value) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => (pair, ""),
            };
            let decoded = percent_decode(value);
            match key {
                "mime" => {
                    if !decoded.trim().is_empty() {
                        mime = Some(decoded.trim().to_owned());
                    }
                }
                "name" => {
                    if !decoded.trim().is_empty() {
                        name = Some(decoded.trim().to_owned());
                    }
                }
                _ => {}
            }
        }
    }

    Some(BlobRef {
        alt: alt.to_owned(),
        digest_hex: digest_hex.to_ascii_uppercase(),
        mime,
        name,
        raw: raw.to_owned(),
    })
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b'+' => {
                out.push(b' ');
                idx += 1;
            }
            b'%' if idx + 2 < bytes.len() => {
                let hi = from_hex_nibble(bytes[idx + 1]);
                let lo = from_hex_nibble(bytes[idx + 2]);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi << 4) | lo);
                    idx += 3;
                } else {
                    out.push(bytes[idx]);
                    idx += 1;
                }
            }
            b => {
                out.push(b);
                idx += 1;
            }
        }
    }
    String::from_utf8_lossy(out.as_slice()).to_string()
}

fn is_hex(b: u8) -> bool {
    b.is_ascii_hexdigit()
}

fn from_hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}

fn merge_adjacent_text_chunks(chunks: Vec<PromptChunk>) -> Vec<PromptChunk> {
    let mut merged = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        match chunk {
            PromptChunk::Text(text) => match merged.last_mut() {
                Some(PromptChunk::Text(existing)) => existing.push_str(text.as_str()),
                _ => merged.push(PromptChunk::Text(text)),
            },
            other => merged.push(other),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::{PromptChunk, split_blob_refs};

    #[test]
    fn parses_blob_marker_with_query() {
        let input = "hello ![cat](blob:blake3:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA?mime=image%2Fpng&name=cat.png) world";
        let chunks = split_blob_refs(input);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], PromptChunk::Text("hello ".to_string()));
        let PromptChunk::Blob(blob) = &chunks[1] else {
            panic!("expected blob");
        };
        assert_eq!(
            blob.digest_hex,
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(blob.mime.as_deref(), Some("image/png"));
        assert_eq!(blob.name.as_deref(), Some("cat.png"));
        assert_eq!(chunks[2], PromptChunk::Text(" world".to_string()));
    }

    #[test]
    fn ignores_non_blob_markdown_images() {
        let input = "![x](https://example.com/x.png)";
        let chunks = split_blob_refs(input);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], PromptChunk::Text(input.to_string()));
    }
}

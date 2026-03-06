use hifitime::Epoch;
use triblespace::prelude::valueschemas::NsTAIInterval;
use triblespace::prelude::{ToValue, Value};

pub(crate) fn now_epoch() -> Epoch {
    Epoch::now().unwrap_or_else(|_| Epoch::from_gregorian_utc(1970, 1, 1, 0, 0, 0, 0))
}

pub(crate) fn epoch_interval(epoch: Epoch) -> Value<NsTAIInterval> {
    (epoch, epoch).to_value()
}

pub(crate) fn interval_key(interval: Value<NsTAIInterval>) -> i128 {
    let (lower, _): (Epoch, Epoch) = interval.from_value();
    epoch_key(lower)
}

fn epoch_key(epoch: Epoch) -> i128 {
    epoch.to_tai_duration().total_nanoseconds()
}

pub(crate) fn format_tai_timestamp(epoch: Epoch) -> String {
    let (y, m, d, hh, mm, ss, _) = epoch.to_gregorian_tai();
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02} TAI")
}

pub(crate) fn format_time_range(interval_start: Value<NsTAIInterval>, interval_end: Value<NsTAIInterval>) -> String {
    let (start, _): (Epoch, Epoch) = interval_start.from_value();
    let (_, end): (Epoch, Epoch) = interval_end.from_value();
    let (y1, m1, d1, h1, mi1, s1, _) = start.to_gregorian_tai();
    let (y2, m2, d2, h2, mi2, s2, _) = end.to_gregorian_tai();
    format!(
        "{y1:04}-{m1:02}-{d1:02}T{h1:02}:{mi1:02}:{s1:02}..{y2:04}-{m2:02}-{d2:02}T{h2:02}:{mi2:02}:{s2:02}"
    )
}

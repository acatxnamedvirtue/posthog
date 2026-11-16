//! Finds label keys that make a metric explode into many series, and drops
//! them before the series fingerprint is final.
//!
//! Detection has two stages so that its cost follows the metrics that grow,
//! not every metric:
//!
//! 1. The series label gate reports each row that is new to its cache. Those
//!    rows are one row per series per window, so their count per metric family
//!    is a cheap count of new series. A family that passes
//!    `candidate_min_new_series` in one window becomes a candidate.
//! 2. For a candidate, every row feeds one HyperLogLog sketch of its original
//!    series fingerprints and one sketch per label key of the label values.
//!    A key is flagged when its distinct values pass `value_limit` and are at
//!    least `min_share` of the distinct series. The share test points at the
//!    key that causes the growth, not at every key of a large metric.
//!
//! A flagged key becomes a drop rule. Rules go to Redis so every pod applies
//! the same rules. A rule expires after `rule_ttl` unless detection sees the
//! key over half the limit again, so a rule ends on its own when the sender
//! stops.
//!
//! In enforce mode the key is removed from the row and the fingerprint is
//! computed again without it, so all series that differ only by that key
//! become one series. Detection reads the rows before the drop, so it keeps
//! measuring the original label while the rule is active.

use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use capture_logs::log_record::ORIGINAL_TIMESTAMP_ATTRIBUTE;
use capture_logs::metric_record::{compute_series_fingerprint, KafkaMetricRow};
use chrono::Utc;
use common_redis::{Client, CustomRedisError, PipelineCommand};
use dashmap::DashMap;
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::histogram_assembly::fingerprint_with_bounds;
use crate::series_label_gate::Clock;

const REDIS_KEY_PREFIX: &str = "capture-logs:metric-label-drop-rules";
/// Rules are spread over hour buckets by expiry, so old buckets expire on
/// their own and no prune command is needed.
const REDIS_BUCKET_SECS: i64 = 3600;
const PULL_PAGE_SIZE: usize = 10_000;
const WRITER_CHANNEL_CAPACITY: usize = 10_000;
const PRUNE_INTERVAL: Duration = Duration::from_secs(300);
/// One estimate walks every register of every sketch of the candidate. This
/// bounds how often one candidate pays that cost.
const EVALUATE_INTERVAL_SECS: i64 = 10;

/// The row builders add these attributes per sample after they compute the
/// fingerprint. Each row has its own value, so they look like the highest
/// cardinality keys there are. They are never flagged, and the recomputed
/// fingerprint leaves them out as the builders do.
const ORIGINAL_TIMESTAMP_ATTRIBUTES: [&str; 2] =
    [ORIGINAL_TIMESTAMP_ATTRIBUTE, "$originalTimestamp"];
/// Enforce mode sets this attribute on every row that lost a label, so the
/// metrics UI can show which series were combined. The value is the sorted,
/// comma-joined list of the dropped keys. The fingerprint leaves it out, so a
/// combined series keeps the fingerprint of a series that never had the key.
pub const DROPPED_LABELS_ATTRIBUTE: &str = "posthog.dropped_labels";
const HISTOGRAM_SUFFIXES: [&str; 3] = ["_bucket", "_count", "_sum"];
const HISTOGRAM_TYPE: &str = "histogram";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// No detection and no drops.
    Off,
    /// Detect and share rules, but leave every row unchanged.
    Shadow,
    /// Detect, share rules, and drop the flagged labels.
    Enforce,
}

impl FromStr for Mode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Mode::Off),
            "shadow" => Ok(Mode::Shadow),
            "enforce" => Ok(Mode::Enforce),
            other => Err(format!("unknown label cardinality mode {other:?}")),
        }
    }
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Off => "off",
            Mode::Shadow => "shadow",
            Mode::Enforce => "enforce",
        }
    }
}

/// How the request path built the fingerprints of its rows. A recomputed
/// fingerprint must use the same recipe, or a collapsed series would not match
/// the series that a row without the label gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FingerprintKind {
    /// Every row uses the label fingerprint.
    Otlp,
    /// A native histogram row folded from classic components also hashes its
    /// bound set, see `histogram_assembly`.
    RemoteWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Attribute,
    Resource,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub mode: Mode,
    /// Sketches restart at each window boundary. Same length as the series
    /// label gate window, so a new-series count and a sketch cover one period.
    pub window: Duration,
    pub value_limit: u64,
    pub min_share: f64,
    pub candidate_min_new_series: u32,
    pub max_candidates: usize,
    pub max_keys_per_candidate: usize,
    pub rule_ttl: Duration,
    pub protected_keys: HashSet<String>,
}

/// One drop rule, also the Redis member. The token is hashed into
/// `metric_key`, so no token reaches Redis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub metric_key: u64,
    pub family: String,
    pub scope: Scope,
    pub key: String,
    pub expires_at: i64,
}

/// HyperLogLog with 256 one-byte registers. The standard error is about 6.5%,
/// which is enough for a threshold test and keeps one sketch at 256 bytes.
#[derive(Clone)]
struct Sketch {
    registers: [u8; SKETCH_REGISTERS],
}

const SKETCH_INDEX_BITS: u32 = 8;
const SKETCH_REGISTERS: usize = 1 << SKETCH_INDEX_BITS;

impl Sketch {
    fn new() -> Self {
        Self {
            registers: [0; SKETCH_REGISTERS],
        }
    }

    fn insert(&mut self, hash: u64) {
        let index = (hash >> (64 - SKETCH_INDEX_BITS)) as usize;
        let rest = hash << SKETCH_INDEX_BITS;
        let rank = (rest.leading_zeros().min(64 - SKETCH_INDEX_BITS) + 1) as u8;
        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    fn estimate(&self) -> u64 {
        let m = SKETCH_REGISTERS as f64;
        let mut sum = 0.0;
        let mut zeros = 0u32;
        for &register in &self.registers {
            sum += 2f64.powi(-(register as i32));
            if register == 0 {
                zeros += 1;
            }
        }
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        // Linear counting is more accurate while many registers are empty.
        if raw <= 2.5 * m && zeros > 0 {
            return (m * (m / zeros as f64).ln()).round() as u64;
        }
        raw.round() as u64
    }
}

struct Candidate {
    token: Arc<str>,
    family: String,
    window: i64,
    series: Sketch,
    keys: HashMap<(Scope, String), Sketch>,
    last_evaluated: i64,
}

impl Candidate {
    fn new(token: Arc<str>, family: String, window: i64) -> Self {
        Self {
            token,
            family,
            window,
            series: Sketch::new(),
            keys: HashMap::new(),
            last_evaluated: i64::MIN,
        }
    }

    fn roll(&mut self, window: i64) {
        if self.window != window {
            self.window = window;
            self.series = Sketch::new();
            self.keys.clear();
        }
    }
}

#[derive(Default)]
struct NewSeriesCount {
    window: i64,
    count: u32,
}

pub struct LabelCardinality {
    settings: Settings,
    clock: Clock,
    new_series: DashMap<u64, NewSeriesCount>,
    candidates: DashMap<u64, Mutex<Candidate>>,
    /// Exact candidate count. `DashMap::len` walks every shard.
    candidate_count: AtomicUsize,
    rules: DashMap<u64, Vec<Rule>>,
    tx: Option<mpsc::Sender<Rule>>,
}

impl LabelCardinality {
    /// Detector that shares rules through Redis. The receiver goes to
    /// [`Self::spawn_redis_sync`].
    pub fn new(settings: Settings) -> (Arc<Self>, mpsc::Receiver<Rule>) {
        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        let detector = Self::build(settings, Some(tx), Arc::new(unix_now));
        (Arc::new(detector), rx)
    }

    /// Detector whose rules stay on this pod. Used when `REDIS_URL` is unset.
    pub fn local_only(settings: Settings) -> Arc<Self> {
        Arc::new(Self::build(settings, None, Arc::new(unix_now)))
    }

    fn build(settings: Settings, tx: Option<mpsc::Sender<Rule>>, clock: Clock) -> Self {
        Self {
            settings,
            clock,
            new_series: DashMap::new(),
            candidates: DashMap::new(),
            candidate_count: AtomicUsize::new(0),
            rules: DashMap::new(),
            tx,
        }
    }

    pub fn mode(&self) -> Mode {
        self.settings.mode
    }

    fn window_index(&self, now: i64) -> i64 {
        now.div_euclid(self.settings.window.as_secs().max(1) as i64)
    }

    /// Stage 1. Count the rows that the series label gate found new, per
    /// metric family. `rows` are the rows the gate saw, `new_rows` the indices
    /// it kept labelled.
    pub fn note_new_series(&self, token: &str, rows: &[KafkaMetricRow], new_rows: &[usize]) {
        if self.settings.mode == Mode::Off || new_rows.is_empty() {
            return;
        }
        let now = (self.clock)();
        let window = self.window_index(now);
        let mut per_family: HashMap<&str, u32> = HashMap::new();
        for &index in new_rows {
            if let Some(row) = rows.get(index) {
                *per_family.entry(family(&row.metric_name)).or_default() += 1;
            }
        }
        let mut token_label: Option<Arc<str>> = None;
        for (family_name, added) in per_family {
            let metric_key = metric_key(token, family_name);
            let promote = {
                let mut entry = self.new_series.entry(metric_key).or_default();
                if entry.window != window {
                    entry.window = window;
                    entry.count = 0;
                }
                let before = entry.count;
                entry.count = entry.count.saturating_add(added);
                before < self.settings.candidate_min_new_series
                    && entry.count >= self.settings.candidate_min_new_series
            };
            if promote {
                let token_label = token_label.get_or_insert_with(|| Arc::from(token));
                self.add_candidate(metric_key, token_label, family_name, window);
            }
        }
    }

    fn add_candidate(&self, metric_key: u64, token: &Arc<str>, family_name: &str, window: i64) {
        if self.candidates.contains_key(&metric_key) {
            return;
        }
        if self.candidate_count.fetch_add(1, Ordering::AcqRel) >= self.settings.max_candidates {
            self.candidate_count.fetch_sub(1, Ordering::AcqRel);
            counter!("capture_metrics_label_cardinality_candidates_full").increment(1);
            return;
        }
        let candidate = Candidate::new(Arc::clone(token), family_name.to_string(), window);
        if let dashmap::Entry::Vacant(slot) = self.candidates.entry(metric_key) {
            slot.insert(Mutex::new(candidate));
        } else {
            self.candidate_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Stage 2. Feed the sketches of each candidate from `rows`, then flag the
    /// keys that pass the limits. Call it before [`Self::apply`], so the
    /// sketches see the labels that a rule drops.
    pub fn observe(&self, token: &str, rows: &[KafkaMetricRow]) {
        if self.settings.mode == Mode::Off || (self.candidates.is_empty() && self.rules.is_empty())
        {
            return;
        }
        let now = (self.clock)();
        let window = self.window_index(now);
        let token_label: Arc<str> = Arc::from(token);

        let mut keys_by_name: HashMap<&str, u64> = HashMap::new();
        let mut groups: HashMap<u64, Vec<usize>> = HashMap::new();
        for (index, row) in rows.iter().enumerate() {
            let metric_key = *keys_by_name
                .entry(row.metric_name.as_str())
                .or_insert_with(|| metric_key(token, family(&row.metric_name)));
            if self.candidates.contains_key(&metric_key) {
                groups.entry(metric_key).or_default().push(index);
            } else if self.rules.contains_key(&metric_key) {
                // A rule hides the series growth from the gate, so the family
                // would never pass stage 1 again. The rule keeps it tracked.
                self.add_candidate(metric_key, &token_label, family(&row.metric_name), window);
                groups.entry(metric_key).or_default().push(index);
            }
        }

        for (metric_key, indices) in groups {
            let Some(slot) = self.candidates.get(&metric_key) else {
                continue;
            };
            let Ok(mut candidate) = slot.lock() else {
                continue;
            };
            candidate.roll(window);
            for index in indices {
                self.feed(&mut candidate, &rows[index]);
            }
            if now.saturating_sub(candidate.last_evaluated) >= EVALUATE_INTERVAL_SECS {
                candidate.last_evaluated = now;
                self.evaluate(metric_key, &candidate, now);
            }
        }
    }

    fn feed(&self, candidate: &mut Candidate, row: &KafkaMetricRow) {
        candidate.series.insert(row.series_fingerprint as u64);
        let max_keys = self.settings.max_keys_per_candidate;
        let mut keys_full = 0u64;
        for (scope, map) in [
            (Scope::Attribute, &row.attributes),
            (Scope::Resource, &row.resource_attributes),
        ] {
            for (key, value) in map {
                if self.is_protected(scope, key) {
                    continue;
                }
                let hash = value_hash(value);
                if let Some(sketch) = candidate.keys.get_mut(&(scope, key.clone())) {
                    sketch.insert(hash);
                    continue;
                }
                if candidate.keys.len() >= max_keys {
                    keys_full += 1;
                    continue;
                }
                let mut sketch = Sketch::new();
                sketch.insert(hash);
                candidate.keys.insert((scope, key.clone()), sketch);
            }
        }
        if keys_full > 0 {
            counter!("capture_metrics_label_cardinality_keys_full").increment(keys_full);
        }
    }

    fn is_protected(&self, scope: Scope, key: &str) -> bool {
        (scope == Scope::Attribute
            && (ORIGINAL_TIMESTAMP_ATTRIBUTES.contains(&key) || key == DROPPED_LABELS_ATTRIBUTE))
            || self.settings.protected_keys.contains(key)
    }

    /// Create a rule for a key over the limits, and extend an active rule
    /// while its key stays over half the limit. Half the limit gives the rule
    /// hysteresis, so a key near the limit does not switch on and off.
    fn evaluate(&self, metric_key: u64, candidate: &Candidate, now: i64) {
        let ttl = self.settings.rule_ttl.as_secs() as i64;
        let series = candidate.series.estimate();
        for ((scope, key), sketch) in &candidate.keys {
            let distinct = sketch.estimate();
            if distinct * 2 < self.settings.value_limit {
                continue;
            }
            let existing = self.rules.get(&metric_key).and_then(|rules| {
                rules
                    .iter()
                    .find(|rule| rule.scope == *scope && rule.key == *key && rule.expires_at > now)
                    .map(|rule| rule.expires_at)
            });
            let new_rule = match existing {
                Some(expires_at) => {
                    // Extend once the rule is half way to expiry, so each pod
                    // writes a busy rule to Redis at most once per half TTL.
                    if expires_at - now > ttl / 2 {
                        continue;
                    }
                    false
                }
                None => {
                    if distinct < self.settings.value_limit
                        || (distinct as f64) < self.settings.min_share * series as f64
                    {
                        continue;
                    }
                    true
                }
            };
            let rule = Rule {
                metric_key,
                family: candidate.family.clone(),
                scope: *scope,
                key: key.clone(),
                expires_at: now + ttl,
            };
            if new_rule {
                warn!(
                    token = %candidate.token,
                    family = %rule.family,
                    scope = ?rule.scope,
                    key = %rule.key,
                    distinct_values = distinct,
                    distinct_series = series,
                    mode = self.settings.mode.as_str(),
                    "High cardinality metric label flagged"
                );
                counter!(
                    "capture_metrics_label_cardinality_rules_created",
                    "token" => Arc::clone(&candidate.token),
                    "mode" => self.settings.mode.as_str()
                )
                .increment(1);
            }
            self.merge_rule(rule.clone());
            if let Some(tx) = &self.tx {
                if tx.try_send(rule).is_err() {
                    counter!("capture_metrics_label_cardinality_redis_queue_full").increment(1);
                }
            }
        }
    }

    /// Add `rule`, or move an existing rule for the same key to the later
    /// expiry.
    fn merge_rule(&self, rule: Rule) {
        let mut rules = self.rules.entry(rule.metric_key).or_default();
        match rules
            .iter_mut()
            .find(|r| r.scope == rule.scope && r.key == rule.key)
        {
            Some(existing) => existing.expires_at = existing.expires_at.max(rule.expires_at),
            None => rules.push(rule),
        }
    }

    /// Drop the flagged labels from `rows`, mark each changed row with
    /// [`DROPPED_LABELS_ATTRIBUTE`], and compute their fingerprints again
    /// without the dropped labels. In shadow mode only count the rows that
    /// would change. Call it after [`Self::observe`] and before the series
    /// label gate. Returns the number of marked rows, so the caller can take
    /// the marker off the billed bytes.
    pub fn apply(&self, token: &str, rows: &mut [KafkaMetricRow], kind: FingerprintKind) -> u64 {
        if self.settings.mode == Mode::Off || self.rules.is_empty() {
            return 0;
        }
        let now = (self.clock)();
        let enforce = self.settings.mode == Mode::Enforce;
        let mut active_by_name: HashMap<String, Vec<(Scope, String)>> = HashMap::new();
        // Rows of one series share labels, so each old fingerprint maps to one
        // new value.
        let mut recomputed: HashMap<i64, i64> = HashMap::new();
        let mut changed = 0u64;

        for row in rows.iter_mut() {
            if !active_by_name.contains_key(&row.metric_name) {
                let active = self.active_rules(metric_key(token, family(&row.metric_name)), now);
                active_by_name.insert(row.metric_name.clone(), active);
            }
            let active = &active_by_name[&row.metric_name];
            if active.is_empty() {
                continue;
            }
            let mut dropped: Vec<&str> = Vec::new();
            for (scope, key) in active {
                let map = match scope {
                    Scope::Attribute => &mut row.attributes,
                    Scope::Resource => &mut row.resource_attributes,
                };
                if !map.contains_key(key) {
                    continue;
                }
                dropped.push(key);
                if enforce {
                    map.remove(key);
                }
            }
            if dropped.is_empty() {
                continue;
            }
            changed += 1;
            if enforce {
                row.series_fingerprint = *recomputed
                    .entry(row.series_fingerprint)
                    .or_insert_with(|| series_fingerprint(row, kind));
                dropped.sort_unstable();
                dropped.dedup();
                // Attribute values are JSON-encoded strings.
                let marker = serde_json::Value::String(dropped.join(",")).to_string();
                row.attributes
                    .insert(DROPPED_LABELS_ATTRIBUTE.to_string(), marker);
            }
        }

        if changed > 0 {
            counter!(
                "capture_metrics_label_cardinality_rows_dropped",
                "mode" => self.settings.mode.as_str()
            )
            .increment(changed);
        }
        if enforce {
            changed
        } else {
            0
        }
    }

    fn active_rules(&self, metric_key: u64, now: i64) -> Vec<(Scope, String)> {
        self.rules
            .get(&metric_key)
            .map(|rules| {
                rules
                    .iter()
                    .filter(|rule| rule.expires_at > now)
                    .map(|rule| (rule.scope, rule.key.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove expired rules, candidates from past windows with no rule, and
    /// stale new-series counts.
    fn prune(&self, now: i64) {
        let window = self.window_index(now);
        self.rules.retain(|_, rules| {
            rules.retain(|rule| rule.expires_at > now);
            !rules.is_empty()
        });
        self.candidates.retain(|metric_key, candidate| {
            let current = candidate
                .get_mut()
                .map(|candidate| candidate.window >= window - 1)
                .unwrap_or(false);
            if current || self.rules.contains_key(metric_key) {
                return true;
            }
            self.candidate_count.fetch_sub(1, Ordering::AcqRel);
            false
        });
        self.new_series.retain(|_, count| count.window >= window);
        gauge!("capture_metrics_label_cardinality_candidates")
            .set(self.candidate_count.load(Ordering::Acquire) as f64);
        let active: usize = self.rules.iter().map(|rules| rules.len()).sum();
        gauge!("capture_metrics_label_cardinality_rules").set(active as f64);
    }

    pub fn spawn_pruner(self: &Arc<Self>) {
        let detector = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(PRUNE_INTERVAL);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                detector.prune((detector.clock)());
            }
        });
    }

    /// Read every rule in Redis that has not expired and merge it.
    pub async fn pull_rules(&self, client: &dyn Client) -> Result<usize, CustomRedisError> {
        let now = (self.clock)();
        let ttl = self.settings.rule_ttl.as_secs() as i64;
        let mut merged = 0usize;
        for key in bucket_keys(now, now + ttl) {
            let mut offset = 0usize;
            loop {
                let page = match client
                    .zrangebyscore_limit(
                        key.clone(),
                        now.to_string(),
                        "+inf".to_string(),
                        offset as isize,
                        PULL_PAGE_SIZE as isize,
                    )
                    .await
                {
                    Ok(page) => page,
                    // An hour with no rules has no key.
                    Err(CustomRedisError::NotFound) => break,
                    Err(e) => return Err(e),
                };
                let page_len = page.len();
                for member in page {
                    match serde_json::from_str::<Rule>(&member) {
                        Ok(rule) if rule.expires_at > now => {
                            self.merge_rule(rule);
                            merged += 1;
                        }
                        Ok(_) => {}
                        Err(e) => debug!("Skipping unreadable label drop rule: {e}"),
                    }
                }
                if page_len < PULL_PAGE_SIZE {
                    break;
                }
                offset += page_len;
            }
        }
        Ok(merged)
    }

    /// Push the rules this pod creates or extends, and pull the rules of every
    /// pod each `interval`. A Redis failure only means a pod finds a rule later
    /// through its own detection.
    pub fn spawn_redis_sync(
        self: &Arc<Self>,
        client: Arc<dyn Client>,
        mut rx: mpsc::Receiver<Rule>,
        interval: Duration,
        timeout: Duration,
    ) {
        let detector = Arc::clone(self);
        let ttl_secs = self.settings.rule_ttl.as_secs() + REDIS_BUCKET_SECS as u64;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    rule = rx.recv() => {
                        let Some(rule) = rule else { return };
                        let mut batch = vec![rule];
                        while let Ok(rule) = rx.try_recv() {
                            batch.push(rule);
                        }
                        let pushed = batch.len() as u64;
                        let commands = rule_commands(&batch, ttl_secs);
                        let outcome = match tokio::time::timeout(timeout, client.execute_pipeline(commands)).await {
                            Ok(Ok(_)) => "ok",
                            Ok(Err(e)) => {
                                debug!("Label drop rule push to Redis failed: {e}");
                                "error"
                            }
                            Err(_) => "timeout",
                        };
                        counter!("capture_metrics_label_cardinality_redis_pushed", "outcome" => outcome)
                            .increment(pushed);
                    }
                    _ = ticker.tick() => {
                        let outcome = match tokio::time::timeout(timeout, detector.pull_rules(client.as_ref())).await {
                            Ok(Ok(merged)) => {
                                debug!("Pulled {merged} label drop rules from Redis");
                                "ok"
                            }
                            Ok(Err(e)) => {
                                debug!("Label drop rule pull from Redis failed: {e}");
                                if e.is_unrecoverable_error() {
                                    client.heal().await;
                                }
                                "error"
                            }
                            Err(_) => "timeout",
                        };
                        counter!("capture_metrics_label_cardinality_redis_pulls", "outcome" => outcome)
                            .increment(1);
                    }
                }
            }
        });
    }
}

/// The fingerprint a row gets when the request path builds it from its
/// current labels. It must match the builders exactly, so that a row whose
/// label a rule dropped joins the series of a row that never had the label.
fn series_fingerprint(row: &KafkaMetricRow, kind: FingerprintKind) -> i64 {
    let mut attributes = row.attributes.clone();
    for attribute in ORIGINAL_TIMESTAMP_ATTRIBUTES {
        attributes.remove(attribute);
    }
    attributes.remove(DROPPED_LABELS_ATTRIBUTE);
    let fingerprint = compute_series_fingerprint(
        &row.metric_name,
        &row.metric_type,
        &row.service_name,
        &row.resource_attributes,
        &attributes,
    );
    // Remote-write gives the `histogram` type only to the native rows that
    // `fold_classic_histograms` builds, and those hash their bound set.
    match kind {
        FingerprintKind::RemoteWrite if row.metric_type == HISTOGRAM_TYPE => {
            fingerprint_with_bounds(fingerprint, &row.histogram_bounds)
        }
        _ => fingerprint,
    }
}

/// The name a rule applies to. The `_bucket`, `_count` and `_sum` series of a
/// classic histogram share one family, so a rule drops a label from all of
/// them. Otherwise the components disagree on their labels and stop folding
/// into one native histogram.
fn family(metric_name: &str) -> &str {
    for suffix in HISTOGRAM_SUFFIXES {
        if let Some(base) = metric_name.strip_suffix(suffix) {
            if !base.is_empty() {
                return base;
            }
        }
    }
    metric_name
}

/// Families of two projects must not share state, so the token is hashed in.
fn metric_key(token: &str, family: &str) -> u64 {
    let mut hasher = SipHasher13::new();
    hasher.write(&(token.len() as u64).to_le_bytes());
    hasher.write(token.as_bytes());
    hasher.write(family.as_bytes());
    hasher.finish()
}

fn value_hash(value: &str) -> u64 {
    let mut hasher = SipHasher13::new();
    hasher.write(value.as_bytes());
    hasher.finish()
}

fn bucket_key(unix_secs: i64) -> String {
    format!(
        "{REDIS_KEY_PREFIX}:{}",
        unix_secs.div_euclid(REDIS_BUCKET_SECS)
    )
}

fn bucket_keys(from: i64, to: i64) -> Vec<String> {
    let first = from.div_euclid(REDIS_BUCKET_SECS);
    let last = to.div_euclid(REDIS_BUCKET_SECS);
    (first..=last)
        .map(|bucket| format!("{REDIS_KEY_PREFIX}:{bucket}"))
        .collect()
}

/// One `ZADD` per expiry bucket, scored by expiry so a pull can skip expired
/// rules, plus one `EXPIRE` per bucket.
fn rule_commands(rules: &[Rule], ttl_secs: u64) -> Vec<PipelineCommand> {
    let mut by_bucket: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    for rule in rules {
        let Ok(member) = serde_json::to_string(rule) else {
            continue;
        };
        by_bucket
            .entry(bucket_key(rule.expires_at))
            .or_default()
            .push((rule.expires_at, member));
    }
    let mut commands = Vec::with_capacity(by_bucket.len() * 2);
    for (bucket, members) in by_bucket {
        commands.push(PipelineCommand::ZAdd {
            key: bucket.clone(),
            members,
        });
        commands.push(PipelineCommand::Expire {
            key: bucket,
            seconds: ttl_secs,
        });
    }
    commands
}

fn unix_now() -> i64 {
    Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI64;

    use capture_logs::endpoints::prometheus::write_timeseries_to_kafka_rows;
    use common_redis::MockRedisClient;
    use prometheus_rw_proto::prometheus::v1::{Label, Sample, TimeSeries};

    use super::*;
    use crate::histogram_assembly::fold_classic_histograms;

    const TOKEN: &str = "phc_test";
    const START: i64 = 1_800_000_000;
    const LIMIT: u64 = 200;

    fn settings(mode: Mode) -> Settings {
        Settings {
            mode,
            window: Duration::from_secs(1800),
            value_limit: LIMIT,
            min_share: 0.5,
            candidate_min_new_series: 10,
            max_candidates: 100,
            max_keys_per_candidate: 32,
            rule_ttl: Duration::from_secs(7200),
            protected_keys: ["le".to_string(), "quantile".to_string()].into(),
        }
    }

    fn detector(mode: Mode) -> (LabelCardinality, mpsc::Receiver<Rule>, Arc<AtomicI64>) {
        let now = Arc::new(AtomicI64::new(START));
        let clock_now = Arc::clone(&now);
        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        let detector = LabelCardinality::build(
            settings(mode),
            Some(tx),
            Arc::new(move || clock_now.load(Ordering::SeqCst)),
        );
        (detector, rx, now)
    }

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// A remote-write classic histogram with one series per `user_id`, plus
    /// `route` with few values. `posthog.original_timestamp` is not here: it is
    /// added per sample by the builder only for out-of-window timestamps.
    fn histogram_series(users: usize, with_user: bool) -> Vec<TimeSeries> {
        let timestamp = Utc::now().timestamp_millis();
        let mut series = Vec::new();
        for user in 0..users {
            let mut base = vec![
                label("job", "api"),
                label("route", &format!("/r{}", user % 4)),
            ];
            if with_user {
                base.push(label("user_id", &format!("u{user}")));
            }
            let mut push = |name: &str, extra: Option<Label>, value: f64| {
                let mut labels = vec![label("__name__", name)];
                labels.extend(base.iter().cloned());
                labels.extend(extra);
                series.push(TimeSeries {
                    labels,
                    samples: vec![Sample { value, timestamp }],
                });
            };
            push("latency_bucket", Some(label("le", "0.5")), 1.0);
            push("latency_bucket", Some(label("le", "+Inf")), 2.0);
            push("latency_sum", None, 0.7);
            push("latency_count", None, 2.0);
            push("requests_total", None, 5.0);
        }
        series
    }

    fn remote_write_rows(users: usize, with_user: bool) -> Vec<KafkaMetricRow> {
        let (rows, _) = write_timeseries_to_kafka_rows(histogram_series(users, with_user), &[]);
        fold_classic_histograms(rows, &[])
    }

    fn all_rows_new(rows: &[KafkaMetricRow]) -> Vec<usize> {
        (0..rows.len()).collect()
    }

    fn rule_keys(detector: &LabelCardinality) -> Vec<(String, Scope, String)> {
        let mut keys: Vec<_> = detector
            .rules
            .iter()
            .flat_map(|rules| {
                rules
                    .iter()
                    .map(|r| (r.family.clone(), r.scope, r.key.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        keys.sort_by(|a, b| (&a.0, &a.2).cmp(&(&b.0, &b.2)));
        keys
    }

    #[test]
    fn flags_only_the_key_that_drives_series_growth() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);

        // `route` has 4 values and `job` 1, so neither passes the limit. `le`
        // is protected and only on the classic rows that did not fold.
        assert_eq!(
            rule_keys(&detector),
            vec![
                (
                    "latency".to_string(),
                    Scope::Attribute,
                    "user_id".to_string()
                ),
                (
                    "requests_total".to_string(),
                    Scope::Attribute,
                    "user_id".to_string()
                ),
            ]
        );
    }

    #[test]
    fn a_family_under_the_candidate_threshold_is_never_measured() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &[0, 1, 2]);
        detector.observe(TOKEN, &rows);

        assert!(rule_keys(&detector).is_empty());
    }

    #[test]
    fn a_key_with_many_values_but_a_small_share_of_series_is_not_flagged() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        // 1000 users, each with 4 instance values: `instance` has 4000 values
        // across 4000 series, `user_id` 1000 values, a quarter of the series.
        let timestamp = Utc::now().timestamp_millis();
        let series: Vec<TimeSeries> = (0..4000)
            .map(|i| TimeSeries {
                labels: vec![
                    label("__name__", "jobs_total"),
                    label("user_id", &format!("u{}", i / 4)),
                    label("instance", &format!("i{i}")),
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp,
                }],
            })
            .collect();
        let (rows, _) = write_timeseries_to_kafka_rows(series, &[]);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);

        // Remote-write copies `instance` to the `service.instance.id` resource
        // attribute, so both copies must go for the series to collapse.
        assert_eq!(
            rule_keys(&detector),
            vec![
                (
                    "jobs_total".to_string(),
                    Scope::Attribute,
                    "instance".to_string()
                ),
                (
                    "jobs_total".to_string(),
                    Scope::Resource,
                    "service.instance.id".to_string()
                ),
            ]
        );
    }

    #[test]
    fn enforce_gives_the_fingerprint_of_rows_built_without_the_label() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let mut rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);
        let marked = detector.apply(TOKEN, &mut rows, FingerprintKind::RemoteWrite);

        let expected = remote_write_rows(1000, false);
        let fingerprints = |rows: &[KafkaMetricRow]| {
            let mut by_name: HashMap<String, HashSet<i64>> = HashMap::new();
            for row in rows {
                by_name
                    .entry(row.metric_name.clone())
                    .or_default()
                    .insert(row.series_fingerprint);
            }
            by_name
        };
        // Native histogram rows hash their bound set, plain rows do not. Both
        // must collapse to the series a sender without `user_id` gets: 4
        // routes, so 4 series per metric.
        assert_eq!(fingerprints(&rows), fingerprints(&expected));
        assert_eq!(fingerprints(&rows)["latency"].len(), 4);
        assert!(rows
            .iter()
            .all(|row| !row.attributes.contains_key("user_id")));
        // The marker is on every collapsed row but not in the fingerprint, so
        // the fingerprints above still equal those of rows without `user_id`.
        assert!(rows.iter().all(|row| {
            row.attributes
                .get(DROPPED_LABELS_ATTRIBUTE)
                .map(String::as_str)
                == Some("\"user_id\"")
        }));
        assert_eq!(marked, rows.len() as u64);
    }

    #[test]
    fn shadow_mode_flags_the_key_but_leaves_rows_unchanged() {
        let (detector, _rx, _) = detector(Mode::Shadow);
        let mut rows = remote_write_rows(1000, true);
        let before: Vec<i64> = rows.iter().map(|r| r.series_fingerprint).collect();
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);
        let marked = detector.apply(TOKEN, &mut rows, FingerprintKind::RemoteWrite);

        assert!(!rule_keys(&detector).is_empty());
        assert_eq!(marked, 0);
        assert!(rows
            .iter()
            .all(|row| !row.attributes.contains_key(DROPPED_LABELS_ATTRIBUTE)));
        let after: Vec<i64> = rows.iter().map(|r| r.series_fingerprint).collect();
        assert_eq!(before, after);
        assert!(rows
            .iter()
            .all(|row| row.attributes.contains_key("user_id")));
    }

    #[test]
    fn a_rule_does_not_apply_to_another_token() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);

        let mut other = remote_write_rows(10, true);
        detector.apply("phc_other", &mut other, FingerprintKind::RemoteWrite);
        assert!(other
            .iter()
            .all(|row| row.attributes.contains_key("user_id")));
    }

    #[test]
    fn a_rule_ends_when_the_key_stays_under_half_the_limit() {
        let (detector, _rx, now) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);
        assert!(!rule_keys(&detector).is_empty());

        // The sender drops to 50 users. Detection keeps running because the
        // rule pins the family, but never extends the rule.
        let ttl = settings(Mode::Enforce).rule_ttl.as_secs() as i64;
        for step in 1..=8 {
            now.store(START + step * ttl / 4, Ordering::SeqCst);
            detector.observe(TOKEN, &remote_write_rows(50, true));
        }
        let mut rows = remote_write_rows(50, true);
        detector.apply(TOKEN, &mut rows, FingerprintKind::RemoteWrite);
        assert!(rows
            .iter()
            .all(|row| row.attributes.contains_key("user_id")));
    }

    #[test]
    fn a_rule_stays_while_the_key_stays_over_half_the_limit() {
        let (detector, _rx, now) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        detector.observe(TOKEN, &rows);

        let ttl = settings(Mode::Enforce).rule_ttl.as_secs() as i64;
        for step in 1..=8 {
            now.store(START + step * ttl / 4, Ordering::SeqCst);
            detector.observe(TOKEN, &remote_write_rows(150, true));
        }
        let mut rows = remote_write_rows(150, true);
        detector.apply(TOKEN, &mut rows, FingerprintKind::RemoteWrite);
        assert!(rows
            .iter()
            .all(|row| !row.attributes.contains_key("user_id")));
    }

    #[tokio::test]
    async fn a_rule_found_on_one_pod_is_enforced_on_another() {
        let (pod_a, mut rx, _) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        pod_a.note_new_series(TOKEN, &rows, &all_rows_new(&rows));
        pod_a.observe(TOKEN, &rows);

        let mut pushed = Vec::new();
        while let Ok(rule) = rx.try_recv() {
            pushed.push(rule);
        }
        let mut client = MockRedisClient::new();
        for command in rule_commands(&pushed, 0) {
            if let PipelineCommand::ZAdd { key, members } = command {
                client = client.zrangebyscore_ret(
                    &key,
                    members.into_iter().map(|(_, member)| member).collect(),
                );
            }
        }

        let (pod_b, _rx, _) = detector(Mode::Enforce);
        assert_eq!(pod_b.pull_rules(&client).await.unwrap(), 2);
        let mut rows = remote_write_rows(10, true);
        pod_b.apply(TOKEN, &mut rows, FingerprintKind::RemoteWrite);
        assert!(rows
            .iter()
            .all(|row| !row.attributes.contains_key("user_id")));
    }

    #[test]
    fn sketch_estimate_stays_within_a_few_standard_errors() {
        for n in [100u64, 1_000, 10_000, 100_000] {
            let mut sketch = Sketch::new();
            for i in 0..n {
                sketch.insert(value_hash(&i.to_string()));
            }
            let error = (sketch.estimate() as f64 - n as f64).abs() / n as f64;
            assert!(error < 0.2, "n={n} estimate={}", sketch.estimate());
        }
    }
}

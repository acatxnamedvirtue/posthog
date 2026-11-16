//! Finds label keys that make a metric explode into many series, and drops
//! them before the series fingerprint is final. The README section "High
//! cardinality label drop" describes the detection stages and the rules.

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
/// Hour buckets by expiry expire on their own, so no prune command is needed.
const REDIS_BUCKET_SECS: i64 = 3600;
const PULL_PAGE_SIZE: usize = 10_000;
const WRITER_CHANNEL_CAPACITY: usize = 10_000;
const PRUNE_INTERVAL: Duration = Duration::from_secs(300);
const EVALUATE_INTERVAL_SECS: i64 = 10;

/// The row builders add these per sample after the fingerprint, so they are
/// never flagged and the recomputed fingerprint leaves them out too.
const ORIGINAL_TIMESTAMP_ATTRIBUTES: [&str; 2] =
    [ORIGINAL_TIMESTAMP_ATTRIBUTE, "$originalTimestamp"];
/// The fingerprint leaves this marker out, so a combined series keeps the
/// fingerprint of a series that never had the dropped key.
pub const DROPPED_LABELS_ATTRIBUTE: &str = "posthog.dropped_labels";
const HISTOGRAM_SUFFIXES: [&str; 3] = ["_bucket", "_count", "_sum"];
const COMPONENT_LABELS: [&str; 2] = ["le", "quantile"];
/// Both row builders copy this resource attribute into `service_name`, which
/// the fingerprint hashes on its own, so a rule can not drop it.
const SERVICE_NAME_RESOURCE: &str = "service.name";
const MAX_PENDING_PUSHES: usize = 10_000;
const HISTOGRAM_TYPE: &str = "histogram";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Off,
    /// Detect and share rules, but leave every row unchanged.
    Shadow,
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

/// A recomputed fingerprint must use the recipe of the request path that built
/// the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FingerprintKind {
    Otlp,
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
    pub window: Duration,
    pub value_limit: u64,
    pub min_share: f64,
    pub candidate_min_new_series: u32,
    pub max_candidates: usize,
    pub max_keys_per_candidate: usize,
    pub max_tracked_metrics: usize,
    pub max_tracked_metrics_per_token: usize,
    pub rule_ttl: Duration,
    pub protected_keys: HashSet<String>,
}

/// Rules and candidates hold hashes, not names, so the size of a sender's
/// metric names and label keys never decides how much memory they keep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub metric_key: u64,
    pub scope: Scope,
    pub key_hash: u64,
    /// Not in the Redis member, so every pod that extends a rule updates the
    /// score of one member instead of adding another.
    #[serde(skip)]
    pub expires_at: i64,
}

/// HyperLogLog with 256 one-byte registers: about 6.5% error, 256 bytes.
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
        if raw <= 2.5 * m && zeros > 0 {
            return (m * (m / zeros as f64).ln()).round() as u64;
        }
        raw.round() as u64
    }
}

struct Candidate {
    token: Arc<str>,
    window: i64,
    /// Last window in which the family passed stage 1. A candidate that stops
    /// qualifying gives up its slot, unless it has a rule.
    qualified_window: i64,
    series: Sketch,
    keys: HashMap<(Scope, u64), Sketch>,
    last_evaluated: i64,
}

impl Candidate {
    fn new(token: Arc<str>, window: i64) -> Self {
        Self {
            token,
            window,
            qualified_window: window,
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

struct NewSeriesCount {
    window: i64,
    count: u32,
    token_hash: u64,
}

struct RowHashes {
    series: u64,
    /// Scope, key hash and value hash of each label that can be dropped.
    values: Vec<(Scope, u64, u64)>,
}

pub struct LabelCardinality {
    settings: Settings,
    clock: Clock,
    new_series: DashMap<u64, NewSeriesCount>,
    tracked_count: AtomicUsize,
    tracked_per_token: DashMap<u64, usize>,
    candidates: DashMap<u64, Mutex<Candidate>>,
    /// Exact candidate count. `DashMap::len` walks every shard.
    candidate_count: AtomicUsize,
    rules: DashMap<u64, Vec<Rule>>,
    protected: HashSet<(Scope, u64)>,
    tx: Option<mpsc::Sender<Rule>>,
}

impl LabelCardinality {
    pub fn new(settings: Settings) -> (Arc<Self>, mpsc::Receiver<Rule>) {
        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        let detector = Self::build(settings, Some(tx), Arc::new(unix_now));
        (Arc::new(detector), rx)
    }

    pub fn local_only(settings: Settings) -> Arc<Self> {
        Arc::new(Self::build(settings, None, Arc::new(unix_now)))
    }

    fn build(settings: Settings, tx: Option<mpsc::Sender<Rule>>, clock: Clock) -> Self {
        let mut protected: HashSet<(Scope, u64)> = settings
            .protected_keys
            .iter()
            .flat_map(|key| {
                let hash = key_hash(key);
                [(Scope::Attribute, hash), (Scope::Resource, hash)]
            })
            .collect();
        for key in ORIGINAL_TIMESTAMP_ATTRIBUTES
            .iter()
            .chain([&DROPPED_LABELS_ATTRIBUTE])
        {
            protected.insert((Scope::Attribute, key_hash(key)));
        }
        protected.insert((Scope::Resource, key_hash(SERVICE_NAME_RESOURCE)));
        Self {
            protected,
            settings,
            clock,
            new_series: DashMap::new(),
            tracked_count: AtomicUsize::new(0),
            tracked_per_token: DashMap::new(),
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

    /// `new_rows` are the indices of the rows the series label gate kept
    /// labelled, which is one row per new series.
    pub fn note_new_series(
        &self,
        token: &str,
        rows: &[KafkaMetricRow],
        new_rows: &[usize],
        kind: FingerprintKind,
    ) {
        if self.settings.mode == Mode::Off || new_rows.is_empty() {
            return;
        }
        let now = (self.clock)();
        let window = self.window_index(now);
        let mut per_family: HashMap<&str, u32> = HashMap::new();
        for &index in new_rows {
            if let Some(row) = rows.get(index) {
                *per_family
                    .entry(family(&row.metric_name, kind))
                    .or_default() += 1;
            }
        }
        let token_hash = token_key(token);
        let mut token_label: Option<Arc<str>> = None;
        let mut tracking_full = 0u64;
        for (family_name, added) in per_family {
            let metric_key = metric_key(token, family_name);
            let qualified = {
                let mut entry = match self.new_series.entry(metric_key) {
                    dashmap::Entry::Occupied(entry) => entry.into_ref(),
                    dashmap::Entry::Vacant(slot) => {
                        if !self.reserve_tracking(token_hash) {
                            tracking_full += 1;
                            continue;
                        }
                        slot.insert(NewSeriesCount {
                            window,
                            count: 0,
                            token_hash,
                        })
                    }
                };
                if entry.window != window {
                    entry.window = window;
                    entry.count = 0;
                }
                let before = entry.count;
                entry.count = entry.count.saturating_add(added);
                before < self.settings.candidate_min_new_series
                    && entry.count >= self.settings.candidate_min_new_series
            };
            if !qualified {
                continue;
            }
            if let Some(slot) = self.candidates.get(&metric_key) {
                if let Ok(mut candidate) = slot.lock() {
                    candidate.qualified_window = window;
                }
                continue;
            }
            let token_label = token_label.get_or_insert_with(|| Arc::from(token));
            self.add_candidate(metric_key, token_label, window);
        }
        if tracking_full > 0 {
            counter!("capture_metrics_label_cardinality_tracking_full").increment(tracking_full);
        }
    }

    fn reserve_tracking(&self, token_hash: u64) -> bool {
        if self.tracked_count.fetch_add(1, Ordering::AcqRel) >= self.settings.max_tracked_metrics {
            self.tracked_count.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        let mut count = self.tracked_per_token.entry(token_hash).or_insert(0);
        if *count >= self.settings.max_tracked_metrics_per_token {
            drop(count);
            self.tracked_count.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        *count += 1;
        true
    }

    fn add_candidate(&self, metric_key: u64, token: &Arc<str>, window: i64) {
        if self.candidates.contains_key(&metric_key) {
            return;
        }
        if self.candidate_count.fetch_add(1, Ordering::AcqRel) >= self.settings.max_candidates {
            self.candidate_count.fetch_sub(1, Ordering::AcqRel);
            counter!("capture_metrics_label_cardinality_candidates_full").increment(1);
            return;
        }
        let candidate = Candidate::new(Arc::clone(token), window);
        if let dashmap::Entry::Vacant(slot) = self.candidates.entry(metric_key) {
            slot.insert(Mutex::new(candidate));
        } else {
            self.candidate_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Call it before [`Self::apply`], so the sketches see the labels that a
    /// rule drops.
    pub fn observe(&self, token: &str, rows: &[KafkaMetricRow], kind: FingerprintKind) {
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
                .or_insert_with(|| metric_key(token, family(&row.metric_name, kind)));
            if self.candidates.contains_key(&metric_key) {
                groups.entry(metric_key).or_default().push(index);
            } else if self.rules.contains_key(&metric_key) {
                // A rule hides the series growth from the gate, so the family
                // would never pass stage 1 again. The rule keeps it tracked.
                self.add_candidate(metric_key, &token_label, window);
                groups.entry(metric_key).or_default().push(index);
            }
        }

        for (metric_key, indices) in groups {
            // Hash outside the lock, so concurrent requests for one metric
            // wait only for the register updates.
            let hashes: Vec<RowHashes> = indices
                .iter()
                .map(|&index| self.row_hashes(&rows[index]))
                .collect();
            let Some(slot) = self.candidates.get(&metric_key) else {
                continue;
            };
            let Ok(mut candidate) = slot.lock() else {
                continue;
            };
            candidate.roll(window);
            for row in hashes {
                self.feed(&mut candidate, row);
            }
            if now.saturating_sub(candidate.last_evaluated) >= EVALUATE_INTERVAL_SECS {
                candidate.last_evaluated = now;
                let group: Vec<&KafkaMetricRow> = indices.iter().map(|&i| &rows[i]).collect();
                self.evaluate(metric_key, &candidate, now, &group);
            }
        }
    }

    /// The series hash leaves out the component labels, so the `_bucket`,
    /// `_count` and `_sum` series of one histogram count as one series.
    fn row_hashes(&self, row: &KafkaMetricRow) -> RowHashes {
        let mut series = value_hash(&row.service_name);
        let mut values = Vec::new();
        for (scope, map) in [
            (Scope::Attribute, &row.attributes),
            (Scope::Resource, &row.resource_attributes),
        ] {
            for (key, value) in map {
                if scope == Scope::Attribute
                    && (ORIGINAL_TIMESTAMP_ATTRIBUTES.contains(&key.as_str())
                        || COMPONENT_LABELS.contains(&key.as_str()))
                {
                    continue;
                }
                let key = key_hash(key);
                let hash = value_hash(value);
                // Label order is not stable, so the pairs combine with a sum.
                series = series.wrapping_add(pair_hash(scope, key, hash));
                if !self.protected.contains(&(scope, key)) {
                    values.push((scope, key, hash));
                }
            }
        }
        RowHashes { series, values }
    }

    fn feed(&self, candidate: &mut Candidate, row: RowHashes) {
        candidate.series.insert(row.series);
        let max_keys = self.settings.max_keys_per_candidate;
        let mut keys_full = 0u64;
        for (scope, key, hash) in row.values {
            if let Some(sketch) = candidate.keys.get_mut(&(scope, key)) {
                sketch.insert(hash);
                continue;
            }
            if candidate.keys.len() >= max_keys {
                keys_full += 1;
                continue;
            }
            let mut sketch = Sketch::new();
            sketch.insert(hash);
            candidate.keys.insert((scope, key), sketch);
        }
        if keys_full > 0 {
            counter!("capture_metrics_label_cardinality_keys_full").increment(keys_full);
        }
    }

    /// An active rule is extended at half the limit, so a key near the limit
    /// does not switch its rule on and off.
    /// `group` gives the names for the log line. They are read from this
    /// request and never kept.
    fn evaluate(
        &self,
        metric_key: u64,
        candidate: &Candidate,
        now: i64,
        group: &[&KafkaMetricRow],
    ) {
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
                    .find(|rule| {
                        rule.scope == *scope && rule.key_hash == *key && rule.expires_at > now
                    })
                    .map(|rule| rule.expires_at)
            });
            let new_rule = match existing {
                Some(expires_at) => {
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
                scope: *scope,
                key_hash: *key,
                expires_at: now + ttl,
            };
            if new_rule {
                let metric = group.first().map_or("", |row| row.metric_name.as_str());
                warn!(
                    token = %candidate.token,
                    metric = %truncated(metric),
                    scope = ?rule.scope,
                    key = %truncated(key_name(group, *scope, *key).unwrap_or("")),
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

    fn merge_rule(&self, rule: Rule) {
        let mut rules = self.rules.entry(rule.metric_key).or_default();
        match rules
            .iter_mut()
            .find(|r| r.scope == rule.scope && r.key_hash == rule.key_hash)
        {
            Some(existing) => existing.expires_at = existing.expires_at.max(rule.expires_at),
            None => rules.push(rule),
        }
    }

    /// Call it after [`Self::observe`] and before the series label gate.
    /// Returns the number of marked rows, so the caller can take the marker
    /// off the billed bytes.
    pub fn apply(&self, token: &str, rows: &mut [KafkaMetricRow], kind: FingerprintKind) -> u64 {
        if self.settings.mode == Mode::Off || self.rules.is_empty() {
            return 0;
        }
        let now = (self.clock)();
        let enforce = self.settings.mode == Mode::Enforce;
        let mut active_by_name: HashMap<String, Vec<(Scope, u64)>> = HashMap::new();
        let mut recomputed: HashMap<i64, i64> = HashMap::new();
        let mut changed = 0u64;

        for row in rows.iter_mut() {
            if !active_by_name.contains_key(&row.metric_name) {
                let active =
                    self.active_rules(metric_key(token, family(&row.metric_name, kind)), now);
                active_by_name.insert(row.metric_name.clone(), active);
            }
            let active = &active_by_name[&row.metric_name];
            if active.is_empty() {
                continue;
            }
            let mut dropped: Vec<String> = Vec::new();
            for (scope, map) in [
                (Scope::Attribute, &mut row.attributes),
                (Scope::Resource, &mut row.resource_attributes),
            ] {
                let matched: Vec<String> = map
                    .keys()
                    .filter(|key| active.contains(&(scope, key_hash(key))))
                    .cloned()
                    .collect();
                for key in matched {
                    if enforce {
                        map.remove(&key);
                    }
                    dropped.push(key);
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

    fn active_rules(&self, metric_key: u64, now: i64) -> Vec<(Scope, u64)> {
        self.rules
            .get(&metric_key)
            .map(|rules| {
                rules
                    .iter()
                    // A rule from Redis can name a key that this pod now
                    // protects.
                    .filter(|rule| {
                        rule.expires_at > now
                            && !self.protected.contains(&(rule.scope, rule.key_hash))
                    })
                    .map(|rule| (rule.scope, rule.key_hash))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn prune(&self, now: i64) {
        let window = self.window_index(now);
        self.rules.retain(|_, rules| {
            rules.retain(|rule| rule.expires_at > now);
            !rules.is_empty()
        });
        self.candidates.retain(|metric_key, candidate| {
            let qualified = candidate
                .get_mut()
                .map(|candidate| candidate.qualified_window >= window - 1)
                .unwrap_or(false);
            if qualified || self.rules.contains_key(metric_key) {
                return true;
            }
            self.candidate_count.fetch_sub(1, Ordering::AcqRel);
            false
        });
        self.new_series.retain(|_, count| {
            if count.window >= window {
                return true;
            }
            self.tracked_count.fetch_sub(1, Ordering::AcqRel);
            if let Some(mut per_token) = self.tracked_per_token.get_mut(&count.token_hash) {
                *per_token = per_token.saturating_sub(1);
            }
            false
        });
        self.tracked_per_token.retain(|_, count| *count > 0);
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

    pub async fn pull_rules(&self, client: &dyn Client) -> Result<usize, CustomRedisError> {
        let now = (self.clock)();
        let ttl = self.settings.rule_ttl.as_secs() as i64;
        let mut merged = 0usize;
        for (key, bucket_end) in bucket_keys(now, now + ttl) {
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
                        Ok(mut rule) => {
                            // The score is at most the end of its bucket, so
                            // the rule lasts at most one bucket longer here.
                            rule.expires_at = bucket_end;
                            self.merge_rule(rule);
                            merged += 1;
                        }
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

    /// A failed push is kept and sent again with the next push or pull, so a
    /// short Redis outage does not hide a rule until its next extension.
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
            let mut pending: Vec<Rule> = Vec::new();
            loop {
                tokio::select! {
                    rule = rx.recv() => {
                        let Some(rule) = rule else { return };
                        pending.push(rule);
                        while let Ok(rule) = rx.try_recv() {
                            pending.push(rule);
                        }
                        push_rules(client.as_ref(), &mut pending, ttl_secs, timeout).await;
                    }
                    _ = ticker.tick() => {
                        if !pending.is_empty() {
                            push_rules(client.as_ref(), &mut pending, ttl_secs, timeout).await;
                        }
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

async fn push_rules(
    client: &dyn Client,
    pending: &mut Vec<Rule>,
    ttl_secs: u64,
    timeout: Duration,
) {
    let pushed = pending.len() as u64;
    let commands = rule_commands(pending, ttl_secs);
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
    if outcome == "ok" {
        pending.clear();
    } else if pending.len() > MAX_PENDING_PUSHES {
        pending.drain(..pending.len() - MAX_PENDING_PUSHES);
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

/// The components of a remote-write classic histogram share one rule, or they
/// disagree on their labels and stop folding into one native histogram. OTLP
/// histograms are native, so an OTLP name is its own family.
fn family(metric_name: &str, kind: FingerprintKind) -> &str {
    if kind == FingerprintKind::Otlp {
        return metric_name;
    }
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

fn token_key(token: &str) -> u64 {
    let mut hasher = SipHasher13::new();
    hasher.write(token.as_bytes());
    hasher.finish()
}

fn pair_hash(scope: Scope, key_hash: u64, value_hash: u64) -> u64 {
    let mut hasher = SipHasher13::new();
    hasher.write_u8(scope as u8);
    hasher.write(&key_hash.to_le_bytes());
    hasher.write(&value_hash.to_le_bytes());
    hasher.finish()
}

/// Separate keys from `value_hash`, so a key and a value with equal text get
/// different hashes.
fn key_hash(key: &str) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(0, 1);
    hasher.write(key.as_bytes());
    hasher.finish()
}

fn key_name<'a>(group: &[&'a KafkaMetricRow], scope: Scope, hash: u64) -> Option<&'a str> {
    group.iter().find_map(|row| {
        let map = match scope {
            Scope::Attribute => &row.attributes,
            Scope::Resource => &row.resource_attributes,
        };
        map.keys()
            .find(|key| key_hash(key) == hash)
            .map(String::as_str)
    })
}

/// Log lines carry at most this much of a sender's name.
fn truncated(name: &str) -> &str {
    const MAX_LOGGED_CHARS: usize = 200;
    match name.char_indices().nth(MAX_LOGGED_CHARS) {
        Some((end, _)) => &name[..end],
        None => name,
    }
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

fn bucket_keys(from: i64, to: i64) -> Vec<(String, i64)> {
    let first = from.div_euclid(REDIS_BUCKET_SECS);
    let last = to.div_euclid(REDIS_BUCKET_SECS);
    (first..=last)
        .map(|bucket| {
            (
                format!("{REDIS_KEY_PREFIX}:{bucket}"),
                (bucket + 1) * REDIS_BUCKET_SECS,
            )
        })
        .collect()
}

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
    const RW: FingerprintKind = FingerprintKind::RemoteWrite;

    fn settings(mode: Mode) -> Settings {
        Settings {
            mode,
            window: Duration::from_secs(1800),
            value_limit: LIMIT,
            min_share: 0.5,
            candidate_min_new_series: 10,
            max_candidates: 100,
            max_keys_per_candidate: 32,
            max_tracked_metrics: 1000,
            max_tracked_metrics_per_token: 1000,
            rule_ttl: Duration::from_secs(7200),
            protected_keys: ["le".to_string(), "quantile".to_string()].into(),
        }
    }

    fn detector(mode: Mode) -> (LabelCardinality, mpsc::Receiver<Rule>, Arc<AtomicI64>) {
        detector_with(settings(mode))
    }

    fn detector_with(
        settings: Settings,
    ) -> (LabelCardinality, mpsc::Receiver<Rule>, Arc<AtomicI64>) {
        let now = Arc::new(AtomicI64::new(START));
        let clock_now = Arc::clone(&now);
        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        let detector = LabelCardinality::build(
            settings,
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

    fn rule_keys(detector: &LabelCardinality) -> HashSet<(u64, Scope, u64)> {
        detector
            .rules
            .iter()
            .flat_map(|rules| {
                rules
                    .iter()
                    .map(|r| (r.metric_key, r.scope, r.key_hash))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn rule_key(family: &str, scope: Scope, key: &str) -> (u64, Scope, u64) {
        (metric_key(TOKEN, family), scope, key_hash(key))
    }

    #[test]
    fn flags_only_the_key_that_drives_series_growth() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);

        assert_eq!(
            rule_keys(&detector),
            HashSet::from([
                rule_key("latency", Scope::Attribute, "user_id"),
                rule_key("requests_total", Scope::Attribute, "user_id"),
            ])
        );
    }

    #[test]
    fn a_family_under_the_candidate_threshold_is_never_measured() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &[0, 1, 2], RW);
        detector.observe(TOKEN, &rows, RW);

        assert!(rule_keys(&detector).is_empty());
    }

    #[test]
    fn a_key_with_many_values_but_a_small_share_of_series_is_not_flagged() {
        let (detector, _rx, _) = detector(Mode::Enforce);
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
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);

        assert_eq!(
            rule_keys(&detector),
            HashSet::from([
                rule_key("jobs_total", Scope::Attribute, "instance"),
                rule_key("jobs_total", Scope::Resource, "service.instance.id"),
            ])
        );
    }

    #[test]
    fn enforce_gives_the_fingerprint_of_rows_built_without_the_label() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let mut rows = remote_write_rows(1000, true);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);
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
        assert_eq!(fingerprints(&rows), fingerprints(&expected));
        assert_eq!(fingerprints(&rows)["latency"].len(), 4);
        assert!(rows
            .iter()
            .all(|row| !row.attributes.contains_key("user_id")));
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
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);
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
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);

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
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);
        assert!(!rule_keys(&detector).is_empty());

        let ttl = settings(Mode::Enforce).rule_ttl.as_secs() as i64;
        for step in 1..=8 {
            now.store(START + step * ttl / 4, Ordering::SeqCst);
            detector.observe(TOKEN, &remote_write_rows(50, true), RW);
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
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);

        let ttl = settings(Mode::Enforce).rule_ttl.as_secs() as i64;
        for step in 1..=8 {
            now.store(START + step * ttl / 4, Ordering::SeqCst);
            detector.observe(TOKEN, &remote_write_rows(150, true), RW);
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
        pod_a.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        pod_a.observe(TOKEN, &rows, RW);

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

    fn named_rows(names: &[String], series_per_metric: usize) -> Vec<KafkaMetricRow> {
        let timestamp = Utc::now().timestamp_millis();
        let series = names
            .iter()
            .flat_map(|name| {
                (0..series_per_metric).map(move |user| TimeSeries {
                    labels: vec![
                        label("__name__", name),
                        label("user_id", &format!("u{user}")),
                    ],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp,
                    }],
                })
            })
            .collect();
        write_timeseries_to_kafka_rows(series, &[]).0
    }

    #[test]
    fn histogram_components_split_across_requests_count_as_one_series() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let (rows, _) = write_timeseries_to_kafka_rows(histogram_series(1000, true), &[]);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);

        assert!(rule_keys(&detector).contains(&rule_key("latency", Scope::Attribute, "user_id")));
    }

    #[test]
    fn new_metric_names_stop_being_tracked_at_the_cap() {
        let mut capped = settings(Mode::Enforce);
        capped.max_tracked_metrics = 5;
        let (detector, _rx, _) = detector_with(capped);
        let names: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
        let rows = named_rows(&names, 1);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);

        assert_eq!(detector.new_series.len(), 5);
    }

    #[test]
    fn one_token_at_its_cap_leaves_room_for_another() {
        let mut capped = settings(Mode::Enforce);
        capped.max_tracked_metrics_per_token = 5;
        let (detector, _rx, _) = detector_with(capped);
        let names: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
        let rows = named_rows(&names, 1);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.note_new_series("phc_other", &rows, &all_rows_new(&rows), RW);

        assert_eq!(detector.new_series.len(), 10);
    }

    #[test]
    fn a_candidate_that_stops_qualifying_frees_its_slot() {
        let mut one_slot = settings(Mode::Enforce);
        one_slot.max_candidates = 1;
        let (detector, _rx, now) = detector_with(one_slot);
        let first = named_rows(&["first".to_string()], 20);
        detector.note_new_series(TOKEN, &first, &all_rows_new(&first), RW);

        let window = settings(Mode::Enforce).window.as_secs() as i64;
        now.store(START + 2 * window, Ordering::SeqCst);
        detector.observe(TOKEN, &first, RW);
        detector.prune(START + 2 * window);

        let second = named_rows(&["second".to_string()], 20);
        detector.note_new_series(TOKEN, &second, &all_rows_new(&second), RW);
        assert!(detector
            .candidates
            .contains_key(&metric_key(TOKEN, "second")));
    }

    fn rule_for(family: &str, scope: Scope, key: &str) -> Rule {
        Rule {
            metric_key: metric_key(TOKEN, family),
            scope,
            key_hash: key_hash(key),
            expires_at: START + 3600,
        }
    }

    #[test]
    fn a_rule_from_redis_does_not_drop_a_key_this_pod_protects() {
        let mut protecting = settings(Mode::Enforce);
        protecting.protected_keys.insert("user_id".to_string());
        let (detector, _rx, _) = detector_with(protecting);
        detector.merge_rule(rule_for("requests_total", Scope::Attribute, "user_id"));

        let mut rows = named_rows(&["requests_total".to_string()], 3);
        detector.apply(TOKEN, &mut rows, RW);
        assert!(rows
            .iter()
            .all(|row| row.attributes.contains_key("user_id")));
    }

    #[test]
    fn an_otlp_name_with_a_histogram_suffix_is_its_own_family() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        detector.merge_rule(rule_for("queue", Scope::Attribute, "user_id"));

        let mut otlp = named_rows(&["queue_count".to_string()], 3);
        detector.apply(TOKEN, &mut otlp, FingerprintKind::Otlp);
        assert!(otlp
            .iter()
            .all(|row| row.attributes.contains_key("user_id")));

        let mut remote_write = named_rows(&["queue_count".to_string()], 3);
        detector.apply(TOKEN, &mut remote_write, RW);
        assert!(remote_write
            .iter()
            .all(|row| !row.attributes.contains_key("user_id")));
    }

    #[test]
    fn service_name_is_never_flagged() {
        let (detector, _rx, _) = detector(Mode::Enforce);
        let timestamp = Utc::now().timestamp_millis();
        let series: Vec<TimeSeries> = (0..1000)
            .map(|i| TimeSeries {
                labels: vec![
                    label("__name__", "jobs_total"),
                    label("job", &format!("j{i}")),
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp,
                }],
            })
            .collect();
        let (rows, _) = write_timeseries_to_kafka_rows(series, &[]);
        detector.note_new_series(TOKEN, &rows, &all_rows_new(&rows), RW);
        detector.observe(TOKEN, &rows, RW);

        assert!(rule_keys(&detector).is_empty());
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

//! Reactive stores: one per subscribed target. A store reloads from its backend on relevant
//! changes (throttled per `delay`) and emits wire messages through its [`Sink`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};

use crate::backend::{ConnectionManager, Error, Middleware, ObservableBackend};
use crate::query::{changed_fields, collect_query_fields, matches};

const DEFAULT_DELAY_MS: u64 = 100;
const COUNT_THROTTLE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Scope {
	Many,
	One,
	Count,
}

impl Scope {
	pub(crate) fn parse(scope: &str) -> Self {
		match scope {
			"many" => Scope::Many,
			"count" => Scope::Count,
			_ => Scope::One,
		}
	}
}

/// Where a store's messages go: the client's outbox, through the channel's middleware.
pub(crate) struct Sink {
	pub out: UnboundedSender<Value>,
	pub middleware: Option<Middleware>,
	pub connection: Arc<dyn ConnectionManager>,
}

impl Sink {
	fn send(&self, message: Value) {
		let message = match &self.middleware {
			Some(middleware) => middleware(message, &self.connection.user()),
			None => message,
		};
		let _ = self.out.send(message);
	}
}

/// Everything derived from the current subscription config; cloned per load.
#[derive(Clone)]
struct View {
	config: Value,
	subscription_id: String,
	query: Value,
	sort: Value,
	fields: Value,
	paging: Value,
	populates: Vec<Value>,
	virtuals: Vec<Value>,
	delay: Duration,
	incremental: bool,
}

struct State {
	view: View,
	total_count: i64,
	/// subscriptionId -> did that config change the query (forces a recount)
	query_changed: HashMap<String, bool>,
	count_throttle: CountThrottle,
}

/// lodash `throttle(sendCount, 5000)` (leading + trailing), which is what v3 clients see.
#[derive(Default)]
struct CountThrottle {
	last_call: Option<Instant>,
	last_invoke: Option<Instant>,
	pending: Option<String>,
	timer: Option<JoinHandle<()>>,
}

impl CountThrottle {
	fn should_invoke(&self, now: Instant) -> bool {
		let elapsed = |t: Option<Instant>| t.is_none_or(|t| now - t >= COUNT_THROTTLE);
		elapsed(self.last_call) || elapsed(self.last_invoke)
	}

	fn remaining(&self, now: Instant) -> Duration {
		let left = |t: Option<Instant>| t.map_or(Duration::ZERO, |t| COUNT_THROTTLE.saturating_sub(now - t));
		left(self.last_call).min(left(self.last_invoke))
	}
}

pub(crate) struct Store {
	inner: Arc<Inner>,
	task: Option<JoinHandle<()>>,
}

struct Inner {
	scope: Scope,
	target: String,
	backend: Arc<dyn ObservableBackend>,
	sink: Sink,
	state: Mutex<State>,
}

impl Drop for Store {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			task.abort();
		}
		if let Some(timer) = self.inner.state.lock().unwrap().count_throttle.timer.take() {
			timer.abort();
		}
	}
}

impl Store {
	pub(crate) fn new(scope: Scope, target: String, backend: Arc<dyn ObservableBackend>, sink: Sink) -> Self {
		let view = View {
			config: json!({"query": {"___initial___": true}, "strict": false, "incremental": false}),
			subscription_id: String::new(),
			query: json!({}),
			sort: json!({}),
			fields: json!({}),
			paging: json!({}),
			populates: vec![],
			virtuals: vec![],
			delay: Duration::from_millis(DEFAULT_DELAY_MS),
			incremental: false,
		};
		let state = State {
			view,
			total_count: -1,
			query_changed: HashMap::new(),
			count_throttle: CountThrottle::default(),
		};
		let inner = Inner {
			scope,
			target,
			backend,
			sink,
			state: Mutex::new(state),
		};
		Store {
			inner: Arc::new(inner),
			task: None,
		}
	}

	/// Applies a new subscription config; an identical config (ignoring subscriptionId) is a no-op.
	pub(crate) fn set_config(&mut self, mut config: Value) {
		if !config.is_object() {
			return;
		}
		{
			let mut state = self.inner.state.lock().unwrap();
			// ponytail: only the top-level subscriptionId is ignored (v3 ignored it at any depth)
			if without_subscription_id(&state.view.config) == without_subscription_id(&config) {
				return;
			}
			let id = config
				.get("subscriptionId")
				.filter(|id| !is_falsy(id))
				.map(as_key)
				.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
			config["subscriptionId"] = id.clone().into();
			if self.inner.scope == Scope::Many {
				let query = |c: &Value| c.get("query").cloned().unwrap_or_else(|| json!({}));
				let changed = query(&state.view.config) != query(&config);
				state.query_changed.insert(id, changed);
			}
			state.view = View::from_config(config, self.inner.scope);
		}
		self.restart();
	}

	/// Reloads now, then on every (throttled) backend change.
	pub(crate) fn restart(&mut self) {
		if let Some(task) = self.task.take() {
			task.abort();
		}
		let inner = self.inner.clone();
		let mut changes = inner.backend.changes();
		self.task = Some(tokio::spawn(async move {
			inner.load(json!({})).await;

			// rxjs throttleTime(delay, {leading: true, trailing: true})
			let delay = inner.state.lock().unwrap().view.delay;
			let Some(mut next) = recv(&mut changes).await else { return };
			loop {
				let window_end = Instant::now() + delay;
				inner.load(next).await;
				let mut trailing = None;
				loop {
					tokio::select! {
						change = recv(&mut changes) => match change {
							Some(change) => trailing = Some(change),
							None => return,
						},
						_ = sleep_until(window_end) => break,
					}
				}
				next = match trailing {
					Some(change) => change,
					None => match recv(&mut changes).await {
						Some(change) => change,
						None => return,
					},
				};
			}
		}));
	}
}

/// Next change; a lagged receiver missed some, so it yields `{}` (reload everything).
async fn recv(changes: &mut broadcast::Receiver<Value>) -> Option<Value> {
	match changes.recv().await {
		Ok(change) => Some(change),
		Err(RecvError::Lagged(_)) => Some(json!({})),
		Err(RecvError::Closed) => None,
	}
}

impl View {
	fn from_config(config: Value, scope: Scope) -> Self {
		let object = |key: &str| config.get(key).cloned().unwrap_or_else(|| json!({}));
		let array = |key: &str| config.get(key).and_then(Value::as_array).cloned().unwrap_or_default();
		let number = |key: &str| config.get(key).and_then(Value::as_i64);

		let fields = match config.get("fields") {
			Some(Value::Array(paths)) => {
				let mut fields = json!({});
				paths.iter().filter_map(Value::as_str).for_each(|path| set_path(&mut fields, path, 1.into()));
				fields
			}
			Some(fields) => fields.clone(),
			None => json!({}),
		};
		let paging = match scope {
			Scope::Many => match number("pageSize").filter(|size| *size > 0) {
				Some(size) => json!({"skip": (number("page").unwrap_or(1) - 1) * size, "limit": size}),
				None => json!({}),
			},
			// sic: v3 sends {skip: 0, limit: 1} unless a skip is configured
			Scope::One => match number("skip").unwrap_or(0) {
				0 => json!({"skip": 0, "limit": 1}),
				_ => json!({}),
			},
			Scope::Count => json!({}),
		};
		View {
			subscription_id: config.get("subscriptionId").map(as_key).unwrap_or_default(),
			query: object("query"),
			sort: object("sort"),
			fields,
			paging,
			populates: array("populates"),
			virtuals: array("virtuals"),
			delay: Duration::from_millis(config.get("delay").and_then(Value::as_u64).unwrap_or(DEFAULT_DELAY_MS)),
			incremental: scope == Scope::Many && config.get("incremental").is_some_and(|v| !is_falsy(v)),
			config,
		}
	}

	fn touches_query(&self, change: &Value) -> bool {
		let changed = changed_fields(change);
		collect_query_fields(&self.query).iter().any(|field| changed.contains(field))
	}

	/// Does the document match the query (ignoring createdAt/updatedAt)? Unsupported queries count as a match.
	fn test_document(&self, document: &Value) -> bool {
		let mut query = self.query.clone();
		if let Some(query) = query.as_object_mut() {
			query.remove("createdAt");
			query.remove("updatedAt");
		}
		matches(&query, document).unwrap_or(true)
	}

	fn consider_fields(&self) -> bool {
		self.fields.as_object().is_some_and(|fields| !fields.is_empty() && !fields.values().any(|v| *v == 0))
	}

	fn fields_changed(&self, change: &Value) -> bool {
		let changed = changed_fields(change);
		self.fields.as_object().is_some_and(|fields| fields.keys().any(|field| changed.contains(field)))
	}

	fn statistics(&self) -> Map<String, Value> {
		let text = |value: &Value| Value::String(value.to_string());
		let mut stats = Map::new();
		stats.insert("query".into(), text(&self.query));
		stats.insert("sort".into(), text(&self.sort));
		stats.insert("fields".into(), text(&self.fields));
		stats.insert("paging".into(), text(&self.paging));
		stats.insert("populates".into(), text(&Value::Array(self.populates.clone())));
		stats.insert("virtuals".into(), text(&Value::Array(self.virtuals.clone())));
		stats
	}
}

impl Inner {
	fn view(&self) -> View {
		self.state.lock().unwrap().view.clone()
	}

	async fn load(self: &Arc<Self>, change: Value) {
		let start = Instant::now();
		let view = self.view();
		let result = match self.scope {
			Scope::Many => self.load_many(&view, &change, start).await,
			Scope::One => self.load_one(&view, &change, start).await,
			Scope::Count => self.load_count(&view, &change, start).await,
		};
		// v3 sent `{}` for most errors (a serialized Error); the message is more useful.
		if let Err(error) = result {
			self.emit(&view, start, json!({"type": "error", "error": error.to_string(), "target": self.target}));
		}
	}

	// ---- count ----

	async fn load_count(&self, view: &View, change: &Value, start: Instant) -> Result<(), Error> {
		let reload = is_empty(change)
			|| match operation(change) {
				"delete" | "insert" => true,
				"replace" | "update" => !has(change, "updateDescription") || view.touches_query(change),
				_ => false,
			};
		if reload {
			let count = self.backend.count(&view.query).await?;
			self.emit_one(view, start, count.into());
		}
		Ok(())
	}

	// ---- one ----

	async fn load_one(&self, view: &View, change: &Value, start: Instant) -> Result<(), Error> {
		let id = id_from_query(&view.query);
		let key = document_key(change);
		let initial = is_empty(change);

		// v3 filtered stream changes before throttling; same result here
		let passes = initial || !is_empty(&view.sort) || operation(change) == "delete" || key == id || view.touches_query(change) || view.test_document(full_document(change));
		let reload = initial
			|| match operation(change) {
				"delete" => true,
				"insert" => id.is_empty(),
				"replace" | "update" => {
					(!id.is_empty() && id == key) || !has(change, "updateDescription") || view.touches_query(change) || !view.consider_fields() || view.fields_changed(change)
				}
				_ => false,
			};
		if !passes || !reload {
			return Ok(());
		}

		if operation(change) == "delete" && id == key {
			self.emit(view, start, json!({"type": "delete", "target": self.target, "payload": key}));
			return Ok(());
		}

		let data = if !is_empty(&view.sort) {
			self.backend.find(&view.query, &view.fields, &view.paging, &view.sort, &[]).await?.into_iter().next()
		} else if !id.is_empty() {
			self.backend.find_by_id(&id, &view.fields, &[]).await?
		} else {
			self.backend.find_one(&view.query, &view.fields, &[]).await?
		};
		let Some(mut data) = data else {
			self.emit_one(view, start, json!({}));
			return Ok(());
		};
		for populate in &view.populates {
			self.backend.populate(&mut data, populate).await?;
		}
		let data = if view.virtuals.is_empty() {
			self.backend.to_json(data)
		} else {
			self.backend.resolve_virtuals(data, &view.virtuals).await?
		};
		self.emit_one(view, start, data);
		Ok(())
	}

	// ---- many ----

	async fn load_many(self: &Arc<Self>, view: &View, change: &Value, start: Instant) -> Result<(), Error> {
		let reload = is_empty(change)
			|| !has(change, "updateDescription")
			|| view.touches_query(change)
			|| match operation(change) {
				"delete" | "insert" => true,
				"replace" | "update" => {
					if view.consider_fields() {
						view.fields_changed(change)
					} else {
						view.test_document(full_document(change))
					}
				}
				_ => false,
			};
		if !reload {
			return Ok(());
		}

		match change.get("fullDocument").filter(|d| !d.is_null()) {
			Some(document) if view.incremental => self.load_incremental(view, change, document.clone(), start).await,
			_ => self.load_all(view, start).await,
		}
	}

	async fn load_incremental(&self, view: &View, change: &Value, mut document: Value, start: Instant) -> Result<(), Error> {
		if operation(change) == "delete" || !view.test_document(&document) {
			self.emit(view, start, json!({"type": "delete", "target": self.target, "payload": document_key(change)}));
			return Ok(());
		}
		for populate in &view.populates {
			self.backend.populate(&mut document, populate).await?;
		}
		if !view.virtuals.is_empty() {
			document = self.backend.resolve_virtuals(document, &view.virtuals).await?;
		}
		self.emit_many(view, start, document, None, false);
		Ok(())
	}

	async fn load_all(self: &Arc<Self>, view: &View, start: Instant) -> Result<(), Error> {
		let mut documents = self.backend.find(&view.query, &view.fields, &view.paging, &view.sort, &view.populates).await?;
		if !view.virtuals.is_empty() {
			let mut resolved = Vec::with_capacity(documents.len());
			for document in documents {
				resolved.push(self.backend.resolve_virtuals(document, &view.virtuals).await?);
			}
			documents = resolved;
		}

		let (total, query_changed) = {
			let state = self.state.lock().unwrap();
			(state.total_count, state.query_changed.get(&view.subscription_id).copied().unwrap_or(false))
		};
		self.emit_many(view, start, Value::Array(documents), Some(total), query_changed);
		if query_changed || self.throttle_count(&view.subscription_id) {
			self.send_count(view).await?;
		}
		self.state.lock().unwrap().query_changed.remove(&view.subscription_id);
		Ok(())
	}

	async fn send_count(&self, view: &View) -> Result<(), Error> {
		let start = Instant::now();
		let total = self.backend.count(&view.query).await? as i64;
		self.state.lock().unwrap().total_count = total;
		self.emit(view, start, json!({"type": "total", "target": self.target, "total": total}));
		Ok(())
	}

	/// Registers a recount request; true = send it now (leading edge), otherwise a timer sends the latest one later.
	fn throttle_count(self: &Arc<Self>, subscription_id: &str) -> bool {
		let now = Instant::now();
		let mut state = self.state.lock().unwrap();
		let throttle = &mut state.count_throttle;
		let invoke = throttle.should_invoke(now);
		throttle.last_call = Some(now);
		throttle.pending = Some(subscription_id.to_string());
		if invoke {
			if let Some(timer) = throttle.timer.take() {
				timer.abort();
			}
			throttle.pending = None;
			throttle.last_invoke = Some(now);
		}
		if throttle.timer.is_none() {
			throttle.timer = Some(self.spawn_count_timer());
		}
		invoke
	}

	fn spawn_count_timer(self: &Arc<Self>) -> JoinHandle<()> {
		let inner = self.clone();
		tokio::spawn(async move {
			let mut wait = COUNT_THROTTLE;
			loop {
				sleep(wait).await;
				let now = Instant::now();
				let pending = {
					let mut state = inner.state.lock().unwrap();
					let throttle = &mut state.count_throttle;
					if !throttle.should_invoke(now) {
						wait = throttle.remaining(now);
						continue;
					}
					throttle.timer = None;
					let pending = throttle.pending.take();
					if pending.is_some() {
						throttle.last_invoke = Some(now);
					}
					pending
				};
				if let Some(subscription_id) = pending {
					let view = View { subscription_id, ..inner.view() };
					if let Err(error) = inner.send_count(&view).await {
						inner.emit(&view, now, json!({"type": "error", "error": error.to_string(), "target": inner.target}));
					}
				}
				return;
			}
		})
	}

	// ---- messages ----

	fn emit(&self, view: &View, start: Instant, message: Value) {
		let mut out = Map::new();
		out.insert("subscriptionId".into(), view.subscription_id.clone().into());
		if let Value::Object(message) = message {
			out.extend(message);
		}
		out.insert("execution_time".into(), format!("{:.2}ms", start.elapsed().as_secs_f64() * 1000.0).into());
		out.extend(view.statistics());
		self.sink.send(Value::Object(out));
	}

	fn update_type(view: &View) -> &'static str {
		if view.incremental { "increment" } else { "update" }
	}

	fn emit_one(&self, view: &View, start: Instant, update: Value) {
		let mut payload = json!({});
		set_path(&mut payload, &self.target, update);
		self.emit(view, start, json!({"type": Self::update_type(view), "target": self.target, "payload": payload}));
	}

	fn emit_many(&self, view: &View, start: Instant, data: Value, total: Option<i64>, recounting: bool) {
		let mut payload = json!({});
		set_path(&mut payload, &self.target, data);
		if let Some(total) = total.filter(|total| !view.incremental && *total >= 0) {
			set_path(&mut payload, &format!("_{}Count", self.target), total.into());
		}
		if recounting {
			set_path(&mut payload, &format!("_{}Recounting", self.target), true.into());
		}
		self.emit(view, start, json!({"type": Self::update_type(view), "target": self.target, "payload": payload}));
	}
}

// ---- JSON helpers (lodash semantics where v3 relied on them) ----

/// lodash `set` for dotted paths (no array/bracket syntax).
pub(crate) fn set_path(target: &mut Value, path: &str, value: Value) {
	let mut current = target;
	let mut parts = path.split('.').peekable();
	while let Some(part) = parts.next() {
		if !current.is_object() {
			*current = json!({});
		}
		let map = current.as_object_mut().unwrap();
		if parts.peek().is_none() {
			map.insert(part.to_string(), value);
			return;
		}
		current = map.entry(part).or_insert_with(|| json!({}));
	}
}

/// lodash `isEmpty`.
fn is_empty(value: &Value) -> bool {
	match value {
		Value::Object(map) => map.is_empty(),
		Value::Array(entries) => entries.is_empty(),
		Value::String(s) => s.is_empty(),
		_ => true,
	}
}

fn is_falsy(value: &Value) -> bool {
	matches!(value, Value::Null | Value::Bool(false)) || value.as_str() == Some("") || value.as_f64() == Some(0.0)
}

fn has(change: &Value, key: &str) -> bool {
	change.get(key).is_some_and(|value| !value.is_null())
}

/// `String(value)` for ids: strings as-is, null as empty, anything else as JSON.
fn as_key(value: &Value) -> String {
	match value {
		Value::String(s) => s.clone(),
		Value::Null => String::new(),
		other => other.to_string(),
	}
}

fn operation(change: &Value) -> &str {
	change.get("operationType").and_then(Value::as_str).unwrap_or("")
}

fn document_key(change: &Value) -> String {
	change.get("documentKey").and_then(|key| key.get("_id")).map(as_key).unwrap_or_default()
}

fn full_document(change: &Value) -> &Value {
	change.get("fullDocument").unwrap_or(&Value::Null)
}

fn id_from_query(query: &Value) -> String {
	match query {
		Value::String(id) => id.clone(),
		_ => query.get("_id").filter(|id| id.is_string() || id.is_number()).map(as_key).unwrap_or_default(),
	}
}

fn without_subscription_id(config: &Value) -> Value {
	let mut config = config.clone();
	if let Some(config) = config.as_object_mut() {
		config.remove("subscriptionId");
	}
	config
}

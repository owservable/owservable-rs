use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexMap;
use serde_json::Value;
use tokio::sync::broadcast;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Post-processes every store message for one `observe` channel, given the connection's user.
pub type Middleware = Arc<dyn Fn(Value, &Value) -> Value + Send + Sync>;

/// A database the stores query and watch.
///
/// `changes()` yields MongoDB-shaped change events:
/// `{operationType, documentKey: {_id}, updateDescription?: {updatedFields, removedFields}, fullDocument?}`.
/// A lagged receiver makes the stores reload everything.
#[async_trait]
pub trait ObservableBackend: Send + Sync {
	fn changes(&self) -> broadcast::Receiver<Value>;

	async fn find(&self, query: &Value, fields: &Value, paging: &Value, sort: &Value, populates: &[Value]) -> Result<Vec<Value>, Error>;

	async fn find_one(&self, query: &Value, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error>;

	async fn find_by_id(&self, id: &str, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error>;

	async fn count(&self, query: &Value) -> Result<u64, Error>;

	async fn populate(&self, _document: &mut Value, _populate: &Value) -> Result<(), Error> {
		Ok(())
	}

	fn to_json(&self, document: Value) -> Value {
		document
	}

	async fn resolve_virtuals(&self, document: Value, _virtuals: &[Value]) -> Result<Value, Error> {
		Ok(document)
	}
}

/// Per-socket session hooks. Everything is optional.
#[async_trait]
pub trait ConnectionManager: Send + Sync {
	fn user(&self) -> Value {
		Value::Null
	}

	async fn connected(&self, _jwt: &Value) {}

	async fn ping(&self, _ms: i64) {}

	async fn location(&self, _path: &str) {}

	async fn disconnected(&self) {}

	/// A `{type: "refresh", payload, refresh_in}` message to send, if any. Re-checked at 95% of `refresh_in` (default 5 min).
	async fn check_session(&self) -> Option<Value> {
		None
	}
}

/// Backends and middlewares by `observe` name.
#[derive(Default)]
pub struct Registry {
	backends: IndexMap<String, Arc<dyn ObservableBackend>>,
	middlewares: HashMap<String, Middleware>,
}

impl Registry {
	/// Returns false (and keeps the existing one) if `observe` is already registered.
	pub fn register(&mut self, observe: impl Into<String>, backend: Arc<dyn ObservableBackend>) -> bool {
		let observe = observe.into();
		if self.backends.contains_key(&observe) {
			return false;
		}
		self.backends.insert(observe, backend);
		true
	}

	pub fn add_middleware(&mut self, observe: impl Into<String>, middleware: impl Fn(Value, &Value) -> Value + Send + Sync + 'static) {
		self.middlewares.insert(observe.into(), Arc::new(middleware));
	}

	pub(crate) fn backend(&self, observe: &str) -> Option<Arc<dyn ObservableBackend>> {
		self.backends.get(observe).cloned()
	}

	pub(crate) fn middleware(&self, observe: &str) -> Option<Middleware> {
		self.middlewares.get(observe).cloned()
	}

	pub(crate) fn keys(&self) -> Vec<&str> {
		self.backends.keys().map(String::as_str).collect()
	}
}

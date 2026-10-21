use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::backend::{ConnectionManager, Error, Registry};
use crate::store::{Scope, Sink, Store};

const PING_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_REFRESH_IN_MS: f64 = 300_000.0;

/// One per websocket: feed it incoming messages with [`Client::consume`], forward everything
/// from the returned receiver to the socket.
pub struct Client {
	registry: Arc<Registry>,
	connection: Arc<dyn ConnectionManager>,
	out: UnboundedSender<Value>,
	stores: IndexMap<String, Store>,
	location: Option<String>,
	session: Option<JoinHandle<()>>,
	ping: Option<JoinHandle<()>>,
}

impl Drop for Client {
	fn drop(&mut self) {
		self.session.take().inspect(JoinHandle::abort);
		self.ping.take().inspect(JoinHandle::abort);
	}
}

impl Client {
	pub fn new(registry: Arc<Registry>, connection: Arc<dyn ConnectionManager>) -> (Self, UnboundedReceiver<Value>) {
		let (out, messages) = unbounded_channel();
		let client = Client {
			registry,
			connection,
			out,
			stores: IndexMap::new(),
			location: None,
			session: None,
			ping: None,
		};
		(client, messages)
	}

	/// Handles one incoming message. Errors only for a subscription to an unregistered `observe`.
	pub async fn consume(&mut self, message: Value) -> Result<(), Error> {
		let text = |key: &str| message.get(key).and_then(Value::as_str).unwrap_or("");
		match text("type") {
			"pong" => {
				let sent = message.get("id").and_then(Value::as_i64).unwrap_or(0);
				self.connection.ping(now_ms() - sent).await;
			}
			"authenticate" => {
				self.connection.connected(message.get("jwt").unwrap_or(&Value::Null)).await;
				self.check_session();
			}
			"location" => {
				let path = text("path");
				if self.location.as_deref() != Some(path) {
					self.location = Some(path.to_string());
					self.connection.location(path).await;
				}
			}
			"subscribe" => self.subscribe(&message)?,
			"unsubscribe" => {
				let target = text("target");
				self.stores.shift_remove(target);
				self.debug("removeSubscription", target);
			}
			"reload" => {
				if let Some(store) = self.stores.get_mut(text("target")) {
					store.restart();
				}
			}
			_ => {}
		}
		Ok(())
	}

	/// Sends `{type: "ping", id}` now and every minute; the client's `pong` reports the round trip.
	pub fn start_ping(&mut self) {
		self.ping.take().inspect(JoinHandle::abort);
		let out = self.out.clone();
		self.ping = Some(tokio::spawn(async move {
			while out.send(json!({"type": "ping", "id": now_ms()})).is_ok() {
				sleep(PING_INTERVAL).await;
			}
		}));
	}

	pub async fn disconnected(&mut self) {
		self.stores.clear();
		self.connection.disconnected().await;
		self.session.take().inspect(JoinHandle::abort);
		self.ping.take().inspect(JoinHandle::abort);
	}

	fn subscribe(&mut self, message: &Value) -> Result<(), Error> {
		let text = |key: &str| message.get(key).and_then(Value::as_str).unwrap_or("");
		let (target, observe) = (text("target"), text("observe"));

		if !self.stores.contains_key(target) {
			let backend = self.registry.backend(observe).ok_or_else(|| {
				let registered = self.registry.keys().join(", ");
				format!(
					"no backend registered for observe \"{observe}\" (target \"{target}\"). Registered backends: {}",
					if registered.is_empty() { "<none>" } else { &registered }
				)
			})?;
			let sink = Sink {
				out: self.out.clone(),
				middleware: self.registry.middleware(observe),
				connection: self.connection.clone(),
			};
			self.stores
				.insert(target.to_string(), Store::new(Scope::parse(text("scope")), target.to_string(), backend, sink));
		}
		if let Some(store) = self.stores.get_mut(target) {
			store.set_config(message.get("config").cloned().unwrap_or(Value::Null));
		}

		self.debug("updateSubscription", target);
		Ok(())
	}

	fn check_session(&mut self) {
		self.session.take().inspect(JoinHandle::abort);
		let (connection, out) = (self.connection.clone(), self.out.clone());
		self.session = Some(tokio::spawn(async move {
			loop {
				let check = connection.check_session().await;
				let refresh_in = check.as_ref().and_then(|c| c.get("refresh_in")).and_then(Value::as_f64).unwrap_or(DEFAULT_REFRESH_IN_MS);
				if let Some(check) = check {
					let _ = out.send(check);
				}
				sleep(Duration::from_millis((refresh_in * 0.95).round() as u64)).await;
			}
		}));
	}

	fn debug(&self, event: &str, target: &str) {
		let targets: Vec<&str> = self.stores.keys().map(String::as_str).collect();
		let payload = json!({"event": event, "target": target, "availableTargets": targets.join(", ")});
		let _ = self.out.send(json!({"type": "debug", "id": now_ms(), "payload": payload}));
	}
}

fn now_ms() -> i64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

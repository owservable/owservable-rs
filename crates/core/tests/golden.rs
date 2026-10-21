//! Replays the fixtures recorded from @owservable/core v3 (tools/record-golden) and expects
//! the same messages, step by step. The in-memory backend mirrors the recorder's MemoryBackend.

use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use owservable_core::{Client, ConnectionManager, Error, ObservableBackend, Registry, query};
use serde_json::{Value, json};
use tokio::sync::broadcast;

struct Memory {
	collection: String,
	docs: Mutex<Vec<Value>>,
	changes: broadcast::Sender<Value>,
}

impl Memory {
	fn new(collection: &str, docs: &Value) -> Arc<Self> {
		let docs = docs.as_array().unwrap().clone();
		Arc::new(Memory {
			collection: collection.into(),
			docs: Mutex::new(docs),
			changes: broadcast::channel(64).0,
		})
	}

	fn matching(&self, query: &Value) -> Vec<Value> {
		self.docs.lock().unwrap().iter().filter(|doc| query::matches(query, doc).unwrap()).cloned().collect()
	}

	fn apply(&self, change: &Value) {
		let ns = json!({"db": "test", "coll": self.collection});
		let mut docs = self.docs.lock().unwrap();
		let event = match change["op"].as_str().unwrap() {
			"insert" => {
				docs.push(change["doc"].clone());
				json!({"ns": ns, "documentKey": {"_id": change["doc"]["_id"]}, "operationType": "insert", "fullDocument": change["doc"]})
			}
			"update" => {
				let doc = docs.iter_mut().find(|doc| doc["_id"] == change["id"]).unwrap();
				for (key, value) in change["set"].as_object().unwrap() {
					doc[key] = value.clone();
				}
				let description = json!({"updatedFields": change["set"], "removedFields": []});
				json!({"ns": ns, "documentKey": {"_id": change["id"]}, "operationType": "update", "updateDescription": description, "fullDocument": doc})
			}
			"delete" => {
				docs.retain(|doc| doc["_id"] != change["id"]);
				json!({"ns": ns, "documentKey": {"_id": change["id"]}, "operationType": "delete"})
			}
			op => panic!("unknown op {op}"),
		};
		drop(docs);
		let _ = self.changes.send(event);
	}
}

#[async_trait]
impl ObservableBackend for Memory {
	fn changes(&self) -> broadcast::Receiver<Value> {
		self.changes.subscribe()
	}

	async fn find(&self, query: &Value, _fields: &Value, paging: &Value, sort: &Value, _populates: &[Value]) -> Result<Vec<Value>, Error> {
		let mut docs = self.matching(query);
		if let Some(sort) = sort.as_object() {
			docs.sort_by(|a, b| {
				for (key, direction) in sort {
					let order = query::compare(&a[key], &b[key]).unwrap_or(Ordering::Equal);
					if order != Ordering::Equal {
						return if direction.as_i64() == Some(-1) { order.reverse() } else { order };
					}
				}
				Ordering::Equal
			});
		}
		let skip = paging["skip"].as_u64().unwrap_or(0) as usize;
		let limit = paging["limit"].as_u64().map_or(usize::MAX, |limit| limit as usize);
		Ok(docs.into_iter().skip(skip).take(limit).collect())
	}

	async fn find_one(&self, query: &Value, _fields: &Value, _populates: &[Value]) -> Result<Option<Value>, Error> {
		Ok(self.matching(query).into_iter().next())
	}

	async fn find_by_id(&self, id: &str, _fields: &Value, _populates: &[Value]) -> Result<Option<Value>, Error> {
		Ok(self.docs.lock().unwrap().iter().find(|doc| doc["_id"] == id).cloned())
	}

	async fn count(&self, query: &Value) -> Result<u64, Error> {
		Ok(self.matching(query).len() as u64)
	}
}

struct Session;

#[async_trait]
impl ConnectionManager for Session {
	fn user(&self) -> Value {
		json!({"id": "u1"})
	}

	async fn check_session(&self) -> Option<Value> {
		Some(json!({"type": "refresh", "payload": {"jwt": "refreshed-jwt", "user": {"id": "u1"}}, "refresh_in": 600000}))
	}
}

fn normalize(mut message: Value) -> Value {
	if message.get("execution_time").is_some() {
		message["execution_time"] = "<ms>".into();
	}
	if matches!(message["type"].as_str(), Some("debug" | "ping")) && message.get("id").is_some() {
		message["id"] = 0.into();
	}
	// error texts differ between implementations; only that it was thrown matters
	if message.get("thrown").is_some() {
		message["thrown"] = true.into();
	}
	message
}

/// Lets spawned store tasks run to their next real wait, like JS microtasks after a synchronous emit.
async fn settle() {
	for _ in 0..20 {
		tokio::task::yield_now().await;
	}
}

async fn replay(fixture: &Value) -> Vec<String> {
	let name = fixture["name"].as_str().unwrap();
	let items = Memory::new("items", &fixture["docs"]);
	let tagged = Memory::new("tagged", &fixture["docs"]);

	let mut registry = Registry::default();
	registry.register("items", items.clone());
	registry.register("tagged", tagged.clone());
	registry.add_middleware("tagged", |mut message, _user| {
		message["tagged"] = true.into();
		message
	});
	let (mut client, mut messages) = Client::new(Arc::new(registry), Arc::new(Session));

	let mut failures = vec![];
	for (i, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
		let mut outputs = vec![];
		if let Some(input) = step.get("input")
			&& let Err(error) = client.consume(input.clone()).await
		{
			outputs.push(json!({"thrown": error.to_string()}));
		}
		for change in step.get("changes").and_then(Value::as_array).into_iter().flatten() {
			items.apply(change);
			tagged.apply(change);
			settle().await;
		}
		if step.get("disconnect").is_some() {
			client.disconnected().await;
		}
		let wait = step["wait"].as_u64().or(fixture["settleMs"].as_u64()).unwrap();
		tokio::time::sleep(Duration::from_millis(wait)).await;
		while let Ok(message) = messages.try_recv() {
			outputs.push(message);
		}

		let actual: Vec<Value> = outputs.into_iter().map(normalize).collect();
		let expected: Vec<Value> = step["outputs"].as_array().unwrap().iter().cloned().map(normalize).collect();
		if actual != expected {
			failures.push(format!(
				"{name} step {i}:\n  expected: {}\n  actual:   {}",
				serde_json::to_string(&expected).unwrap(),
				serde_json::to_string(&actual).unwrap()
			));
		}
	}
	failures
}

#[tokio::test(start_paused = true)]
async fn golden_fixtures_match_v3() {
	let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");
	let mut paths: Vec<_> = std::fs::read_dir(dir).unwrap().map(|entry| entry.unwrap().path()).collect();
	paths.sort();
	assert!(!paths.is_empty(), "no fixtures in {dir}");

	let mut failures = vec![];
	for path in paths {
		let fixture: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
		failures.extend(replay(&fixture).await);
	}
	assert!(failures.is_empty(), "{} step(s) differ:\n{}", failures.len(), failures.join("\n"));
}

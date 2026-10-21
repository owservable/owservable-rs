//! Against a real database file: changes written by a separate connection (as another process
//! would) arrive as change events, SQL queries agree with the stores' matcher, and a Client
//! subscribed through the full stack receives live updates.

use std::sync::Arc;
use std::time::Duration;

use owservable_core::{Client, ConnectionManager, ObservableBackend, Registry, query};
use owservable_sqlite::Sqlite;
use rusqlite::Connection;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);

struct TempDb(std::path::PathBuf);

impl Drop for TempDb {
	fn drop(&mut self) {
		for suffix in ["", "-wal", "-shm"] {
			let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
		}
	}
}

async fn next_change(changes: &mut broadcast::Receiver<Value>) -> Value {
	timeout(WAIT, changes.recv()).await.expect("no change event in time").unwrap()
}

struct Anonymous;
impl ConnectionManager for Anonymous {}

#[tokio::test]
async fn live_updates_from_another_connection() {
	let db = TempDb(std::env::temp_dir().join(format!("owservable-sqlite-test-{}.db", std::process::id())));
	let external = Connection::open(&db.0).unwrap();
	external.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(())).unwrap();
	external
		.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, amount INTEGER, status TEXT)")
		.unwrap();

	let sqlite = Sqlite::open(&db.0, Duration::from_millis(20)).unwrap();
	let backend = sqlite.watch("items").unwrap();
	let mut changes = backend.changes();

	// insert -> refetched fullDocument, text documentKey like v3
	external.execute("INSERT INTO items (name, amount, status) VALUES ('a', 1, 'open')", []).unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "insert");
	assert_eq!(change["documentKey"], json!({"_id": "1"}));
	assert_eq!(change["fullDocument"], json!({"id": 1, "name": "a", "amount": 1, "status": "open"}));

	// update -> only the changed columns
	external.execute("UPDATE items SET amount = 9 WHERE id = 1", []).unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "update");
	assert_eq!(change["updateDescription"], json!({"updatedFields": {"amount": 9}, "removedFields": []}));
	assert_eq!(change["fullDocument"]["amount"], 9);

	// delete -> no refetch
	external.execute("DELETE FROM items WHERE id = 1", []).unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "delete");
	assert_eq!(change["documentKey"], json!({"_id": "1"}));
	assert!(change.get("fullDocument").is_none());

	// queries agree with the matcher the stores use (including NULL handling)
	external
		.execute_batch("INSERT INTO items (name, amount, status) VALUES ('b', 5, 'open'), ('c', 7, 'closed'), ('d', NULL, NULL), ('e', 2, 'open'), ('f', 7, NULL)")
		.unwrap();
	let all = backend.find(&json!({}), &json!({}), &json!({}), &json!({"id": 1}), &[]).await.unwrap();
	assert_eq!(all.len(), 5);
	for query in [
		json!({"amount": {"$gte": 5}}),
		json!({"status": {"$ne": "open"}}),
		json!({"status": {"$in": ["closed", null]}}),
		json!({"status": {"$nin": ["open"]}}),
		json!({"amount": {"$exists": false}}),
		json!({"$or": [{"amount": 2}, {"name": "c"}], "status": {"$ne": null}}),
		json!({"$nor": [{"status": "open"}, {"amount": 7}]}),
		json!({"name": {"$not": {"$in": ["b", "c"]}}}),
	] {
		let found = backend.find(&query, &json!({}), &json!({}), &json!({"id": 1}), &[]).await.unwrap();
		let expected: Vec<Value> = all.iter().filter(|doc| query::matches(&query, doc).unwrap()).cloned().collect();
		assert_eq!(found, expected, "{query}");
		assert_eq!(backend.count(&query).await.unwrap(), expected.len() as u64, "{query}");
	}

	// sort, paging, projection, id lookup
	let page = backend
		.find(&json!({}), &json!({"name": 1}), &json!({"skip": 1, "limit": 2}), &json!({"amount": -1, "id": 1}), &[])
		.await
		.unwrap();
	assert_eq!(page, vec![json!({"id": 6, "name": "f"}), json!({"id": 2, "name": "b"})]);
	assert_eq!(backend.find_by_id("3", &json!({}), &[]).await.unwrap().unwrap()["name"], "c");
	assert!(backend.find_by_id("99", &json!({}), &[]).await.unwrap().is_none());
	assert!(backend.find(&json!({"name": {"$regex": "x"}}), &json!({}), &json!({}), &json!({}), &[]).await.is_err());

	// re-watching is idempotent
	sqlite.watch("items").unwrap();
	assert!(sqlite.watch("missing").is_err());

	// full stack: a subscribed client gets an update when another connection writes
	let mut registry = Registry::default();
	registry.register("items", backend.clone());
	let (mut client, mut messages) = Client::new(Arc::new(registry), Arc::new(Anonymous));
	let subscribe = json!({"type": "subscribe", "target": "open", "scope": "count", "observe": "items", "config": {"subscriptionId": "s1", "query": {"status": "open"}}});
	client.consume(subscribe).await.unwrap();
	let mut next_count = async || loop {
		let message = timeout(WAIT, messages.recv()).await.expect("no message in time").unwrap();
		if message["type"] == "update" {
			return message["payload"]["open"].clone();
		}
	};
	assert_eq!(next_count().await, 2);
	external.execute("UPDATE items SET status = 'open' WHERE id = 3", []).unwrap();
	assert_eq!(next_count().await, 3);
}

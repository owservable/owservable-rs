//! Against a real server (set OWSERVABLE_POSTGRES_URL, e.g. `host=127.0.0.1 port=5432 user=postgres`):
//! writes from another connection arrive as change events, SQL queries agree with the stores'
//! matcher, the listener recovers from a dropped connection, and a Client gets live updates.

use std::sync::Arc;
use std::time::Duration;

use owservable_core::{Client, ConnectionManager, ObservableBackend, Registry, query};
use owservable_postgres::Postgres;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_postgres::NoTls;

const WAIT: Duration = Duration::from_secs(10);

async fn next_change(changes: &mut broadcast::Receiver<Value>) -> Value {
	timeout(WAIT, changes.recv()).await.expect("no change event in time").unwrap()
}

struct Anonymous;
impl ConnectionManager for Anonymous {}

#[tokio::test]
async fn live_updates_from_another_connection() {
	let Ok(url) = std::env::var("OWSERVABLE_POSTGRES_URL") else {
		eprintln!("skipped: OWSERVABLE_POSTGRES_URL not set");
		return;
	};
	let table = format!("items_{}", std::process::id());
	let (external, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
	tokio::spawn(connection);
	external
		.batch_execute(&format!(
			"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT NOT NULL, amount INTEGER, status TEXT, ref UUID)"
		))
		.await
		.unwrap();

	let postgres = Postgres::connect(&url).await.unwrap();
	let backend = postgres.watch(&table).await.unwrap();
	let mut changes = backend.changes();
	tokio::time::sleep(Duration::from_millis(200)).await; // listener's LISTEN in place

	// insert -> refetched fullDocument, text documentKey like v3
	external
		.execute(&format!("INSERT INTO {table} (name, amount, status) VALUES ('a', 1, 'open')"), &[])
		.await
		.unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "insert");
	assert_eq!(change["documentKey"], json!({"_id": "1"}));
	assert_eq!(change["fullDocument"], json!({"id": 1, "name": "a", "amount": 1, "status": "open", "ref": null}));

	// update -> only the changed columns
	external.execute(&format!("UPDATE {table} SET amount = 9 WHERE id = 1"), &[]).await.unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "update");
	assert_eq!(change["updateDescription"], json!({"updatedFields": {"amount": 9}, "removedFields": []}));
	assert_eq!(change["fullDocument"]["amount"], 9);

	// delete -> no refetch
	external.execute(&format!("DELETE FROM {table} WHERE id = 1"), &[]).await.unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "delete");
	assert_eq!(change["documentKey"], json!({"_id": "1"}));
	assert!(change.get("fullDocument").is_none());

	// queries agree with the matcher the stores use (including NULL handling and regex)
	external
		.batch_execute(&format!(
			"INSERT INTO {table} (name, amount, status, ref) VALUES ('b', 5, 'open', '0b6f4a8e-1c1a-4c55-9d3e-2f0d1a7c9b11'), ('c', 7, 'closed', NULL), ('d', NULL, NULL, NULL), ('e', 2, 'open', NULL), ('Fox', 7, NULL, NULL)"
		))
		.await
		.unwrap();
	for _ in 0..5 {
		next_change(&mut changes).await;
	}
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
		json!({"name": {"$regex": "^f", "$options": "i"}}),
		json!({"ref": "0b6f4a8e-1c1a-4c55-9d3e-2f0d1a7c9b11"}),
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
	// Postgres sorts NULLs first when descending
	assert_eq!(page, vec![json!({"id": 3, "name": "c"}), json!({"id": 6, "name": "Fox"})]);
	assert_eq!(backend.find_by_id("3", &json!({}), &[]).await.unwrap().unwrap()["name"], "c");
	assert!(backend.find_by_id("99", &json!({}), &[]).await.unwrap().is_none());

	// re-watching is idempotent; unknown tables fail
	postgres.watch(&table).await.unwrap();
	assert!(postgres.watch("missing_table").await.is_err());

	// a dropped listening connection reconnects and asks stores to reload ({}), then keeps delivering
	external
		.execute(
			"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()",
			&[],
		)
		.await
		.unwrap();
	assert_eq!(next_change(&mut changes).await, json!({}));
	external.execute(&format!("UPDATE {table} SET name = 'b2' WHERE id = 2"), &[]).await.unwrap();
	assert_eq!(next_change(&mut changes).await["updateDescription"]["updatedFields"], json!({"name": "b2"}));

	// full stack: a subscribed client gets an update when another connection writes
	let mut registry = Registry::default();
	registry.register("items", backend.clone());
	let (mut client, mut messages) = Client::new(Arc::new(registry), Arc::new(Anonymous));
	let subscribe = json!({"type": "subscribe", "target": "open", "scope": "count", "observe": "items", "config": {"subscriptionId": "s1", "query": {"status": "open"}}});
	client.consume(subscribe).await.unwrap();
	let mut next_count = async || {
		loop {
			let message = timeout(WAIT, messages.recv()).await.expect("no message in time").unwrap();
			if message["type"] == "update" {
				return message["payload"]["open"].clone();
			}
		}
	};
	assert_eq!(next_count().await, 2);
	external.execute(&format!("UPDATE {table} SET status = 'open' WHERE id = 3"), &[]).await.unwrap();
	assert_eq!(next_count().await, 3);

	external.batch_execute(&format!("DROP TABLE {table}")).await.unwrap();
}

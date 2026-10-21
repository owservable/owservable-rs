//! Against a real replica set (set OWSERVABLE_MONGODB_URL, e.g. `mongodb://127.0.0.1:27017/?directConnection=true`):
//! writes from another client arrive as change events shaped like v3's, queries agree with the
//! stores' matcher, and a Client subscribed through the full stack gets live updates.

use std::sync::Arc;
use std::time::Duration;

use mongodb::bson::{self, Document, doc, oid::ObjectId};
use owservable_core::{Client, ConnectionManager, ObservableBackend, Registry, query};
use owservable_mongodb::{Cast, MongoDb};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(10);

async fn next_change(changes: &mut broadcast::Receiver<Value>) -> Value {
	timeout(WAIT, changes.recv()).await.expect("no change event in time").unwrap()
}

struct Anonymous;
impl ConnectionManager for Anonymous {}

fn date(iso: &str) -> bson::DateTime {
	bson::DateTime::parse_rfc3339_str(iso).unwrap()
}

#[tokio::test]
async fn live_updates_from_another_client() {
	let Ok(url) = std::env::var("OWSERVABLE_MONGODB_URL") else {
		eprintln!("skipped: OWSERVABLE_MONGODB_URL not set");
		return;
	};
	let name = format!("items_{}", std::process::id());
	let external = mongodb::Client::with_uri_str(&url).await.unwrap().database("owservable_test").collection::<Document>(&name);
	external.drop().await.unwrap();

	let mongo = MongoDb::connect(&url, "owservable_test").await.unwrap();
	let backend = mongo.watch(&name, &[("dueAt", Cast::Date)]);
	let mut changes = backend.changes();
	tokio::time::sleep(Duration::from_millis(500)).await; // change stream open

	// insert -> fullDocument as mongoose JSON (hex ObjectId, ISO date)
	let id = ObjectId::new();
	external
		.insert_one(doc! {"_id": id, "name": "a", "amount": 1, "status": "open", "dueAt": date("2026-10-21T10:00:00Z")})
		.await
		.unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "insert");
	assert_eq!(change["documentKey"], json!({"_id": id.to_hex()}));
	assert_eq!(change["ns"]["coll"], name.as_str());
	assert_eq!(
		change["fullDocument"],
		json!({"_id": id.to_hex(), "name": "a", "amount": 1, "status": "open", "dueAt": "2026-10-21T10:00:00.000Z"})
	);
	assert!(change.get("updateDescription").is_none());

	// update -> native updateDescription
	external.update_one(doc! {"_id": id}, doc! {"$set": {"amount": 9}, "$unset": {"status": ""}}).await.unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "update");
	assert_eq!(change["updateDescription"]["updatedFields"], json!({"amount": 9}));
	assert_eq!(change["updateDescription"]["removedFields"], json!(["status"]));
	assert_eq!(change["fullDocument"]["amount"], 9);

	// delete -> no fullDocument
	external.delete_one(doc! {"_id": id}).await.unwrap();
	let change = next_change(&mut changes).await;
	assert_eq!(change["operationType"], "delete");
	assert_eq!(change["documentKey"], json!({"_id": id.to_hex()}));
	assert!(change.get("fullDocument").is_none());

	// queries agree with the matcher the stores use
	external
		.insert_many([
			doc! {"name": "b", "amount": 5, "status": "open", "dueAt": date("2026-10-20T00:00:00Z")},
			doc! {"name": "c", "amount": 7, "status": "closed", "dueAt": date("2026-10-22T00:00:00Z")},
			doc! {"name": "d"},
			doc! {"name": "e", "amount": 2, "status": "open", "tags": ["x", "y"]},
			doc! {"name": "Fox", "amount": 7, "status": null},
		])
		.await
		.unwrap();
	for _ in 0..5 {
		next_change(&mut changes).await;
	}
	let all = backend.find(&json!({}), &json!({}), &json!({}), &json!({"name": 1}), &[]).await.unwrap();
	assert_eq!(all.len(), 5);
	for query in [
		json!({"amount": {"$gte": 5}}),
		json!({"status": {"$ne": "open"}}),
		json!({"status": {"$in": ["closed", null]}}),
		json!({"amount": {"$exists": false}}),
		json!({"$or": [{"amount": 2}, {"name": "c"}]}),
		json!({"$nor": [{"status": "open"}, {"amount": 7}]}),
		json!({"name": {"$not": {"$in": ["b", "c"]}}}),
		json!({"name": {"$regex": "^f", "$options": "i"}}),
		json!({"tags": "x"}),
		json!({"dueAt": {"$gte": "2026-10-21T00:00:00.000Z"}}),
	] {
		let found = backend.find(&query, &json!({}), &json!({}), &json!({"name": 1}), &[]).await.unwrap();
		let expected: Vec<Value> = all.iter().filter(|doc| query::matches(&query, doc).unwrap()).cloned().collect();
		assert_eq!(found, expected, "{query}");
		assert_eq!(backend.count(&query).await.unwrap(), expected.len() as u64, "{query}");
	}

	// sort, paging, projection, id lookup (hex string cast to ObjectId)
	let page = backend
		.find(
			&json!({"amount": {"$exists": true}}),
			&json!({"name": 1}),
			&json!({"skip": 1, "limit": 2}),
			&json!({"amount": -1, "name": 1}),
			&[],
		)
		.await
		.unwrap();
	let names: Vec<&str> = page.iter().map(|doc| doc["name"].as_str().unwrap()).collect();
	assert_eq!(names, ["c", "b"]);
	assert_eq!(page[0].as_object().unwrap().len(), 2, "projection keeps _id + name");
	let b_id = all.iter().find(|doc| doc["name"] == "b").unwrap()["_id"].as_str().unwrap().to_string();
	assert_eq!(backend.find_by_id(&b_id, &json!({}), &[]).await.unwrap().unwrap()["name"], "b");
	assert_eq!(
		backend.find(&json!({"_id": {"$in": [b_id]}}), &json!({}), &json!({}), &json!({}), &[]).await.unwrap().len(),
		1
	);
	assert!(backend.find_by_id(&ObjectId::new().to_hex(), &json!({}), &[]).await.unwrap().is_none());

	// full stack: a subscribed client gets an update when another client writes
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
	external.update_one(doc! {"name": "c"}, doc! {"$set": {"status": "open"}}).await.unwrap();
	assert_eq!(next_count().await, 3);

	external.drop().await.unwrap();
}

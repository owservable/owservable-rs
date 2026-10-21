//! owservable mongodb backend: change streams.
//!
//! Each watched collection gets a change stream (`fullDocument: updateLookup`). On errors it
//! resumes from the last event after a backoff (1s doubling to 30s); if resuming is impossible
//! it starts a fresh stream and makes every store reload.
//!
//! ```no_run
//! # async fn example() -> Result<(), owservable_core::Error> {
//! use owservable_core::Registry;
//! use owservable_mongodb::{Cast, MongoDb};
//!
//! let db = MongoDb::connect("mongodb://localhost:27017/?replicaSet=rs0", "app").await?;
//! let mut registry = Registry::default();
//! registry.register("tasks", db.watch("tasks", &[("owner", Cast::ObjectId), ("dueAt", Cast::Date)]));
//! # Ok(()) }
//! ```
//!
//! Documents are sent as JSON the way mongoose's `toJSON` + `JSON.stringify` would: ObjectIds as
//! hex strings, dates as ISO strings. There is no schema, so query strings are cast only for `_id`
//! (ObjectId) and the fields named in `watch`; Extended JSON (`{"$oid": ...}`, `{"$date": ...}`)
//! works anywhere. Change streams need a replica set or sharded cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use mongodb::bson::{self, Bson, Document, oid::ObjectId};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::FullDocumentType;
use mongodb::{Client, Collection, Database};
use owservable_core::{Error, ObservableBackend};
use serde_json::{Map, Value, json};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const CHANGES_CAPACITY: usize = 1024;
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const LOGICAL_OPERATORS: [&str; 3] = ["$and", "$or", "$nor"];

/// How query strings for a field are turned into BSON (mongoose did this from the schema).
#[derive(Clone, Copy, Debug)]
pub enum Cast {
	ObjectId,
	/// RFC 3339 strings, e.g. `2026-10-21T10:00:00.000Z`.
	Date,
}

impl Cast {
	/// Extended JSON for `text`, if it parses as this type.
	fn wrap(self, text: &str) -> Option<Value> {
		match self {
			Cast::ObjectId => ObjectId::parse_str(text).ok().map(|_| json!({"$oid": text})),
			Cast::Date => bson::DateTime::parse_rfc3339_str(text).ok().map(|_| json!({"$date": text})),
		}
	}
}

pub struct MongoDb {
	database: Database,
}

impl MongoDb {
	pub async fn connect(uri: &str, database: &str) -> Result<Self, Error> {
		let client = Client::with_uri_str(uri).await?;
		Ok(MongoDb {
			database: client.database(database),
		})
	}

	/// The backend for `collection`, with its change stream running (stopped when the backend is dropped).
	pub fn watch(&self, collection: &str, casts: &[(&str, Cast)]) -> Arc<MongoBackend> {
		let collection: Collection<Document> = self.database.collection(collection);
		let changes = broadcast::channel(CHANGES_CAPACITY).0;
		let stream = tokio::spawn(stream_changes(collection.clone(), changes.clone()));
		let mut casts: HashMap<String, Cast> = casts.iter().map(|(field, cast)| (field.to_string(), *cast)).collect();
		casts.entry("_id".into()).or_insert(Cast::ObjectId);
		Arc::new(MongoBackend {
			collection,
			casts,
			changes,
			stream,
		})
	}
}

async fn stream_changes(collection: Collection<Document>, changes: broadcast::Sender<Value>) {
	let mut resume: Option<ResumeToken> = None;
	let mut delay = RECONNECT_INITIAL_DELAY;
	loop {
		let mut watch = collection.watch().full_document(FullDocumentType::UpdateLookup);
		if let Some(token) = &resume {
			watch = watch.resume_after(token.clone());
		}
		match watch.await {
			Ok(mut stream) => {
				delay = RECONNECT_INITIAL_DELAY;
				while let Some(Ok(event)) = stream.next().await {
					resume = stream.resume_token();
					if let Ok(event) = bson::to_document(&event) {
						let _ = changes.send(change_event(event));
					}
				}
			}
			Err(_) if resume.is_some() => {
				// the resume point is gone (e.g. oplog rolled over): start fresh and reload everything
				resume = None;
				let _ = changes.send(json!({}));
				continue;
			}
			Err(_) => {}
		}
		tokio::time::sleep(delay).await;
		delay = (delay * 2).min(RECONNECT_MAX_DELAY);
	}
}

fn change_event(mut event: Document) -> Value {
	let mut change = Map::new();
	for key in ["ns", "documentKey", "operationType", "updateDescription", "fullDocument"] {
		if let Some(value) = event.remove(key).filter(|value| *value != Bson::Null) {
			change.insert(key.to_string(), to_json(value));
		}
	}
	Value::Object(change)
}

/// BSON as mongoose `toJSON` + `JSON.stringify` would send it.
fn to_json(value: Bson) -> Value {
	match value {
		Bson::Document(document) => Value::Object(document.into_iter().map(|(key, value)| (key, to_json(value))).collect()),
		Bson::Array(values) => values.into_iter().map(to_json).collect(),
		Bson::ObjectId(id) => id.to_hex().into(),
		Bson::DateTime(date) => iso_string(date).into(),
		Bson::Decimal128(decimal) => decimal.to_string().into(),
		Bson::Int32(i) => i.into(),
		Bson::Int64(i) => i.into(),
		Bson::Null | Bson::Undefined => Value::Null,
		other => other.into_relaxed_extjson(),
	}
}

/// `Date.prototype.toISOString`: always milliseconds, always `Z`.
fn iso_string(date: bson::DateTime) -> String {
	let Ok(rfc) = date.try_to_rfc3339_string() else { return date.to_string() };
	let seconds = rfc.split(['.', 'Z']).next().unwrap_or(&rfc);
	format!("{seconds}.{:03}Z", date.timestamp_millis().rem_euclid(1000))
}

/// Rewrites strings of cast fields as Extended JSON, inside equality, operator objects, arrays and `$and/$or/$nor`.
fn cast_query(query: &Value, casts: &HashMap<String, Cast>) -> Value {
	match query {
		Value::Object(map) => Value::Object(
			map.iter()
				.map(|(key, value)| {
					let value = match casts.get(key) {
						_ if LOGICAL_OPERATORS.contains(&key.as_str()) => cast_query(value, casts),
						Some(cast) => cast_value(value, *cast),
						None => value.clone(),
					};
					(key.clone(), value)
				})
				.collect(),
		),
		Value::Array(entries) => entries.iter().map(|entry| cast_query(entry, casts)).collect(),
		other => other.clone(),
	}
}

fn cast_value(value: &Value, cast: Cast) -> Value {
	match value {
		Value::String(text) => cast.wrap(text).unwrap_or_else(|| value.clone()),
		Value::Array(entries) => entries.iter().map(|entry| cast_value(entry, cast)).collect(),
		Value::Object(ops) if ops.keys().all(|key| key.starts_with('$')) => Value::Object(ops.iter().map(|(op, arg)| (op.clone(), cast_value(arg, cast))).collect()),
		other => other.clone(),
	}
}

fn document(value: Value) -> Result<Document, Error> {
	match Bson::try_from(value)? {
		Bson::Document(document) => Ok(document),
		other => Err(format!("expected an object, got {other}").into()),
	}
}

fn optional_document(value: &Value) -> Result<Option<Document>, Error> {
	match value.as_object() {
		Some(map) if !map.is_empty() => Ok(Some(document(value.clone())?)),
		_ => Ok(None),
	}
}

/// One watched collection.
pub struct MongoBackend {
	collection: Collection<Document>,
	casts: HashMap<String, Cast>,
	changes: broadcast::Sender<Value>,
	stream: JoinHandle<()>,
}

impl Drop for MongoBackend {
	fn drop(&mut self) {
		self.stream.abort();
	}
}

impl MongoBackend {
	fn filter(&self, query: &Value) -> Result<Document, Error> {
		document(cast_query(query, &self.casts))
	}
}

#[async_trait]
impl ObservableBackend for MongoBackend {
	fn changes(&self) -> broadcast::Receiver<Value> {
		self.changes.subscribe()
	}

	async fn find(&self, query: &Value, fields: &Value, paging: &Value, sort: &Value, _populates: &[Value]) -> Result<Vec<Value>, Error> {
		let mut find = self.collection.find(self.filter(query)?).allow_disk_use(true);
		if let Some(projection) = optional_document(fields)? {
			find = find.projection(projection);
		}
		if let Some(sort) = optional_document(sort)? {
			find = find.sort(sort);
		}
		if let Some(skip) = paging.get("skip").and_then(Value::as_u64) {
			find = find.skip(skip);
		}
		if let Some(limit) = paging.get("limit").and_then(Value::as_i64) {
			find = find.limit(limit);
		}
		let documents: Vec<Document> = find.await?.collect::<Vec<_>>().await.into_iter().collect::<Result<_, _>>()?;
		Ok(documents.into_iter().map(|d| to_json(Bson::Document(d))).collect())
	}

	async fn find_one(&self, query: &Value, fields: &Value, _populates: &[Value]) -> Result<Option<Value>, Error> {
		let mut find = self.collection.find_one(self.filter(query)?);
		if let Some(projection) = optional_document(fields)? {
			find = find.projection(projection);
		}
		Ok(find.await?.map(|d| to_json(Bson::Document(d))))
	}

	async fn find_by_id(&self, id: &str, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error> {
		// _id strings that parse as ObjectIds are cast; anything else is looked up as-is
		self.find_one(&json!({"_id": id}), fields, populates).await
	}

	async fn count(&self, query: &Value) -> Result<u64, Error> {
		Ok(self.collection.count_documents(self.filter(query)?).await?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn casts_and_converts() {
		let casts: HashMap<String, Cast> = [("_id".into(), Cast::ObjectId), ("at".into(), Cast::Date)].into();
		let hex = "64b7f0c2a1b2c3d4e5f60718";
		let query = json!({"_id": {"$in": [hex, "plain"]}, "$or": [{"at": {"$gte": "2026-10-21T10:00:00.000Z"}}, {"name": hex}]});
		let filter = document(cast_query(&query, &casts)).unwrap();
		let ids = filter.get_document("_id").unwrap().get_array("$in").unwrap();
		assert_eq!(ids[0], Bson::ObjectId(ObjectId::parse_str(hex).unwrap()));
		assert_eq!(ids[1], Bson::String("plain".into()));
		let or = filter.get_array("$or").unwrap();
		assert!(matches!(or[0].as_document().unwrap().get_document("at").unwrap().get("$gte"), Some(Bson::DateTime(_))));
		assert_eq!(or[1].as_document().unwrap().get("name"), Some(&Bson::String(hex.into())), "uncast field stays a string");

		let date = bson::DateTime::parse_rfc3339_str("2026-10-21T10:00:00Z").unwrap();
		let doc = bson::doc! {"_id": ObjectId::parse_str(hex).unwrap(), "at": date, "n": 1_i64, "x": Bson::Null};
		assert_eq!(to_json(Bson::Document(doc)), json!({"_id": hex, "at": "2026-10-21T10:00:00.000Z", "n": 1, "x": null}));
		let precise = bson::DateTime::from_millis(1_761_040_800_123);
		assert_eq!(iso_string(precise), "2025-10-21T10:00:00.123Z");
	}
}

//! owservable postgres backend: LISTEN/NOTIFY live updates.
//!
//! A row-level trigger calls `pg_notify('owservable', {table, op, id, changed})` on every write;
//! a dedicated listening connection turns those into MongoDB-shaped change events, refetching
//! the row for inserts and updates. The listener reconnects with backoff (1s doubling to 30s)
//! and makes every store reload after reconnecting, since notifications may have been missed.
//!
//! ```no_run
//! # async fn example() -> Result<(), owservable_core::Error> {
//! use owservable_core::Registry;
//! use owservable_postgres::Postgres;
//!
//! let db = Postgres::connect("host=localhost user=app dbname=app").await?;
//! let mut registry = Registry::default();
//! registry.register("tasks", db.watch("tasks").await?);
//! # Ok(()) }
//! ```
//!
//! Documents are rows as JSON objects keyed by column name (via `row_to_json`); `_id` in queries
//! means the primary key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use owservable_core::sql::{POSTGRES, ident, literal, order_by, projection, where_clause};
use owservable_core::{Error, ObservableBackend};
use serde_json::{Map, Value, json};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{AsyncMessage, Client, NoTls, Notification};

const CHANNEL: &str = "owservable";
const CHANGES_CAPACITY: usize = 1024;
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
/// Serializes trigger installation across processes starting at the same time.
const INSTALL_LOCK_KEY: i64 = 776277001;

type Tables = Arc<Mutex<HashMap<String, Watched>>>;

struct Watched {
	pk: String,
	changes: broadcast::Sender<Value>,
}

/// A database connection for queries plus a listening connection (both stopped on drop).
// ponytail: no TLS and one query connection that is not re-established if it drops; add a pool/TLS connector when deployments need them.
pub struct Postgres {
	client: Arc<Client>,
	tables: Tables,
	tasks: Vec<JoinHandle<()>>,
}

impl Drop for Postgres {
	fn drop(&mut self) {
		self.tasks.iter().for_each(JoinHandle::abort);
	}
}

impl Postgres {
	/// Connects with a libpq-style config string (`host=... user=... dbname=...` or a `postgres://` URL).
	pub async fn connect(config: &str) -> Result<Self, Error> {
		let (client, connection) = tokio_postgres::connect(config, NoTls).await?;
		let driver = tokio::spawn(async move {
			let _ = connection.await;
		});
		let client = Arc::new(client);
		let tables = Tables::default();
		let listener = tokio::spawn(listen(config.to_string(), client.clone(), tables.clone()));
		Ok(Postgres {
			client,
			tables,
			tasks: vec![driver, listener],
		})
	}

	/// Installs (or replaces) the notify trigger on `table` and returns its backend. The table needs a primary key.
	pub async fn watch(&self, table: &str) -> Result<Arc<PostgresBackend>, Error> {
		let name = ident(table);
		let pk_rows = self
			.client
			.query(
				"SELECT a.attname::text FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = $1::text::regclass AND i.indisprimary",
				&[&name],
			)
			.await?;
		let pk: String = pk_rows.first().ok_or_else(|| format!("table \"{table}\" has no primary key"))?.get(0);
		let columns: Vec<String> = self
			.client
			.query(
				"SELECT attname::text FROM pg_attribute WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
				&[&name],
			)
			.await?
			.iter()
			.map(|row| row.get(0))
			.collect();

		self.client.batch_execute(&install_sql(table, &pk)).await?;

		let changes = {
			let mut tables = self.tables.lock().unwrap();
			let watched = tables.entry(table.to_string()).or_insert_with(|| Watched {
				pk: pk.clone(),
				changes: broadcast::channel(CHANGES_CAPACITY).0,
			});
			watched.changes.clone()
		};
		Ok(Arc::new(PostgresBackend {
			client: self.client.clone(),
			table: table.to_string(),
			pk,
			columns,
			changes,
		}))
	}
}

fn install_sql(table: &str, pk: &str) -> String {
	format!(
		"BEGIN;
SELECT pg_advisory_xact_lock({INSTALL_LOCK_KEY});
CREATE OR REPLACE FUNCTION owservable_notify() RETURNS trigger AS $owservable$
DECLARE
	pk_column text := TG_ARGV[0];
	pk_value text;
	changed text[];
BEGIN
	IF TG_OP = 'DELETE' THEN
		pk_value := to_jsonb(OLD) ->> pk_column;
	ELSE
		pk_value := to_jsonb(NEW) ->> pk_column;
	END IF;
	IF TG_OP = 'UPDATE' THEN
		SELECT array_agg(o.key) INTO changed
		FROM jsonb_each(to_jsonb(OLD)) o JOIN jsonb_each(to_jsonb(NEW)) n ON o.key = n.key
		WHERE o.value IS DISTINCT FROM n.value;
	END IF;
	PERFORM pg_notify({channel}, json_build_object('table', TG_TABLE_NAME, 'op', lower(TG_OP), 'id', pk_value, 'changed', changed)::text);
	RETURN NULL;
END;
$owservable$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER {trigger} AFTER INSERT OR UPDATE OR DELETE ON {table_ident} FOR EACH ROW EXECUTE FUNCTION owservable_notify({pk});
COMMIT;",
		channel = literal(CHANNEL),
		trigger = ident(&format!("{table}_owservable_notify")),
		table_ident = ident(table),
		pk = literal(pk),
	)
}

/// Keeps a LISTEN connection open, reconnecting with backoff, and fans notifications out to the watched tables.
async fn listen(config: String, client: Arc<Client>, tables: Tables) {
	let mut delay = RECONNECT_INITIAL_DELAY;
	let mut connected_before = false;
	loop {
		if let Ok((listener, mut connection)) = tokio_postgres::connect(&config, NoTls).await {
			let (notifications, mut received) = mpsc::unbounded_channel();
			let driver = tokio::spawn(async move {
				while let Some(Ok(message)) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
					if let AsyncMessage::Notification(notification) = message {
						let _ = notifications.send(notification);
					}
				}
			});
			if listener.batch_execute(&format!("LISTEN {}", ident(CHANNEL))).await.is_ok() {
				delay = RECONNECT_INITIAL_DELAY;
				if connected_before {
					// notifications may have been missed while disconnected
					broadcast_all(&tables, json!({}));
				}
				connected_before = true;
				while let Some(notification) = received.recv().await {
					forward(&client, &tables, &notification).await;
				}
			}
			driver.abort();
		}
		tokio::time::sleep(delay).await;
		delay = (delay * 2).min(RECONNECT_MAX_DELAY);
	}
}

fn broadcast_all(tables: &Tables, change: Value) {
	for watched in tables.lock().unwrap().values() {
		let _ = watched.changes.send(change.clone());
	}
}

async fn forward(client: &Client, tables: &Tables, notification: &Notification) {
	let Ok(payload) = serde_json::from_str::<Value>(notification.payload()) else { return };
	let table = payload["table"].as_str().unwrap_or_default();
	let Some((pk, changes)) = tables.lock().unwrap().get(table).map(|w| (w.pk.clone(), w.changes.clone())) else {
		return;
	};

	let op = payload["op"].as_str().unwrap_or_default();
	let id = payload["id"].clone();
	let mut change = json!({"ns": {"coll": table}, "documentKey": {"_id": id}, "operationType": op});
	if op != "delete" {
		let sql = format!("SELECT row_to_json(t) FROM (SELECT * FROM {} WHERE {} = $1) t", ident(table), ident(&pk));
		// ponytail: a failed refetch sends the change without fullDocument; stores then reload from scratch
		if let Ok(Some(row)) = client.query_opt(&sql, &[&Param(id)]).await {
			let document: Value = row.get(0);
			if op == "update" {
				let columns = payload["changed"].as_array().into_iter().flatten().filter_map(Value::as_str);
				let updated: Map<String, Value> = columns.map(|c| (c.to_string(), document.get(c).cloned().unwrap_or(Value::Bool(true)))).collect();
				change["updateDescription"] = json!({"updatedFields": updated, "removedFields": []});
			}
			change["fullDocument"] = document;
		}
	}
	let _ = changes.send(change);
}

/// A JSON scalar sent in text format, so Postgres parses it as whatever type the parameter has
/// (integer, uuid, timestamp, numeric, enum, ...).
#[derive(Debug)]
struct Param(Value);

impl ToSql for Param {
	fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
		match &self.0 {
			Value::Null => return Ok(IsNull::Yes),
			Value::String(s) => out.put_slice(s.as_bytes()),
			other => out.put_slice(other.to_string().as_bytes()),
		}
		Ok(IsNull::No)
	}

	fn accepts(_ty: &Type) -> bool {
		true
	}

	fn encode_format(&self, _ty: &Type) -> Format {
		Format::Text
	}

	to_sql_checked!();
}

/// One watched table.
pub struct PostgresBackend {
	client: Arc<Client>,
	table: String,
	pk: String,
	columns: Vec<String>,
	changes: broadcast::Sender<Value>,
}

impl PostgresBackend {
	async fn rows(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Value>, Error> {
		let params: Vec<Param> = params.into_iter().map(Param).collect();
		let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
		Ok(self.client.query(sql, &refs).await?.iter().map(|row| row.get(0)).collect())
	}
}

#[async_trait]
impl ObservableBackend for PostgresBackend {
	fn changes(&self) -> broadcast::Receiver<Value> {
		self.changes.subscribe()
	}

	async fn find(&self, query: &Value, fields: &Value, paging: &Value, sort: &Value, _populates: &[Value]) -> Result<Vec<Value>, Error> {
		let condition = where_clause(query, &self.pk, &POSTGRES, 0)?;
		let mut sql = format!(
			"SELECT {} FROM {} WHERE {}{}",
			projection(fields, &self.pk, &self.columns),
			ident(&self.table),
			condition.sql,
			order_by(sort, &self.pk)
		);
		if let Some(limit) = paging.get("limit").and_then(Value::as_i64) {
			sql += &format!(" LIMIT {limit}");
		}
		if let Some(skip) = paging.get("skip").and_then(Value::as_i64) {
			sql += &format!(" OFFSET {skip}");
		}
		self.rows(&format!("SELECT row_to_json(t) FROM ({sql}) t"), condition.params).await
	}

	async fn find_one(&self, query: &Value, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error> {
		Ok(self.find(query, fields, &json!({"limit": 1}), &json!({}), populates).await?.pop())
	}

	async fn find_by_id(&self, id: &str, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error> {
		self.find_one(&json!({"_id": id}), fields, populates).await
	}

	async fn count(&self, query: &Value) -> Result<u64, Error> {
		let condition = where_clause(query, &self.pk, &POSTGRES, 0)?;
		let sql = format!("SELECT json_build_array(COUNT(*)) FROM {} WHERE {}", ident(&self.table), condition.sql);
		let counts = self.rows(&sql, condition.params).await?;
		Ok(counts.first().and_then(|c| c[0].as_u64()).unwrap_or(0))
	}
}

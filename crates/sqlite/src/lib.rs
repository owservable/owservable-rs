//! owservable sqlite backend: journal-trigger live updates.
//!
//! `AFTER INSERT/UPDATE/DELETE` triggers write every change to a `_owservable_changes` journal
//! table, which a poller reads on a short interval. Triggers fire whichever connection or process
//! writes, so changes from other processes sharing the database file are seen too.
//!
//! ```no_run
//! # async fn example() -> Result<(), owservable_core::Error> {
//! use std::{sync::Arc, time::Duration};
//! use owservable_core::Registry;
//! use owservable_sqlite::Sqlite;
//!
//! let db = Sqlite::open("app.db", Duration::from_millis(250))?;
//! let mut registry = Registry::default();
//! registry.register("tasks", db.watch("tasks")?);
//! # Ok(()) }
//! ```
//!
//! Documents are rows as JSON objects keyed by column name; `_id` in queries means the primary key.
//! Local database files only, in WAL mode (set on open).

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use owservable_core::sql::{SQLITE, ident, order_by, where_clause};
use owservable_core::{Error, ObservableBackend};
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, params_from_iter};
use serde_json::{Map, Value, json};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const JOURNAL: &str = "_owservable_changes";
const PRUNE_EVERY_POLLS: u32 = 40;
const CHANGES_CAPACITY: usize = 1024;

// ponytail: one connection shared by queries and the poller; use a pool if queries contend.
type Shared = Arc<Mutex<Connection>>;
type Tables = Arc<Mutex<HashMap<String, Watched>>>;

struct Watched {
	pk: String,
	changes: broadcast::Sender<Value>,
}

/// An open database file and its journal poller (stopped on drop).
pub struct Sqlite {
	connection: Shared,
	tables: Tables,
	poller: JoinHandle<()>,
}

impl Drop for Sqlite {
	fn drop(&mut self) {
		self.poller.abort();
	}
}

impl Sqlite {
	/// Opens (or creates) the database in WAL mode, creates the journal and starts polling it.
	/// Call inside a tokio runtime.
	pub fn open(path: impl AsRef<Path>, poll_interval: Duration) -> Result<Self, Error> {
		let connection = Connection::open(path)?;
		connection.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
		connection.busy_timeout(Duration::from_secs(5))?;
		connection.execute_batch(&format!(
			"CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT NOT NULL, op TEXT NOT NULL, pk TEXT, changed TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
			ident(JOURNAL)
		))?;
		// start after what is already journaled; older entries describe changes nobody is waiting for
		let last_seen: i64 = connection.query_row(&format!("SELECT COALESCE(MAX(id), 0) FROM {}", ident(JOURNAL)), [], |row| row.get(0))?;

		let connection = Arc::new(Mutex::new(connection));
		let tables = Tables::default();
		let poller = tokio::spawn(poll(connection.clone(), tables.clone(), poll_interval, last_seen));
		Ok(Sqlite { connection, tables, poller })
	}

	/// Installs (or re-installs) the change triggers on `table` and returns its backend.
	/// The table needs a primary key.
	pub fn watch(&self, table: &str) -> Result<Arc<SqliteBackend>, Error> {
		let (pk, columns) = {
			let connection = self.connection.lock().unwrap();
			let (pk, columns) = table_info(&connection, table)?;
			connection.execute_batch(&triggers_sql(table, &pk, &columns))?;
			(pk, columns)
		};
		let changes = {
			let mut tables = self.tables.lock().unwrap();
			let watched = tables.entry(table.to_string()).or_insert_with(|| Watched {
				pk: pk.clone(),
				changes: broadcast::channel(CHANGES_CAPACITY).0,
			});
			watched.changes.clone()
		};
		Ok(Arc::new(SqliteBackend {
			connection: self.connection.clone(),
			table: table.to_string(),
			pk,
			columns,
			changes,
		}))
	}
}

/// (primary key column, all columns) of a table.
fn table_info(connection: &Connection, table: &str) -> Result<(String, Vec<String>), Error> {
	let mut statement = connection.prepare(&format!("PRAGMA table_info({})", ident(table)))?;
	let rows: Vec<(String, i64)> = statement.query_map([], |row| Ok((row.get("name")?, row.get("pk")?)))?.collect::<Result<_, _>>()?;
	if rows.is_empty() {
		return Err(format!("table \"{table}\" does not exist").into());
	}
	let pk = rows
		.iter()
		.find(|(_, pk)| *pk == 1)
		.map(|(name, _)| name.clone())
		.ok_or_else(|| format!("table \"{table}\" has no primary key"))?;
	Ok((pk, rows.into_iter().map(|(name, _)| name).collect()))
}

fn literal(text: &str) -> String {
	format!("'{}'", text.replace('\'', "''"))
}

fn triggers_sql(table: &str, pk: &str, columns: &[String]) -> String {
	let (t, j, name, pk) = (ident(table), ident(JOURNAL), literal(table), ident(pk));
	let trigger = |suffix: &str| ident(&format!("{table}_owservable_{suffix}"));
	let changed: Vec<String> = columns
		.iter()
		.map(|c| format!("CASE WHEN OLD.{0} IS NOT NEW.{0} THEN {1} ELSE '' END", ident(c), literal(&format!("{c},"))))
		.collect();
	let changed = format!("RTRIM({}, ',')", changed.join(" || "));
	let (ins, upd, del) = (trigger("ins"), trigger("upd"), trigger("del"));
	format!(
		"BEGIN;
DROP TRIGGER IF EXISTS {ins};
CREATE TRIGGER {ins} AFTER INSERT ON {t} BEGIN INSERT INTO {j} (table_name, op, pk, changed) VALUES ({name}, 'insert', NEW.{pk}, NULL); END;
DROP TRIGGER IF EXISTS {upd};
CREATE TRIGGER {upd} AFTER UPDATE ON {t} BEGIN INSERT INTO {j} (table_name, op, pk, changed) VALUES ({name}, 'update', NEW.{pk}, {changed}); END;
DROP TRIGGER IF EXISTS {del};
CREATE TRIGGER {del} AFTER DELETE ON {t} BEGIN INSERT INTO {j} (table_name, op, pk, changed) VALUES ({name}, 'delete', OLD.{pk}, NULL); END;
COMMIT;"
	)
}

async fn poll(connection: Shared, tables: Tables, interval: Duration, mut last_seen: i64) {
	let mut ticker = tokio::time::interval(interval);
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	let mut polls: u32 = 0;
	loop {
		ticker.tick().await;
		polls = polls.wrapping_add(1);
		let prune = polls.is_multiple_of(PRUNE_EVERY_POLLS);
		let watched: HashMap<String, String> = tables.lock().unwrap().iter().map(|(table, watched)| (table.clone(), watched.pk.clone())).collect();
		let shared = connection.clone();
		let result = tokio::task::spawn_blocking(move || read_journal(&shared.lock().unwrap(), &watched, last_seen, prune)).await;
		// ponytail: a failed poll is retried from the same position next tick, but not reported anywhere
		let Ok(Ok((events, seen))) = result else { continue };
		last_seen = seen;
		let tables = tables.lock().unwrap();
		for (table, change) in events {
			if let Some(watched) = tables.get(&table) {
				let _ = watched.changes.send(change);
			}
		}
	}
}

/// Journal rows after `last_seen` as MongoDB-shaped change events (inserts/updates refetch the row).
fn read_journal(connection: &Connection, watched: &HashMap<String, String>, last_seen: i64, prune: bool) -> rusqlite::Result<(Vec<(String, Value)>, i64)> {
	type Row = (i64, String, String, Option<String>, Option<String>);
	let sql = format!("SELECT id, table_name, op, pk, changed FROM {} WHERE id > ? ORDER BY id", ident(JOURNAL));
	let rows: Vec<Row> = connection
		.prepare_cached(&sql)?
		.query_map([last_seen], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?
		.collect::<Result<_, _>>()?;

	let mut seen = last_seen;
	let mut events = vec![];
	for (id, table, op, pk, changed) in rows {
		seen = id;
		let Some(pk_column) = watched.get(&table) else { continue };
		let pk = pk.unwrap_or_default();
		let mut change = json!({"ns": {"coll": table}, "documentKey": {"_id": pk}, "operationType": op});
		if op != "delete" {
			let sql = format!("SELECT * FROM {} WHERE {} = ?", ident(&table), ident(pk_column));
			if let Some(document) = select(connection, &sql, &[json!(pk)])?.pop() {
				if op == "update" {
					let columns = changed.as_deref().unwrap_or("").split(',').filter(|c| !c.is_empty());
					let updated: Map<String, Value> = columns.map(|c| (c.to_string(), document.get(c).cloned().unwrap_or(Value::Bool(true)))).collect();
					change["updateDescription"] = json!({"updatedFields": updated, "removedFields": []});
				}
				change["fullDocument"] = document;
			}
		}
		events.push((table, change));
	}
	// ponytail: pruning assumes one poller per file; several owservable processes on one file need a shared cursor instead
	if prune {
		connection.execute(&format!("DELETE FROM {} WHERE id <= ?", ident(JOURNAL)), [seen])?;
	}
	Ok((events, seen))
}

fn select(connection: &Connection, sql: &str, params: &[Value]) -> rusqlite::Result<Vec<Value>> {
	let mut statement = connection.prepare_cached(sql)?;
	let names: Vec<String> = statement.column_names().into_iter().map(String::from).collect();
	let rows = statement.query_map(params_from_iter(params.iter().map(to_sql)), |row| {
		let mut document = Map::new();
		for (i, name) in names.iter().enumerate() {
			document.insert(name.clone(), from_sql(row.get_ref(i)?));
		}
		Ok(Value::Object(document))
	})?;
	rows.collect()
}

fn to_sql(value: &Value) -> SqlValue {
	match value {
		Value::Null => SqlValue::Null,
		Value::Bool(b) => SqlValue::Integer(*b as i64),
		Value::Number(n) => n.as_i64().map_or_else(|| SqlValue::Real(n.as_f64().unwrap_or(f64::NAN)), SqlValue::Integer),
		Value::String(s) => SqlValue::Text(s.clone()),
		other => SqlValue::Text(other.to_string()),
	}
}

fn from_sql(value: ValueRef) -> Value {
	match value {
		ValueRef::Null => Value::Null,
		ValueRef::Integer(i) => i.into(),
		ValueRef::Real(f) => json!(f),
		ValueRef::Text(text) => String::from_utf8_lossy(text).into(),
		ValueRef::Blob(bytes) => bytes.to_vec().into(),
	}
}

/// One watched table.
pub struct SqliteBackend {
	connection: Shared,
	table: String,
	pk: String,
	columns: Vec<String>,
	changes: broadcast::Sender<Value>,
}

impl SqliteBackend {
	async fn run<T: Send + 'static>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static) -> Result<T, Error> {
		let connection = self.connection.clone();
		Ok(tokio::task::spawn_blocking(move || f(&connection.lock().unwrap())).await??)
	}

	fn select_sql(&self, query: &Value, fields: &Value, paging: &Value, sort: &Value) -> Result<(String, Vec<Value>), Error> {
		let condition = where_clause(query, &self.pk, &SQLITE, 0)?;
		let mut sql = format!(
			"SELECT {} FROM {} WHERE {}{}",
			self.projection(fields),
			ident(&self.table),
			condition.sql,
			order_by(sort, &self.pk)
		);
		let limit = paging.get("limit").and_then(Value::as_i64);
		let skip = paging.get("skip").and_then(Value::as_i64).unwrap_or(0);
		if limit.is_some() || skip > 0 {
			sql += &format!(" LIMIT {} OFFSET {skip}", limit.unwrap_or(-1));
		}
		Ok((sql, condition.params))
	}

	/// Included columns (`{name: 1}`) plus the primary key; everything when none are included.
	fn projection(&self, fields: &Value) -> String {
		let included = |column: &String| {
			fields
				.get(column)
				.is_some_and(|v| !matches!(v, Value::Null | Value::Bool(false)) && v.as_f64() != Some(0.0))
		};
		if !self.columns.iter().any(included) {
			return "*".into();
		}
		let columns: Vec<String> = self.columns.iter().filter(|c| **c == self.pk || included(c)).map(|c| ident(c)).collect();
		columns.join(", ")
	}
}

#[async_trait]
impl ObservableBackend for SqliteBackend {
	fn changes(&self) -> broadcast::Receiver<Value> {
		self.changes.subscribe()
	}

	async fn find(&self, query: &Value, fields: &Value, paging: &Value, sort: &Value, _populates: &[Value]) -> Result<Vec<Value>, Error> {
		let (sql, params) = self.select_sql(query, fields, paging, sort)?;
		self.run(move |connection| select(connection, &sql, &params)).await
	}

	async fn find_one(&self, query: &Value, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error> {
		Ok(self.find(query, fields, &json!({"limit": 1}), &json!({}), populates).await?.pop())
	}

	async fn find_by_id(&self, id: &str, fields: &Value, populates: &[Value]) -> Result<Option<Value>, Error> {
		// the id arrives as text; SQLite applies the column's numeric affinity when comparing
		self.find_one(&json!({"_id": id}), fields, populates).await
	}

	async fn count(&self, query: &Value) -> Result<u64, Error> {
		let condition = where_clause(query, &self.pk, &SQLITE, 0)?;
		let sql = format!("SELECT COUNT(*) FROM {} WHERE {}", ident(&self.table), condition.sql);
		let count: i64 = self
			.run(move |connection| connection.query_row(&sql, params_from_iter(condition.params.iter().map(to_sql)), |row| row.get(0)))
			.await?;
		Ok(count as u64)
	}
}

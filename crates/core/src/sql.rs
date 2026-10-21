//! Mongo-style query -> SQL `WHERE` / `ORDER BY`, shared by the SQL backends.
//!
//! Values are always bound as parameters; identifiers are always quoted. Predicates follow Mongo
//! semantics rather than SQL three-valued logic (`$ne`/`$nin`/`$not`/`$nor` match NULL columns),
//! so SQL results agree with [`crate::query::matches`] used by the stores.

use serde_json::Value;

pub struct Dialect {
	pub placeholder: fn(usize) -> String,
	pub ilike: &'static str,
	/// (case-sensitive, case-insensitive) regex operators, if the database has them.
	pub regex: Option<(&'static str, &'static str)>,
}

pub const SQLITE: Dialect = Dialect {
	placeholder: |_| "?".into(),
	ilike: "LIKE",
	regex: None,
};

pub const POSTGRES: Dialect = Dialect {
	placeholder: |i| format!("${i}"),
	ilike: "ILIKE",
	regex: Some(("~", "~*")),
};

/// A translated condition: `sql` refers to `params` through the dialect's placeholders.
#[derive(Debug, PartialEq)]
pub struct Where {
	pub sql: String,
	pub params: Vec<Value>,
}

/// Translates `query`; `_id` refers to the `pk` column. `first_param` is the number of params bound before these (postgres numbering).
pub fn where_clause(query: &Value, pk: &str, dialect: &Dialect, first_param: usize) -> Result<Where, String> {
	let mut builder = Builder {
		dialect,
		pk,
		params: vec![],
		offset: first_param,
	};
	let sql = builder.query(query)?;
	Ok(Where { sql, params: builder.params })
}

/// ` ORDER BY ...` (with a leading space) or an empty string. -1/"desc"/"descending" sort descending; dotted paths are ignored.
pub fn order_by(sort: &Value, pk: &str) -> String {
	let Some(sort) = sort.as_object() else { return String::new() };
	let terms: Vec<String> = sort
		.iter()
		.filter(|(key, _)| !key.contains('.'))
		.map(|(key, direction)| {
			let direction = match direction {
				Value::String(s) => s.to_lowercase(),
				other => other.to_string(),
			};
			let descending = ["-1", "desc", "descending"].contains(&direction.as_str());
			format!("{} {}", ident(column(key, pk)), if descending { "DESC" } else { "ASC" })
		})
		.collect();
	if terms.is_empty() { String::new() } else { format!(" ORDER BY {}", terms.join(", ")) }
}

/// Double-quoted SQL identifier.
pub fn ident(name: &str) -> String {
	format!("\"{}\"", name.replace('"', "\"\""))
}

fn column<'a>(field: &'a str, pk: &'a str) -> &'a str {
	if field == "_id" { pk } else { field }
}

fn not(predicate: &str) -> String {
	format!("NOT COALESCE(({predicate}), FALSE)")
}

fn join(parts: Vec<String>, operator: &str, empty: &str) -> String {
	match parts.len() {
		0 => empty.to_string(),
		1 => parts.into_iter().next().unwrap(),
		_ => format!("({})", parts.join(operator)),
	}
}

struct Builder<'a> {
	dialect: &'a Dialect,
	pk: &'a str,
	params: Vec<Value>,
	offset: usize,
}

impl Builder<'_> {
	fn bind(&mut self, value: &Value) -> Result<String, String> {
		if value.is_object() || value.is_array() {
			return Err(format!("expected a scalar value, got {value}"));
		}
		self.params.push(value.clone());
		Ok((self.dialect.placeholder)(self.offset + self.params.len()))
	}

	fn query(&mut self, query: &Value) -> Result<String, String> {
		let Value::Object(query) = query else {
			return Err(format!("query must be an object, got {query}"));
		};
		let mut parts = vec![];
		for (key, condition) in query {
			parts.push(match key.as_str() {
				"$and" => join(self.branches(condition)?, " AND ", "TRUE"),
				"$or" => join(self.branches(condition)?, " OR ", "FALSE"),
				"$nor" => not(&join(self.branches(condition)?, " OR ", "FALSE")),
				op if op.starts_with('$') => return Err(format!("unsupported operator {op}")),
				field if field.contains('.') => return Err(format!("nested field paths are not supported: {field}")),
				field => {
					let column = ident(column(field, self.pk));
					self.condition(&column, condition)?
				}
			});
		}
		Ok(join(parts, " AND ", "TRUE"))
	}

	fn branches(&mut self, condition: &Value) -> Result<Vec<String>, String> {
		let branches = condition.as_array().ok_or_else(|| format!("logical operator needs an array, got {condition}"))?;
		branches.iter().map(|branch| self.query(branch)).collect()
	}

	fn condition(&mut self, column: &str, condition: &Value) -> Result<String, String> {
		let Value::Object(ops) = condition else {
			return self.equals(column, condition);
		};
		if !ops.keys().next().is_some_and(|key| key.starts_with('$')) {
			return Err(format!("object equality is not supported: {condition}"));
		}
		let mut parts = vec![];
		for (op, arg) in ops {
			parts.push(match op.as_str() {
				"$eq" => self.equals(column, arg)?,
				"$ne" => not(&self.equals(column, arg)?),
				"$gt" => format!("{column} > {}", self.bind(arg)?),
				"$gte" => format!("{column} >= {}", self.bind(arg)?),
				"$lt" => format!("{column} < {}", self.bind(arg)?),
				"$lte" => format!("{column} <= {}", self.bind(arg)?),
				"$in" => self.one_of(column, arg)?,
				"$nin" => not(&self.one_of(column, arg)?),
				"$exists" => match arg {
					Value::Bool(true) => format!("{column} IS NOT NULL"),
					Value::Bool(false) => format!("{column} IS NULL"),
					Value::Number(n) if n.as_i64() == Some(1) => format!("{column} IS NOT NULL"),
					Value::Number(n) if n.as_i64() == Some(0) => format!("{column} IS NULL"),
					_ => return Err(format!("$exists needs a boolean, got {arg}")),
				},
				"$not" => match arg {
					Value::Object(inner) if inner.keys().all(|key| key.starts_with('$')) && !inner.is_empty() => not(&self.condition(column, arg)?),
					_ => return Err(format!("$not needs an operator object, got {arg}")),
				},
				"$like" => format!("{column} LIKE {}", self.bind(arg)?),
				"$ilike" => format!("{column} {} {}", self.dialect.ilike, self.bind(arg)?),
				"$regex" => self.regex(column, arg, ops.get("$options"))?,
				"$options" if ops.contains_key("$regex") => continue,
				_ => return Err(format!("unsupported operator {op}")),
			});
		}
		Ok(join(parts, " AND ", "TRUE"))
	}

	fn equals(&mut self, column: &str, value: &Value) -> Result<String, String> {
		match value {
			Value::Null => Ok(format!("{column} IS NULL")),
			Value::Array(_) => Err("array equality is not supported, use $in".into()),
			_ => Ok(format!("{column} = {}", self.bind(value)?)),
		}
	}

	fn one_of(&mut self, column: &str, list: &Value) -> Result<String, String> {
		let list = list.as_array().ok_or_else(|| format!("$in/$nin need an array, got {list}"))?;
		let mut placeholders = vec![];
		for value in list.iter().filter(|value| !value.is_null()) {
			placeholders.push(self.bind(value)?);
		}
		let mut parts = vec![];
		if !placeholders.is_empty() {
			parts.push(format!("{column} IN ({})", placeholders.join(", ")));
		}
		if list.iter().any(Value::is_null) {
			parts.push(format!("{column} IS NULL"));
		}
		Ok(join(parts, " OR ", "FALSE"))
	}

	fn regex(&mut self, column: &str, pattern: &Value, options: Option<&Value>) -> Result<String, String> {
		let Some((sensitive, insensitive)) = self.dialect.regex else {
			return Err("regular expressions are not supported by this backend".into());
		};
		let options = options.map_or(Ok(""), |options| options.as_str().ok_or("$options must be a string"))?;
		if let Some(unsupported) = options.chars().find(|c| *c != 'i') {
			return Err(format!("unsupported regex option '{unsupported}'"));
		}
		if !pattern.is_string() {
			return Err(format!("$regex needs a string, got {pattern}"));
		}
		let operator = if options.contains('i') { insensitive } else { sensitive };
		Ok(format!("{column} {operator} {}", self.bind(pattern)?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde_json::json;

	fn sqlite(query: Value) -> Result<(String, Vec<Value>), String> {
		where_clause(&query, "id", &SQLITE, 0).map(|w| (w.sql, w.params))
	}

	#[test]
	fn translates_queries() {
		assert_eq!(sqlite(json!({})).unwrap().0, "TRUE");
		assert_eq!(
			sqlite(json!({"_id": 3, "name": null})).unwrap(),
			("(\"id\" = ? AND \"name\" IS NULL)".into(), vec![json!(3)])
		);
		assert_eq!(
			sqlite(json!({"n": {"$gte": 1, "$ne": 5}, "s": {"$in": ["a", null]}})).unwrap().0,
			"((\"n\" >= ? AND NOT COALESCE((\"n\" = ?), FALSE)) AND (\"s\" IN (?) OR \"s\" IS NULL))"
		);
		assert_eq!(
			sqlite(json!({"$or": [{"a": 1}, {"b": {"$exists": false}}], "$nor": []})).unwrap().0,
			"((\"a\" = ? OR \"b\" IS NULL) AND NOT COALESCE((FALSE), FALSE))"
		);
		assert_eq!(sqlite(json!({"a\"b": {"$nin": []}})).unwrap().0, "NOT COALESCE((FALSE), FALSE)");

		let postgres = where_clause(&json!({"a": 1, "b": {"$regex": "^x", "$options": "i"}}), "id", &POSTGRES, 2).unwrap();
		assert_eq!(postgres.sql, "(\"a\" = $3 AND \"b\" ~* $4)");

		for bad in [
			json!({"a.b": 1}),
			json!({"$where": "1"}),
			json!({"a": {"$size": 1}}),
			json!({"a": [1]}),
			json!({"a": {"b": 1}}),
			json!({"a": {"$regex": "x"}}),
			json!({"a": {"$in": [[1]]}}),
		] {
			assert!(sqlite(bad.clone()).is_err(), "{bad}");
		}
	}

	#[test]
	fn orders() {
		assert_eq!(
			order_by(&json!({"_id": 1, "n": -1, "m": "DESC", "x.y": 1}), "id"),
			" ORDER BY \"id\" ASC, \"n\" DESC, \"m\" DESC"
		);
		assert_eq!(order_by(&json!({}), "id"), "");
	}
}

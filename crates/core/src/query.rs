//! Mongo-style query helpers: document matching (the subset of sift that owservable queries use)
//! and which root fields a query or a change touches.

use std::cmp::Ordering;
use std::collections::HashSet;

use regex::Regex;
use serde_json::Value;

const LOGICAL_OPERATORS: [&str; 3] = ["$and", "$or", "$nor"];

fn root_of(path: &str) -> &str {
	path.split('.').next().unwrap_or(path)
}

/// Root fields listed in a change's `updateDescription` (updated and removed).
pub fn changed_fields(change: &Value) -> HashSet<String> {
	let Some(description) = change.get("updateDescription") else {
		return HashSet::new();
	};
	let removed = description.get("removedFields").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str);
	let updated = description
		.get("updatedFields")
		.and_then(Value::as_object)
		.into_iter()
		.flat_map(|fields| fields.keys().map(String::as_str));
	removed.chain(updated).map(|field| root_of(field).to_string()).collect()
}

/// Root fields a query filters on, including `$field` references inside operators like `$expr`.
pub fn collect_query_fields(query: &Value) -> HashSet<String> {
	let mut fields = HashSet::new();
	collect(query, &mut fields);
	fields
}

fn collect(query: &Value, fields: &mut HashSet<String>) {
	match query {
		Value::Array(entries) => entries.iter().for_each(|entry| collect(entry, fields)),
		Value::Object(map) => {
			for (key, value) in map {
				if LOGICAL_OPERATORS.contains(&key.as_str()) {
					collect(value, fields);
				} else if key.starts_with('$') {
					collect_references(value, fields);
				} else {
					fields.insert(root_of(key).to_string());
				}
			}
		}
		_ => {}
	}
}

fn collect_references(value: &Value, fields: &mut HashSet<String>) {
	match value {
		Value::String(s) if s.len() > 1 && s.starts_with('$') && !s.starts_with("$$") => {
			fields.insert(root_of(&s[1..]).to_string());
		}
		Value::Array(entries) => entries.iter().for_each(|entry| collect_references(entry, fields)),
		Value::Object(map) => map.values().for_each(|entry| collect_references(entry, fields)),
		_ => {}
	}
}

/// Does `doc` match the Mongo-style `query`? Errors on operators outside the supported set
/// (`$and $or $nor $eq $ne $gt $gte $lt $lte $in $nin $exists $not $regex/$options`).
pub fn matches(query: &Value, doc: &Value) -> Result<bool, String> {
	let Value::Object(query) = query else {
		return Err(format!("query must be an object, got {query}"));
	};
	for (key, condition) in query {
		let ok = match key.as_str() {
			"$and" => all_of(condition, doc)?,
			"$or" => any_of(condition, doc)?,
			"$nor" => !any_of(condition, doc)?,
			op if op.starts_with('$') => return Err(format!("unsupported operator {op}")),
			path => field_matches(get_path(doc, path), condition)?,
		};
		if !ok {
			return Ok(false);
		}
	}
	Ok(true)
}

fn subqueries(condition: &Value) -> Result<&Vec<Value>, String> {
	condition.as_array().ok_or_else(|| format!("logical operator needs an array, got {condition}"))
}

fn all_of(condition: &Value, doc: &Value) -> Result<bool, String> {
	for query in subqueries(condition)? {
		if !matches(query, doc)? {
			return Ok(false);
		}
	}
	Ok(true)
}

fn any_of(condition: &Value, doc: &Value) -> Result<bool, String> {
	for query in subqueries(condition)? {
		if matches(query, doc)? {
			return Ok(true);
		}
	}
	Ok(false)
}

// ponytail: no implicit traversal of arrays of subdocuments ("items.name"); numeric segments index arrays.
fn get_path<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
	path.split('.').try_fold(doc, |value, key| match value {
		Value::Array(entries) => key.parse::<usize>().ok().and_then(|i| entries.get(i)),
		_ => value.get(key),
	})
}

fn field_matches(field: Option<&Value>, condition: &Value) -> Result<bool, String> {
	let Value::Object(ops) = condition else {
		return Ok(equals(field, condition));
	};
	if !ops.keys().next().is_some_and(|key| key.starts_with('$')) {
		return Ok(equals(field, condition));
	}
	for (op, arg) in ops {
		let ok = match op.as_str() {
			"$eq" => equals(field, arg),
			"$ne" => !equals(field, arg),
			"$gt" => compares(field, arg, |o| o == Ordering::Greater),
			"$gte" => compares(field, arg, |o| o != Ordering::Less),
			"$lt" => compares(field, arg, |o| o == Ordering::Less),
			"$lte" => compares(field, arg, |o| o != Ordering::Greater),
			"$in" => subqueries(arg)?.iter().any(|value| equals(field, value)),
			"$nin" => !subqueries(arg)?.iter().any(|value| equals(field, value)),
			// null counts as missing, matching the SQL backends (a column always "exists"); Mongo itself would say it exists
			"$exists" => field.is_some_and(|value| !value.is_null()) == truthy(arg),
			"$not" => !field_matches(field, arg)?,
			"$regex" => regex_matches(field, arg, ops.get("$options"))?,
			"$options" => true,
			_ => return Err(format!("unsupported operator {op}")),
		};
		if !ok {
			return Ok(false);
		}
	}
	Ok(true)
}

/// Mongo equality: a missing field equals null, an array field matches if it contains the value.
fn equals(field: Option<&Value>, value: &Value) -> bool {
	match field {
		None => value.is_null(),
		Some(field) => field == value || field.as_array().is_some_and(|entries| entries.contains(value)),
	}
}

fn candidates(field: Option<&Value>) -> Vec<&Value> {
	match field {
		None => vec![],
		Some(Value::Array(entries)) => entries.iter().collect(),
		Some(value) => vec![value],
	}
}

fn compares(field: Option<&Value>, arg: &Value, accept: impl Fn(Ordering) -> bool) -> bool {
	candidates(field).into_iter().any(|value| compare(value, arg).is_some_and(&accept))
}

/// Order of two JSON values of the same kind (numbers or strings); other pairs are incomparable.
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
	match (a, b) {
		(Value::Number(a), Value::Number(b)) => a.as_f64()?.partial_cmp(&b.as_f64()?),
		(Value::String(a), Value::String(b)) => Some(a.cmp(b)),
		_ => None,
	}
}

fn truthy(value: &Value) -> bool {
	match value {
		Value::Null => false,
		Value::Bool(b) => *b,
		Value::Number(n) => n.as_f64() != Some(0.0),
		Value::String(s) => !s.is_empty(),
		_ => true,
	}
}

// ponytail: regex compiled per evaluation; cache by pattern if it shows up in profiles.
fn regex_matches(field: Option<&Value>, pattern: &Value, options: Option<&Value>) -> Result<bool, String> {
	let pattern = pattern.as_str().ok_or_else(|| format!("$regex needs a string, got {pattern}"))?;
	let flags: String = options.and_then(Value::as_str).unwrap_or("").chars().filter(|c| "imsx".contains(*c)).collect();
	let source = if flags.is_empty() { pattern.to_string() } else { format!("(?{flags}){pattern}") };
	let regex = Regex::new(&source).map_err(|e| e.to_string())?;
	Ok(candidates(field).into_iter().filter_map(Value::as_str).any(|s| regex.is_match(s)))
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde_json::json;

	#[test]
	fn matches_supported_operators() {
		let doc = json!({"name": "Alpha", "n": 3, "tags": ["x", "y"], "nested": {"ok": true}, "nothing": null});
		let yes = [
			json!({}),
			json!({"name": "Alpha", "n": 3}),
			json!({"tags": "x"}),
			json!({"nested.ok": true}),
			json!({"missing": null}),
			json!({"n": {"$gt": 2, "$lte": 3}}),
			json!({"n": {"$in": [1, 3]}, "name": {"$nin": ["Bravo"]}}),
			json!({"name": {"$ne": "Bravo"}, "missing": {"$exists": false}, "nothing": {"$exists": false}, "n": {"$exists": true}}),
			json!({"name": {"$regex": "^al", "$options": "i"}}),
			json!({"n": {"$not": {"$gt": 5}}}),
			json!({"$or": [{"n": 1}, {"n": 3}], "$and": [{"name": "Alpha"}], "$nor": [{"n": 9}]}),
			json!({"tags.1": "y"}),
		];
		for query in yes {
			assert_eq!(matches(&query, &doc), Ok(true), "{query}");
		}
		let no = [
			json!({"name": "Bravo"}),
			json!({"n": {"$lt": 3}}),
			json!({"tags": {"$in": ["z"]}}),
			json!({"name": {"$regex": "^al"}}),
			json!({"n": "3"}),
		];
		for query in no {
			assert_eq!(matches(&query, &doc), Ok(false), "{query}");
		}
		assert!(matches(&json!({"$where": "x"}), &doc).is_err());
		assert!(matches(&json!({"n": {"$size": 1}}), &doc).is_err());
	}

	#[test]
	fn collects_fields() {
		let query = json!({"a.b": 1, "$or": [{"c": 1}, {"d": {"$gt": 1}}], "$expr": {"$gt": ["$e.f", "$$NOW"]}});
		let mut fields: Vec<_> = collect_query_fields(&query).into_iter().collect();
		fields.sort();
		assert_eq!(fields, ["a", "c", "d", "e"]);

		let change = json!({"updateDescription": {"updatedFields": {"x.y": 1, "z": 2}, "removedFields": ["w.v"]}});
		let mut changed: Vec<_> = changed_fields(&change).into_iter().collect();
		changed.sort();
		assert_eq!(changed, ["w", "x", "z"]);
	}
}

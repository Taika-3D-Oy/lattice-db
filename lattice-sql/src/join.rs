//! JOIN execution — hash join and left join over in-memory JSON rows.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Hash join: for each left row, look up matching right rows by key.
/// Produces combined JSON objects (left fields merged with right fields).
pub fn hash_join(
    left: &[JsonValue],
    right: &[JsonValue],
    left_col: &str,
    right_col: &str,
) -> Vec<JsonValue> {
    // Build hash table from right rows.
    let mut index: HashMap<String, Vec<&JsonValue>> = HashMap::new();
    for row in right {
        if let Some(key) = extract_string(row, right_col) {
            index.entry(key).or_default().push(row);
        }
    }

    let mut result = Vec::new();
    for left_row in left {
        if let Some(key) = extract_string(left_row, left_col) {
            if let Some(matches) = index.get(&key) {
                for right_row in matches {
                    result.push(merge_rows(left_row, right_row));
                }
            }
        }
    }
    result
}

/// Left join: like hash join, but includes left rows that have no match
/// on the right side (right columns are null).
pub fn left_join(
    left: &[JsonValue],
    right: &[JsonValue],
    left_col: &str,
    right_col: &str,
) -> Vec<JsonValue> {
    let mut index: HashMap<String, Vec<&JsonValue>> = HashMap::new();
    for row in right {
        if let Some(key) = extract_string(row, right_col) {
            index.entry(key).or_default().push(row);
        }
    }

    let mut result = Vec::new();
    for left_row in left {
        let key = extract_string(left_row, left_col);
        let matches = key.as_ref().and_then(|k| index.get(k));

        match matches {
            Some(right_rows) => {
                for right_row in right_rows {
                    result.push(merge_rows(left_row, right_row));
                }
            }
            None => {
                // No match — emit left row with nulls for right columns.
                result.push(left_row.clone());
            }
        }
    }
    result
}

/// Merge two JSON objects into one. Right fields overwrite left on collision.
fn merge_rows(left: &JsonValue, right: &JsonValue) -> JsonValue {
    let mut merged = match left {
        JsonValue::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    if let JsonValue::Object(right_map) = right {
        for (k, v) in right_map {
            // Prefix right-side column if it collides with left.
            if merged.contains_key(k) {
                // Use the right-side value — in practice the user disambiguates
                // with table.column in the projection. For the merged row, we
                // keep both via a simple suffix strategy.
                let alt_key = format!("{k}_1");
                if !merged.contains_key(&alt_key) {
                    merged.insert(alt_key, v.clone());
                }
            } else {
                merged.insert(k.clone(), v.clone());
            }
        }
    }
    JsonValue::Object(merged)
}

/// Extract a field value as a string for join key comparison.
fn extract_string(row: &JsonValue, field: &str) -> Option<String> {
    let val = row.get(field)?;
    match val {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

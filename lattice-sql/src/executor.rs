//! Query executor — takes a plan and executes it against lattice-db
//! via the client SDK, returning result sets.

use lattice_db_client::{
    AggOp as ClientAggOp, Filter, LatticeDb, ScanQuery,
};
use serde_json::Value as JsonValue;

use crate::catalog::{Catalog, ColumnType};
use crate::join;
use crate::planner::*;

// ── Result types ───────────────────────────────────────────────────

/// A SQL result set — columns + rows of JSON values.
#[derive(Debug, serde::Serialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<JsonValue>>,
    pub affected_rows: u64,
}

impl ResultSet {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
        }
    }

    pub fn with_affected(n: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: n,
        }
    }
}

// ── Executor ───────────────────────────────────────────────────────

/// Execute a plan against lattice-db.
pub async fn execute(
    plan: Plan,
    db: &LatticeDb,
    catalog: &mut Catalog,
) -> Result<ResultSet, String> {
    match plan {
        Plan::TableScan(p) => exec_table_scan(p, db).await,
        Plan::Aggregate(p) => exec_aggregate(p, db).await,
        Plan::Join(p) => exec_join(p, db).await,
        Plan::Insert(p) => exec_insert(p, db, catalog).await,
        Plan::Update(p) => exec_update(p, db).await,
        Plan::Delete(p) => exec_delete(p, db).await,
        Plan::CreateTable(p) => exec_create_table(p, db, catalog).await,
        Plan::DropTable(p) => exec_drop_table(p, db, catalog).await,
        Plan::CreateIndex(p) => exec_create_index(p, db).await,
        Plan::DropIndex(p) => exec_drop_index(p, db).await,
    }
}

// ── SELECT (table scan) ───────────────────────────────────────────

async fn exec_table_scan(plan: TableScanPlan, db: &LatticeDb) -> Result<ResultSet, String> {
    let mut query = ScanQuery::new();
    for f in &plan.filters {
        query = query.filter(&f.field, &f.op, &f.value);
    }
    if let Some((ref field, asc)) = plan.order_by {
        query = query.order_by(field, if asc { "asc" } else { "desc" });
    }
    if let Some(limit) = plan.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = plan.offset {
        query = query.offset(offset);
    }

    let result = db
        .scan(&plan.table, query)
        .await
        .map_err(|e| format!("scan failed: {e}"))?;

    let mut rows = Vec::new();
    for row in &result.rows {
        let obj: JsonValue =
            serde_json::from_slice(&row.value).map_err(|e| format!("json decode: {e}"))?;
        let projected: Vec<JsonValue> = plan
            .columns
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(JsonValue::Null))
            .collect();
        rows.push(projected);
    }

    Ok(ResultSet {
        columns: plan.columns,
        rows,
        affected_rows: 0,
    })
}

// ── AGGREGATE ──────────────────────────────────────────────────────

async fn exec_aggregate(plan: AggregatePlan, db: &LatticeDb) -> Result<ResultSet, String> {
    let filters: Vec<Filter> = plan
        .filters
        .iter()
        .map(|f| Filter {
            field: f.field.clone(),
            op: f.op.clone(),
            value: f.value.clone(),
        })
        .collect();

    let client_ops: Vec<ClientAggOp> = plan
        .ops
        .iter()
        .map(|op| match op.fn_name.as_str() {
            "count" => ClientAggOp::count(),
            "sum" => ClientAggOp::sum(op.field.as_deref().unwrap_or("")),
            "avg" => ClientAggOp::avg(op.field.as_deref().unwrap_or("")),
            "min" => ClientAggOp::min(op.field.as_deref().unwrap_or("")),
            "max" => ClientAggOp::max(op.field.as_deref().unwrap_or("")),
            _ => ClientAggOp::count(),
        })
        .collect();

    let groups = db
        .aggregate(
            &plan.table,
            &filters,
            plan.group_by.as_deref(),
            &client_ops,
        )
        .await
        .map_err(|e| format!("aggregate failed: {e}"))?;

    // Build column names.
    let mut columns = Vec::new();
    if let Some(ref gb) = plan.group_by {
        columns.push(gb.clone());
    }
    for op in &plan.ops {
        let name = match &op.field {
            Some(f) => format!("{}({})", op.fn_name.to_uppercase(), f),
            None => format!("{}(*)", op.fn_name.to_uppercase()),
        };
        columns.push(name);
    }

    // Build rows.
    let mut rows = Vec::new();
    for group in &groups {
        let mut row = Vec::new();
        if plan.group_by.is_some() {
            row.push(
                group
                    .key
                    .as_ref()
                    .map(|k| JsonValue::String(k.clone()))
                    .unwrap_or(JsonValue::Null),
            );
        }
        for res in &group.results {
            row.push(res.value.clone());
        }
        rows.push(row);
    }

    Ok(ResultSet {
        columns,
        rows,
        affected_rows: 0,
    })
}

// ── JOIN ───────────────────────────────────────────────────────────

async fn exec_join(plan: JoinPlan, db: &LatticeDb) -> Result<ResultSet, String> {
    // Load left table rows.
    let left_rows = load_table_rows(&plan.left.table, &plan.left.filters, db).await?;

    // Execute join pipeline.
    let mut combined = left_rows;

    for step in &plan.joins {
        let right_rows = load_table_rows(&step.table, &[], db).await?;

        combined = match step.join_type {
            JoinStepType::Inner => {
                join::hash_join(&combined, &right_rows, &step.left_col, &step.right_col)
            }
            JoinStepType::Left => {
                join::left_join(&combined, &right_rows, &step.left_col, &step.right_col)
            }
        };
    }

    // Project output columns.
    let mut columns = Vec::new();
    let mut rows = Vec::new();

    // If projection is just "*", include all fields.
    let project_all = plan.projection.len() == 1 && plan.projection[0].1 == "*";

    if project_all {
        // Collect all unique column names from the first combined row.
        if let Some(first) = combined.first() {
            if let JsonValue::Object(map) = first {
                columns = map.keys().cloned().collect();
            }
        }
    } else {
        columns = plan
            .projection
            .iter()
            .map(|(_, name)| name.clone())
            .collect();
    }

    for row_val in &combined {
        let projected: Vec<JsonValue> = columns
            .iter()
            .map(|col| row_val.get(col).cloned().unwrap_or(JsonValue::Null))
            .collect();
        rows.push(projected);
    }

    // Apply ORDER BY.
    if let Some((ref field, asc)) = plan.order_by {
        let col_idx = columns.iter().position(|c| c == field);
        if let Some(idx) = col_idx {
            rows.sort_by(|a, b| {
                let va = &a[idx];
                let vb = &b[idx];
                let cmp = cmp_json_values(va, vb);
                if asc { cmp } else { cmp.reverse() }
            });
        }
    }

    // Apply OFFSET + LIMIT.
    if let Some(offset) = plan.offset {
        rows = rows.into_iter().skip(offset as usize).collect();
    }
    if let Some(limit) = plan.limit {
        rows.truncate(limit as usize);
    }

    Ok(ResultSet {
        columns,
        rows,
        affected_rows: 0,
    })
}

/// Load all rows from a table as JSON values.
async fn load_table_rows(
    table: &str,
    filters: &[PlanFilter],
    db: &LatticeDb,
) -> Result<Vec<JsonValue>, String> {
    let mut query = ScanQuery::new();
    for f in filters {
        query = query.filter(&f.field, &f.op, &f.value);
    }
    let result = db
        .scan(table, query)
        .await
        .map_err(|e| format!("scan {table}: {e}"))?;

    let mut rows = Vec::new();
    for row in &result.rows {
        let obj: JsonValue =
            serde_json::from_slice(&row.value).map_err(|e| format!("json decode: {e}"))?;
        rows.push(obj);
    }
    Ok(rows)
}

// ── INSERT ─────────────────────────────────────────────────────────

async fn exec_insert(
    plan: InsertPlan,
    db: &LatticeDb,
    catalog: &Catalog,
) -> Result<ResultSet, String> {
    let table_def = catalog
        .get(&plan.table)
        .ok_or_else(|| format!("table {} not found", plan.table))?;

    let mut inserted = 0u64;

    for row_pairs in &plan.rows {
        // Build JSON object from column-value pairs.
        let mut obj = serde_json::Map::new();
        let mut pk_value = None;

        for (col, val) in row_pairs {
            // Convert to appropriate JSON type based on catalog.
            let json_val = if let Some(col_def) = table_def.column(col) {
                typed_json_value(val, &col_def.col_type)
            } else {
                JsonValue::String(val.clone())
            };

            if col == &plan.pk_column {
                pk_value = Some(val.clone());
            }
            obj.insert(col.clone(), json_val);
        }

        let pk = pk_value.ok_or_else(|| {
            format!("INSERT missing primary key column '{}'", plan.pk_column)
        })?;

        // Validate NOT NULL constraints.
        for col_def in &table_def.columns {
            if col_def.not_null && !col_def.primary_key {
                if !obj.contains_key(&col_def.name) {
                    if let Some(ref default) = col_def.default {
                        obj.insert(col_def.name.clone(), default.clone());
                    } else {
                        return Err(format!(
                            "NOT NULL violation: column '{}' is required",
                            col_def.name
                        ));
                    }
                }
            }
        }

        // Validate FOREIGN KEY constraints.
        for col_def in &table_def.columns {
            if let Some(ref fk) = col_def.foreign_key {
                if let Some(val) = obj.get(&col_def.name) {
                    if let Some(fk_val) = val.as_str() {
                        let exists = db
                            .exists(&fk.table, fk_val)
                            .await
                            .map_err(|e| format!("FK check failed: {e}"))?;
                        if !exists {
                            return Err(format!(
                                "FK violation: {}={} references {}.{} which does not exist",
                                col_def.name, fk_val, fk.table, fk.column
                            ));
                        }
                    }
                }
            }
        }

        // Use `create` (insert-if-not-exists semantics).
        let bytes =
            serde_json::to_vec(&JsonValue::Object(obj)).map_err(|e| format!("json: {e}"))?;
        db.create(&plan.table, &pk, &bytes)
            .await
            .map_err(|e| format!("insert failed: {e}"))?;
        inserted += 1;
    }

    Ok(ResultSet::with_affected(inserted))
}

// ── UPDATE ─────────────────────────────────────────────────────────

async fn exec_update(plan: UpdatePlan, db: &LatticeDb) -> Result<ResultSet, String> {
    // Single-key fast path.
    if let Some(ref pk_val) = plan.pk_filter {
        let row = db
            .get(&plan.table, pk_val)
            .await
            .map_err(|e| format!("get: {e}"))?;
        let mut obj: JsonValue =
            serde_json::from_slice(&row.value).map_err(|e| format!("json: {e}"))?;
        apply_assignments(&mut obj, &plan.assignments);
        let bytes = serde_json::to_vec(&obj).map_err(|e| format!("json: {e}"))?;
        db.cas(&plan.table, pk_val, &bytes, row.revision)
            .await
            .map_err(|e| format!("cas: {e}"))?;
        return Ok(ResultSet::with_affected(1));
    }

    // Multi-row: scan + update each.
    let mut query = ScanQuery::new();
    for f in &plan.filters {
        query = query.filter(&f.field, &f.op, &f.value);
    }
    let result = db
        .scan(&plan.table, query)
        .await
        .map_err(|e| format!("scan: {e}"))?;

    let mut updated = 0u64;
    for row in &result.rows {
        let mut obj: JsonValue =
            serde_json::from_slice(&row.value).map_err(|e| format!("json: {e}"))?;
        apply_assignments(&mut obj, &plan.assignments);
        let bytes = serde_json::to_vec(&obj).map_err(|e| format!("json: {e}"))?;
        db.cas(&plan.table, &row.key, &bytes, row.revision)
            .await
            .map_err(|e| format!("cas: {e}"))?;
        updated += 1;
    }

    Ok(ResultSet::with_affected(updated))
}

fn apply_assignments(obj: &mut JsonValue, assignments: &[(String, String)]) {
    if let JsonValue::Object(ref mut map) = obj {
        for (col, val) in assignments {
            // Try to parse as number/bool first, fall back to string.
            let json_val = if val == "null" {
                JsonValue::Null
            } else if let Ok(n) = val.parse::<i64>() {
                JsonValue::Number(n.into())
            } else if let Ok(n) = val.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map(JsonValue::Number)
                    .unwrap_or_else(|| JsonValue::String(val.clone()))
            } else if val == "true" || val == "false" {
                JsonValue::Bool(val == "true")
            } else {
                JsonValue::String(val.clone())
            };
            map.insert(col.clone(), json_val);
        }
    }
}

// ── DELETE ──────────────────────────────────────────────────────────

async fn exec_delete(plan: DeletePlan, db: &LatticeDb) -> Result<ResultSet, String> {
    // Single-key fast path.
    if let Some(ref pk_val) = plan.pk_filter {
        db.delete(&plan.table, pk_val)
            .await
            .map_err(|e| format!("delete: {e}"))?;
        return Ok(ResultSet::with_affected(1));
    }

    // Multi-row: scan + delete each.
    let mut query = ScanQuery::new();
    for f in &plan.filters {
        query = query.filter(&f.field, &f.op, &f.value);
    }
    let result = db
        .scan(&plan.table, query)
        .await
        .map_err(|e| format!("scan: {e}"))?;

    let mut deleted = 0u64;
    for row in &result.rows {
        db.delete(&plan.table, &row.key)
            .await
            .map_err(|e| format!("delete: {e}"))?;
        deleted += 1;
    }

    Ok(ResultSet::with_affected(deleted))
}

// ── DDL ────────────────────────────────────────────────────────────

async fn exec_create_table(
    plan: CreateTablePlan,
    db: &LatticeDb,
    catalog: &mut Catalog,
) -> Result<ResultSet, String> {
    if plan.if_not_exists && catalog.has_table(&plan.table_def.name) {
        return Ok(ResultSet::empty());
    }

    // Also set up a lattice-db schema for basic type validation.
    let mut schema_fields = serde_json::Map::new();
    for col in &plan.table_def.columns {
        let mut field_def = serde_json::Map::new();
        let type_str = match col.col_type {
            ColumnType::Text => "string",
            ColumnType::Integer | ColumnType::Real => "number",
            ColumnType::Boolean => "boolean",
        };
        field_def.insert("type".into(), JsonValue::String(type_str.into()));
        if col.not_null {
            field_def.insert("required".into(), JsonValue::Bool(true));
        }
        schema_fields.insert(col.name.clone(), JsonValue::Object(field_def));
    }
    let schema = serde_json::json!({ "fields": schema_fields });

    // Set schema on the underlying lattice-db table.
    db.set_schema(&plan.table_def.name, &schema)
        .await
        .map_err(|e| format!("set schema: {e}"))?;

    // Create indexes for UNIQUE columns.
    for col in &plan.table_def.columns {
        if col.unique && !col.primary_key {
            db.create_index(&plan.table_def.name, &col.name)
                .await
                .map_err(|e| format!("create unique index: {e}"))?;
        }
    }

    // Persist table definition in catalog.
    catalog.create_table(plan.table_def, db).await?;

    Ok(ResultSet::empty())
}

async fn exec_drop_table(
    plan: DropTablePlan,
    db: &LatticeDb,
    catalog: &mut Catalog,
) -> Result<ResultSet, String> {
    if plan.if_exists && !catalog.has_table(&plan.table) {
        return Ok(ResultSet::empty());
    }
    catalog.drop_table(&plan.table, db).await?;
    Ok(ResultSet::empty())
}

async fn exec_create_index(plan: CreateIndexPlan, db: &LatticeDb) -> Result<ResultSet, String> {
    if plan.columns.len() == 1 {
        db.create_index(&plan.table, &plan.columns[0])
            .await
            .map_err(|e| format!("create index: {e}"))?;
    } else {
        let cols: Vec<&str> = plan.columns.iter().map(|s| s.as_str()).collect();
        db.create_compound_index(&plan.table, &cols)
            .await
            .map_err(|e| format!("create compound index: {e}"))?;
    }
    Ok(ResultSet::empty())
}

async fn exec_drop_index(plan: DropIndexPlan, db: &LatticeDb) -> Result<ResultSet, String> {
    db.drop_index(&plan.table, &plan.name)
        .await
        .map_err(|e| format!("drop index: {e}"))?;
    Ok(ResultSet::empty())
}

// ── Helpers ────────────────────────────────────────────────────────

/// Convert a string value to a typed JSON value based on column type.
fn typed_json_value(val: &str, col_type: &ColumnType) -> JsonValue {
    match col_type {
        ColumnType::Integer => val
            .parse::<i64>()
            .map(|n| JsonValue::Number(n.into()))
            .unwrap_or_else(|_| JsonValue::String(val.into())),
        ColumnType::Real => val
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(val.into())),
        ColumnType::Boolean => match val {
            "true" | "1" => JsonValue::Bool(true),
            "false" | "0" => JsonValue::Bool(false),
            _ => JsonValue::String(val.into()),
        },
        ColumnType::Text => JsonValue::String(val.into()),
    }
}

/// Compare two JSON values for ordering.
fn cmp_json_values(a: &JsonValue, b: &JsonValue) -> std::cmp::Ordering {
    match (a, b) {
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            let fa = a.as_f64().unwrap_or(0.0);
            let fb = b.as_f64().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
        }
        (JsonValue::String(a), JsonValue::String(b)) => a.cmp(b),
        (JsonValue::Bool(a), JsonValue::Bool(b)) => a.cmp(b),
        (JsonValue::Null, JsonValue::Null) => std::cmp::Ordering::Equal,
        (JsonValue::Null, _) => std::cmp::Ordering::Less,
        (_, JsonValue::Null) => std::cmp::Ordering::Greater,
        _ => {
            let sa = a.to_string();
            let sb = b.to_string();
            sa.cmp(&sb)
        }
    }
}

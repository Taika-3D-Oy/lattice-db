//! Query planner — converts parsed SQL statements into execution plans
//! that map to lattice-db client operations.

use crate::catalog::{Catalog, TableDef};
use crate::parser::*;

// ── Execution plan types ───────────────────────────────────────────

/// A fully resolved execution plan ready for the executor.
#[derive(Debug)]
pub enum Plan {
    /// Scan a single table with optional filters, ordering, limit.
    TableScan(TableScanPlan),
    /// Aggregate query.
    Aggregate(AggregatePlan),
    /// Join two or more tables.
    Join(JoinPlan),
    /// Insert one or more rows.
    Insert(InsertPlan),
    /// Update rows matching a condition.
    Update(UpdatePlan),
    /// Delete rows matching a condition.
    Delete(DeletePlan),
    /// DDL: create table.
    CreateTable(CreateTablePlan),
    /// DDL: drop table.
    DropTable(DropTablePlan),
    /// DDL: create index.
    CreateIndex(CreateIndexPlan),
    /// DDL: drop index.
    DropIndex(DropIndexPlan),
}

#[derive(Debug)]
pub struct TableScanPlan {
    pub table: String,
    pub columns: Vec<String>,
    pub filters: Vec<PlanFilter>,
    pub order_by: Option<(String, bool)>, // (field, asc)
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct PlanFilter {
    pub field: String,
    pub op: String,
    pub value: String,
}

#[derive(Debug)]
pub struct AggregatePlan {
    pub table: String,
    pub filters: Vec<PlanFilter>,
    pub group_by: Option<String>,
    pub ops: Vec<AggOp>,
    /// Non-aggregate columns selected alongside aggregates (for GROUP BY).
    pub extra_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AggOp {
    pub fn_name: String,
    pub field: Option<String>,
}

#[derive(Debug)]
pub struct JoinPlan {
    /// The left (driving) table scan.
    pub left: TableScanPlan,
    /// Joined tables in order.
    pub joins: Vec<JoinStep>,
    /// Final projection columns: (table_alias, column_name).
    pub projection: Vec<(Option<String>, String)>,
    /// Final ORDER BY, if any.
    pub order_by: Option<(String, bool)>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug)]
pub struct JoinStep {
    pub table: String,
    pub alias: Option<String>,
    pub join_type: JoinStepType,
    pub left_col: String,
    pub right_col: String,
}

#[derive(Debug)]
pub enum JoinStepType {
    Inner,
    Left,
}

#[derive(Debug)]
pub struct InsertPlan {
    pub table: String,
    pub pk_column: String,
    /// Each row: Vec of (column_name, value_string).
    pub rows: Vec<Vec<(String, String)>>,
}

#[derive(Debug)]
pub struct UpdatePlan {
    pub table: String,
    pub assignments: Vec<(String, String)>,
    pub filters: Vec<PlanFilter>,
    /// True if the WHERE clause targets the PK only → single-key update.
    pub pk_filter: Option<String>,
}

#[derive(Debug)]
pub struct DeletePlan {
    pub table: String,
    pub filters: Vec<PlanFilter>,
    /// True if the WHERE clause targets the PK only → single-key delete.
    pub pk_filter: Option<String>,
}

#[derive(Debug)]
pub struct CreateTablePlan {
    pub table_def: crate::catalog::TableDef,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct DropTablePlan {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug)]
pub struct CreateIndexPlan {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug)]
pub struct DropIndexPlan {
    pub table: String,
    pub name: String,
}

// ── Planner ────────────────────────────────────────────────────────

/// Plan a parsed SQL statement against the catalog.
pub fn plan(stmt: SqlStatement, catalog: &Catalog) -> Result<Plan, String> {
    match stmt {
        SqlStatement::Select(sel) => plan_select(sel, catalog),
        SqlStatement::Insert(ins) => plan_insert(ins, catalog),
        SqlStatement::Update(upd) => plan_update(upd, catalog),
        SqlStatement::Delete(del) => plan_delete(del, catalog),
        SqlStatement::CreateTable(ct) => Ok(Plan::CreateTable(CreateTablePlan {
            table_def: ct.table_def,
            if_not_exists: ct.if_not_exists,
        })),
        SqlStatement::DropTable(dt) => Ok(Plan::DropTable(DropTablePlan {
            table: dt.table,
            if_exists: dt.if_exists,
        })),
        SqlStatement::CreateIndex(ci) => {
            ensure_table_exists(&ci.table, catalog)?;
            Ok(Plan::CreateIndex(CreateIndexPlan {
                table: ci.table,
                columns: ci.columns,
            }))
        }
        SqlStatement::DropIndex(di) => Ok(Plan::DropIndex(DropIndexPlan {
            table: di.table,
            name: di.name,
        })),
    }
}

// ── SELECT planning ────────────────────────────────────────────────

fn plan_select(sel: SelectStmt, catalog: &Catalog) -> Result<Plan, String> {
    let table = &sel.from.table;
    ensure_table_exists(table, catalog)?;

    let has_joins = !sel.joins.is_empty();
    let has_aggregates = sel
        .columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Aggregate { .. }));

    if has_joins {
        return plan_join_select(sel, catalog);
    }

    if has_aggregates {
        return plan_aggregate_select(sel, catalog);
    }

    // Simple table scan.
    let table_def = catalog.get(table).unwrap();
    let columns = resolve_columns(&sel.columns, table_def)?;
    let filters = where_to_filters(&sel.where_clause)?;

    let order_by = sel.order_by.first().map(|o| (o.field.clone(), o.asc));

    Ok(Plan::TableScan(TableScanPlan {
        table: table.clone(),
        columns,
        filters,
        order_by,
        limit: sel.limit,
        offset: sel.offset,
    }))
}

fn plan_aggregate_select(sel: SelectStmt, _catalog: &Catalog) -> Result<Plan, String> {
    let table = &sel.from.table;
    let filters = where_to_filters(&sel.where_clause)?;
    let group_by = sel.group_by.first().cloned();

    let mut ops = Vec::new();
    let mut extra_columns = Vec::new();

    for col in &sel.columns {
        match col {
            SelectColumn::Aggregate { fn_name, field } => {
                ops.push(AggOp {
                    fn_name: fn_name.to_lowercase(),
                    field: field.clone(),
                });
            }
            SelectColumn::Column { name, .. } => {
                extra_columns.push(name.clone());
            }
            _ => {}
        }
    }

    Ok(Plan::Aggregate(AggregatePlan {
        table: table.clone(),
        filters,
        group_by,
        ops,
        extra_columns,
    }))
}

fn plan_join_select(sel: SelectStmt, catalog: &Catalog) -> Result<Plan, String> {
    let base_table = &sel.from.table;
    let base_alias = sel.from.alias.as_deref().unwrap_or(base_table);

    // Verify all joined tables exist.
    for j in &sel.joins {
        ensure_table_exists(&j.table, catalog)?;
    }

    let base_def = catalog.get(base_table).unwrap();

    // Build left scan (just load all columns for now; projection happens after join).
    let left = TableScanPlan {
        table: base_table.clone(),
        columns: base_def
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect(),
        filters: where_to_filters_for_table(&sel.where_clause, Some(base_alias))?,
        order_by: None,
        limit: None,
        offset: None,
    };

    // Build join steps.
    let mut joins = Vec::new();
    for j in &sel.joins {
        let step_type = match j.join_type {
            JoinType::Inner => JoinStepType::Inner,
            JoinType::Left => JoinStepType::Left,
        };
        joins.push(JoinStep {
            table: j.table.clone(),
            alias: j.alias.clone(),
            join_type: step_type,
            left_col: j.on.left_col.clone(),
            right_col: j.on.right_col.clone(),
        });
    }

    // Resolve final projection.
    let projection = sel
        .columns
        .iter()
        .map(|c| match c {
            SelectColumn::Column { table, name } => Ok((table.clone(), name.clone())),
            SelectColumn::Star { .. } => Ok((None, "*".to_string())),
            _ => Err("aggregates in JOIN queries are not yet supported".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let order_by = sel.order_by.first().map(|o| (o.field.clone(), o.asc));

    Ok(Plan::Join(JoinPlan {
        left,
        joins,
        projection,
        order_by,
        limit: sel.limit,
        offset: sel.offset,
    }))
}

// ── INSERT planning ────────────────────────────────────────────────

fn plan_insert(ins: InsertStmt, catalog: &Catalog) -> Result<Plan, String> {
    let table_def = ensure_table_exists(&ins.table, catalog)?;
    let pk = table_def
        .primary_key()
        .ok_or_else(|| format!("table {} has no primary key", ins.table))?;
    let pk_name = pk.name.clone();

    let mut planned_rows = Vec::new();
    for row_values in &ins.rows {
        if row_values.len() != ins.columns.len() {
            return Err("column count does not match value count".into());
        }
        let pairs: Vec<(String, String)> = ins
            .columns
            .iter()
            .zip(row_values.iter())
            .map(|(c, v)| (c.clone(), v.clone()))
            .collect();
        planned_rows.push(pairs);
    }

    Ok(Plan::Insert(InsertPlan {
        table: ins.table,
        pk_column: pk_name,
        rows: planned_rows,
    }))
}

// ── UPDATE planning ────────────────────────────────────────────────

fn plan_update(upd: UpdateStmt, catalog: &Catalog) -> Result<Plan, String> {
    let table_def = ensure_table_exists(&upd.table, catalog)?;
    let pk_name = table_def.primary_key().map(|c| c.name.clone());
    let filters = where_to_filters(&upd.where_clause)?;

    // Detect single-key update: WHERE pk = 'value'
    let pk_filter = pk_name.as_ref().and_then(|pk| {
        if filters.len() == 1 && filters[0].field == *pk && filters[0].op == "eq" {
            Some(filters[0].value.clone())
        } else {
            None
        }
    });

    Ok(Plan::Update(UpdatePlan {
        table: upd.table,
        assignments: upd.assignments,
        filters,
        pk_filter,
    }))
}

// ── DELETE planning ────────────────────────────────────────────────

fn plan_delete(del: DeleteStmt, catalog: &Catalog) -> Result<Plan, String> {
    let table_def = ensure_table_exists(&del.table, catalog)?;
    let pk_name = table_def.primary_key().map(|c| c.name.clone());
    let filters = where_to_filters(&del.where_clause)?;

    // Detect single-key delete.
    let pk_filter = pk_name.as_ref().and_then(|pk| {
        if filters.len() == 1 && filters[0].field == *pk && filters[0].op == "eq" {
            Some(filters[0].value.clone())
        } else {
            None
        }
    });

    Ok(Plan::Delete(DeletePlan {
        table: del.table,
        filters,
        pk_filter,
    }))
}

// ── Helpers ────────────────────────────────────────────────────────

fn ensure_table_exists<'a>(table: &str, catalog: &'a Catalog) -> Result<&'a TableDef, String> {
    catalog
        .get(table)
        .ok_or_else(|| format!("table {table} does not exist"))
}

fn resolve_columns(cols: &[SelectColumn], table_def: &TableDef) -> Result<Vec<String>, String> {
    let mut result = Vec::new();
    for col in cols {
        match col {
            SelectColumn::Column { name, .. } => result.push(name.clone()),
            SelectColumn::Star { .. } => {
                result.extend(table_def.column_names().iter().map(|s| s.to_string()));
            }
            SelectColumn::Aggregate { .. } => {
                return Err("unexpected aggregate in non-aggregate query".into());
            }
        }
    }
    Ok(result)
}

/// Flatten a WHERE expression tree into a list of AND-ed filters.
/// Only supports simple AND-ed comparisons for now. OR/NOT fall back
/// to post-filtering in the executor.
fn where_to_filters(expr: &Option<WhereExpr>) -> Result<Vec<PlanFilter>, String> {
    match expr {
        None => Ok(Vec::new()),
        Some(e) => collect_and_filters(e),
    }
}

fn where_to_filters_for_table(
    expr: &Option<WhereExpr>,
    _table_alias: Option<&str>,
) -> Result<Vec<PlanFilter>, String> {
    let all = where_to_filters(expr)?;
    // If a table alias is specified, only include filters for that table.
    // Filters without a table qualifier are included for all tables.
    Ok(all) // TODO: implement per-table filter scoping for JOIN queries
}

fn collect_and_filters(expr: &WhereExpr) -> Result<Vec<PlanFilter>, String> {
    match expr {
        WhereExpr::Comparison {
            field, op, value, ..
        } => {
            let op_str = match op {
                CmpOp::Eq => "eq",
                CmpOp::Neq => "neq",
                CmpOp::Lt => "lt",
                CmpOp::Lte => "lte",
                CmpOp::Gt => "gt",
                CmpOp::Gte => "gte",
                CmpOp::Like => "prefix", // simplified LIKE → prefix for now
            };
            Ok(vec![PlanFilter {
                field: field.clone(),
                op: op_str.to_string(),
                value: value.clone(),
            }])
        }
        WhereExpr::And(left, right) => {
            let mut filters = collect_and_filters(left)?;
            filters.extend(collect_and_filters(right)?);
            Ok(filters)
        }
        // OR/NOT are more complex — for now, return empty and let executor
        // post-filter (the scan will return all rows, executor filters).
        WhereExpr::Or(_, _) | WhereExpr::Not(_) => Ok(Vec::new()),
        WhereExpr::IsNull { .. } | WhereExpr::IsNotNull { .. } => Ok(Vec::new()),
    }
}

//! SQL parser — wraps `sqlparser-rs` and converts SQL strings into our
//! internal statement types.

use sqlparser::ast::{
    self, AssignmentTarget, ColumnDef as SqlColumnDef, ColumnOption, DataType,
    Expr, FromTable, FunctionArg, FunctionArgExpr, GroupByExpr, ObjectName,
    OrderByKind, SelectItem, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins, UnaryOperator, Value,
    JoinConstraint, JoinOperator,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::{ColumnDef, ColumnType, ForeignKey, TableDef};

// ── Our internal AST ───────────────────────────────────────────────

/// A parsed SQL statement in our simplified internal representation.
#[derive(Debug)]
pub enum SqlStatement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
}

#[derive(Debug)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: FromClause,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<WhereExpr>,
    pub group_by: Vec<String>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    Column { table: Option<String>, name: String },
    Star { table: Option<String> },
    Aggregate { fn_name: String, field: Option<String> },
}

#[derive(Debug)]
pub struct FromClause {
    pub table: String,
    pub alias: Option<String>,
}

#[derive(Debug)]
pub struct JoinClause {
    pub table: String,
    pub alias: Option<String>,
    pub join_type: JoinType,
    pub on: JoinCondition,
}

#[derive(Debug)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug)]
pub struct JoinCondition {
    pub left_table: Option<String>,
    pub left_col: String,
    pub right_table: Option<String>,
    pub right_col: String,
}

#[derive(Debug)]
pub enum WhereExpr {
    Comparison {
        field: String,
        table: Option<String>,
        op: CmpOp,
        value: String,
    },
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
    IsNull { field: String, table: Option<String> },
    IsNotNull { field: String, table: Option<String> },
}

#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq, Neq, Lt, Lte, Gt, Gte, Like,
}

#[derive(Debug)]
pub struct OrderByItem {
    pub field: String,
    pub asc: bool,
}

#[derive(Debug)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, String)>,
    pub where_clause: Option<WhereExpr>,
}

#[derive(Debug)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<WhereExpr>,
}

#[derive(Debug)]
pub struct CreateTableStmt {
    pub table_def: TableDef,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug)]
pub struct CreateIndexStmt {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug)]
pub struct DropIndexStmt {
    pub table: String,
    pub name: String,
}

// ── Parser ─────────────────────────────────────────────────────────

pub fn parse(sql: &str) -> Result<SqlStatement, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse error: {e}"))?;

    if statements.is_empty() {
        return Err("empty SQL statement".into());
    }
    if statements.len() > 1 {
        return Err("only single statements are supported".into());
    }

    convert_statement(statements.into_iter().next().unwrap())
}

fn convert_statement(stmt: Statement) -> Result<SqlStatement, String> {
    match stmt {
        Statement::Query(query) => convert_select(*query),
        Statement::Insert(insert) => convert_insert(insert),
        Statement::Update { table, assignments, selection, .. } => {
            convert_update(table, assignments, selection)
        }
        Statement::Delete(delete) => convert_delete(delete),
        Statement::CreateTable(ct) => convert_create_table(ct),
        Statement::Drop { object_type, names, if_exists, .. } => {
            convert_drop(object_type, names, if_exists)
        }
        Statement::CreateIndex(ci) => convert_create_index(ci),
        _ => Err("unsupported statement type".into()),
    }
}

// ── SELECT ─────────────────────────────────────────────────────────

fn convert_select(query: ast::Query) -> Result<SqlStatement, String> {
    let limit = query.limit.as_ref().and_then(|e| expr_to_u32(e));
    let offset = query.offset.as_ref().and_then(|o| expr_to_u32(&o.value));

    let body = match *query.body {
        SetExpr::Select(sel) => sel,
        _ => return Err("only simple SELECT is supported".into()),
    };

    if body.from.is_empty() {
        return Err("SELECT requires a FROM clause".into());
    }
    let (from, joins) = convert_from(&body.from)?;

    let columns = body
        .projection
        .iter()
        .map(convert_select_item)
        .collect::<Result<Vec<_>, _>>()?;

    let where_clause = body.selection.as_ref().map(convert_expr).transpose()?;

    let group_by = match &body.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| expr_to_column_name(e).ok_or_else(|| "unsupported GROUP BY expression".to_string()))
            .collect::<Result<Vec<_>, _>>()?,
        GroupByExpr::All(_) => Vec::new(),
    };

    let order_by = query
        .order_by
        .as_ref()
        .map(|ob| match &ob.kind {
            OrderByKind::Expressions(exprs) => exprs
                .iter()
                .map(|item| {
                    let field = expr_to_column_name(&item.expr)
                        .ok_or_else(|| "unsupported ORDER BY expression".to_string())?;
                    Ok(OrderByItem { field, asc: item.options.asc.unwrap_or(true) })
                })
                .collect::<Result<Vec<_>, String>>(),
            OrderByKind::All(_) => Ok(Vec::new()),
        })
        .transpose()?
        .unwrap_or_default();

    Ok(SqlStatement::Select(SelectStmt {
        columns, from, joins, where_clause, group_by, order_by, limit, offset,
    }))
}

fn convert_select_item(item: &SelectItem) -> Result<SelectColumn, String> {
    match item {
        SelectItem::UnnamedExpr(expr) => convert_select_expr(expr),
        SelectItem::ExprWithAlias { expr, .. } => convert_select_expr(expr),
        SelectItem::Wildcard(_) => Ok(SelectColumn::Star { table: None }),
        SelectItem::QualifiedWildcard(kind, _) => {
            let table_name = match kind {
                ast::SelectItemQualifiedWildcardKind::ObjectName(name) => object_name_to_string(name),
                ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                    expr_to_column_name(expr).unwrap_or_default()
                }
            };
            Ok(SelectColumn::Star { table: Some(table_name) })
        }
    }
}

fn convert_select_expr(expr: &Expr) -> Result<SelectColumn, String> {
    match expr {
        Expr::Identifier(ident) => Ok(SelectColumn::Column {
            table: None, name: ident.value.clone(),
        }),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(SelectColumn::Column {
            table: Some(parts[0].value.clone()), name: parts[1].value.clone(),
        }),
        Expr::Function(func) => {
            let fn_name = object_name_to_string(&func.name).to_uppercase();
            let field = match &func.args {
                ast::FunctionArguments::List(arg_list) => {
                    arg_list.args.first().and_then(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_column_name(e),
                        _ => None,
                    })
                }
                _ => None,
            };
            Ok(SelectColumn::Aggregate { fn_name, field })
        }
        _ => Err("unsupported SELECT expression".into()),
    }
}

// ── FROM + JOINs ───────────────────────────────────────────────────

fn convert_from(from: &[TableWithJoins]) -> Result<(FromClause, Vec<JoinClause>), String> {
    let first = &from[0];
    let base = convert_table_factor(&first.relation)?;
    let mut joins = Vec::new();

    for join in &first.joins {
        let table_info = convert_table_factor(&join.relation)?;
        let (join_type, constraint) = match &join.join_operator {
            JoinOperator::Inner(c) => (JoinType::Inner, c),
            JoinOperator::LeftOuter(c) => (JoinType::Left, c),
            _ => return Err("only INNER JOIN and LEFT JOIN are supported".into()),
        };
        let on = match constraint {
            JoinConstraint::On(expr) => convert_join_on(expr)?,
            _ => return Err("only ON join condition is supported".into()),
        };
        joins.push(JoinClause {
            table: table_info.table, alias: table_info.alias, join_type, on,
        });
    }

    Ok((base, joins))
}

fn convert_table_factor(tf: &TableFactor) -> Result<FromClause, String> {
    match tf {
        TableFactor::Table { name, alias, .. } => Ok(FromClause {
            table: object_name_to_string(name),
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        }),
        _ => Err("only plain table references are supported in FROM".into()),
    }
}

fn convert_join_on(expr: &Expr) -> Result<JoinCondition, String> {
    match expr {
        Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
            let (lt, lc) = expr_to_qualified_column(left)
                .ok_or("left side of ON must be a column reference")?;
            let (rt, rc) = expr_to_qualified_column(right)
                .ok_or("right side of ON must be a column reference")?;
            Ok(JoinCondition { left_table: lt, left_col: lc, right_table: rt, right_col: rc })
        }
        _ => Err("only simple equality conditions are supported in ON".into()),
    }
}

// ── WHERE ──────────────────────────────────────────────────────────

fn convert_expr(expr: &Expr) -> Result<WhereExpr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            ast::BinaryOperator::And => {
                Ok(WhereExpr::And(Box::new(convert_expr(left)?), Box::new(convert_expr(right)?)))
            }
            ast::BinaryOperator::Or => {
                Ok(WhereExpr::Or(Box::new(convert_expr(left)?), Box::new(convert_expr(right)?)))
            }
            _ => {
                let (table, field) = expr_to_qualified_column(left)
                    .ok_or("left side of comparison must be a column")?;
                let value = expr_to_value(right)
                    .ok_or("right side of comparison must be a literal value")?;
                Ok(WhereExpr::Comparison { field, table, op: convert_binop(op)?, value })
            }
        },
        Expr::UnaryOp { op: UnaryOperator::Not, expr: inner } => {
            Ok(WhereExpr::Not(Box::new(convert_expr(inner)?)))
        }
        Expr::IsNull(inner) => {
            let (table, field) = expr_to_qualified_column(inner)
                .ok_or("IS NULL requires a column reference")?;
            Ok(WhereExpr::IsNull { field, table })
        }
        Expr::IsNotNull(inner) => {
            let (table, field) = expr_to_qualified_column(inner)
                .ok_or("IS NOT NULL requires a column reference")?;
            Ok(WhereExpr::IsNotNull { field, table })
        }
        Expr::Nested(inner) => convert_expr(inner),
        _ => Err("unsupported WHERE expression".into()),
    }
}

fn convert_binop(op: &ast::BinaryOperator) -> Result<CmpOp, String> {
    match op {
        ast::BinaryOperator::Eq => Ok(CmpOp::Eq),
        ast::BinaryOperator::NotEq => Ok(CmpOp::Neq),
        ast::BinaryOperator::Lt => Ok(CmpOp::Lt),
        ast::BinaryOperator::LtEq => Ok(CmpOp::Lte),
        ast::BinaryOperator::Gt => Ok(CmpOp::Gt),
        ast::BinaryOperator::GtEq => Ok(CmpOp::Gte),
        _ => Err(format!("unsupported comparison operator: {op}")),
    }
}

// ── INSERT ─────────────────────────────────────────────────────────

fn convert_insert(insert: ast::Insert) -> Result<SqlStatement, String> {
    let table = match &insert.table {
        TableObject::TableName(name) => object_name_to_string(name),
        TableObject::TableFunction(_) => return Err("table functions not supported".into()),
    };
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

    let source = insert.source.ok_or("INSERT requires a VALUES clause")?;
    let rows = match *source.body {
        SetExpr::Values(values) => values
            .rows
            .iter()
            .map(|row| row.iter().map(|e| expr_to_value(e).unwrap_or_default()).collect())
            .collect(),
        _ => return Err("only INSERT ... VALUES is supported".into()),
    };

    Ok(SqlStatement::Insert(InsertStmt { table, columns, rows }))
}

// ── UPDATE ─────────────────────────────────────────────────────────

fn convert_update(
    table: ast::TableWithJoins,
    assignments: Vec<ast::Assignment>,
    selection: Option<Expr>,
) -> Result<SqlStatement, String> {
    let table_name = convert_table_factor(&table.relation)?.table;
    let assigns: Vec<(String, String)> = assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                AssignmentTarget::ColumnName(name) => object_name_to_string(name),
                AssignmentTarget::Tuple(names) => {
                    names.iter().map(object_name_to_string).collect::<Vec<_>>().join(".")
                }
            };
            (col, expr_to_value(&a.value).unwrap_or_default())
        })
        .collect();

    let where_clause = selection.as_ref().map(convert_expr).transpose()?;
    Ok(SqlStatement::Update(UpdateStmt { table: table_name, assignments: assigns, where_clause }))
}

// ── DELETE ──────────────────────────────────────────────────────────

fn convert_delete(delete: ast::Delete) -> Result<SqlStatement, String> {
    let tables_vec = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let first = tables_vec.first().ok_or("DELETE requires a FROM table")?;
    let table = convert_table_factor(&first.relation)?.table;
    let where_clause = delete.selection.as_ref().map(convert_expr).transpose()?;
    Ok(SqlStatement::Delete(DeleteStmt { table, where_clause }))
}

// ── CREATE TABLE ───────────────────────────────────────────────────

fn convert_create_table(ct: ast::CreateTable) -> Result<SqlStatement, String> {
    let table = object_name_to_string(&ct.name);
    let mut columns = Vec::new();
    for col in &ct.columns {
        columns.push(convert_column_def(col)?);
    }
    Ok(SqlStatement::CreateTable(CreateTableStmt {
        table_def: TableDef { name: table, columns },
        if_not_exists: ct.if_not_exists,
    }))
}

fn convert_column_def(col: &SqlColumnDef) -> Result<ColumnDef, String> {
    let name = col.name.value.clone();
    let col_type = convert_data_type(&col.data_type)?;
    let mut primary_key = false;
    let mut not_null = false;
    let mut unique = false;
    let mut default = None;
    let mut foreign_key = None;

    for opt in &col.options {
        match &opt.option {
            ColumnOption::Unique { is_primary, .. } => {
                if *is_primary { primary_key = true; not_null = true; } else { unique = true; }
            }
            ColumnOption::NotNull => not_null = true,
            ColumnOption::Default(expr) => {
                default = expr_to_value(expr).map(|v| serde_json::Value::String(v));
            }
            ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                let ref_col = referred_columns.first()
                    .map(|c| c.value.clone())
                    .unwrap_or_else(|| "id".to_string());
                foreign_key = Some(ForeignKey {
                    table: object_name_to_string(foreign_table),
                    column: ref_col,
                });
            }
            _ => {}
        }
    }

    Ok(ColumnDef { name, col_type, primary_key, not_null, unique, default, foreign_key })
}

fn convert_data_type(dt: &DataType) -> Result<ColumnType, String> {
    match dt {
        DataType::Text | DataType::Varchar(_) | DataType::CharVarying(_)
        | DataType::Char(_) | DataType::String(_) => Ok(ColumnType::Text),

        DataType::Integer(_) | DataType::Int(_) | DataType::BigInt(_)
        | DataType::SmallInt(_) | DataType::TinyInt(_) => Ok(ColumnType::Integer),

        DataType::Real | DataType::Float(_) | DataType::Double(_)
        | DataType::DoublePrecision | DataType::Numeric(_)
        | DataType::Decimal(_) => Ok(ColumnType::Real),

        DataType::Boolean => Ok(ColumnType::Boolean),
        _ => Err(format!("unsupported data type: {dt}")),
    }
}

// ── DROP ───────────────────────────────────────────────────────────

fn convert_drop(
    object_type: ast::ObjectType,
    names: Vec<ObjectName>,
    if_exists: bool,
) -> Result<SqlStatement, String> {
    let name = names.first().map(object_name_to_string).ok_or("DROP requires a name")?;
    match object_type {
        ast::ObjectType::Table => Ok(SqlStatement::DropTable(DropTableStmt { table: name, if_exists })),
        ast::ObjectType::Index => Ok(SqlStatement::DropIndex(DropIndexStmt { table: String::new(), name })),
        _ => Err("unsupported DROP object type".into()),
    }
}

// ── CREATE INDEX ───────────────────────────────────────────────────

fn convert_create_index(ci: ast::CreateIndex) -> Result<SqlStatement, String> {
    let table = object_name_to_string(&ci.table_name);
    let columns: Vec<String> = ci.columns.iter().filter_map(|c| expr_to_column_name(&c.expr)).collect();
    Ok(SqlStatement::CreateIndex(CreateIndexStmt { table, columns }))
}

// ── Helpers ────────────────────────────────────────────────────────

fn object_name_to_string(name: &ObjectName) -> String {
    name.0.iter().filter_map(|p| p.as_ident().map(|i| i.value.clone())).collect::<Vec<_>>().join(".")
}

fn expr_to_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Some(parts.last()?.value.clone()),
        _ => None,
    }
}

fn expr_to_qualified_column(expr: &Expr) -> Option<(Option<String>, String)> {
    match expr {
        Expr::Identifier(ident) => Some((None, ident.value.clone())),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((Some(parts[0].value.clone()), parts[1].value.clone()))
        }
        _ => None,
    }
}

fn expr_to_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(val_with_span) => match &val_with_span.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
            Value::Number(n, _) => Some(n.clone()),
            Value::Boolean(b) => Some(b.to_string()),
            Value::Null => Some("null".to_string()),
            _ => None,
        },
        Expr::UnaryOp { op: UnaryOperator::Minus, expr: inner } => {
            expr_to_value(inner).map(|v| format!("-{v}"))
        }
        _ => None,
    }
}

fn expr_to_u32(expr: &Expr) -> Option<u32> {
    expr_to_value(expr).and_then(|v| v.parse().ok())
}

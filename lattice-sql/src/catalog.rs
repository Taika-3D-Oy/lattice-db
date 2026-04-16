//! Schema catalog — stores table definitions in lattice-db's `_sql_catalog` table.
//!
//! Each table definition records column names, types, constraints (PK, NOT NULL,
//! UNIQUE, FOREIGN KEY), and is persisted as a JSON row in lattice-db itself.

use lattice_db_client::LatticeDb;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The reserved lattice-db table that stores the SQL catalog.
const CATALOG_TABLE: &str = "_sql_catalog";

// ── Column / table definitions ─────────────────────────────────────

/// SQL column types we support.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Text,
    Integer,
    Real,
    Boolean,
}

/// A foreign key reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
}

/// A single column definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: ColumnType,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub not_null: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub foreign_key: Option<ForeignKey>,
}

/// A complete table definition as stored in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    /// Find the primary key column, if any.
    pub fn primary_key(&self) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.primary_key)
    }

    /// Get a column definition by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Return all column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

// ── In-memory catalog ──────────────────────────────────────────────

/// In-memory catalog loaded from lattice-db on startup.
pub struct Catalog {
    tables: HashMap<String, TableDef>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Load all table definitions from lattice-db.
    pub async fn load(db: &LatticeDb) -> Result<Self, String> {
        let mut catalog = Self::new();

        // List all keys in the catalog table — each key is a table name.
        let keys = db.keys(CATALOG_TABLE).await.unwrap_or_default();
        for key in &keys {
            match db.get_json::<TableDef>(CATALOG_TABLE, key).await {
                Ok(def) => {
                    catalog.tables.insert(def.name.clone(), def);
                }
                Err(e) => {
                    eprintln!("lattice-sql: failed to load catalog entry {key}: {e}");
                }
            }
        }
        Ok(catalog)
    }

    /// Get a table definition.
    pub fn get(&self, table: &str) -> Option<&TableDef> {
        self.tables.get(table)
    }

    /// Check if a table exists.
    pub fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Register a new table (in memory + persist to lattice-db).
    pub async fn create_table(&mut self, def: TableDef, db: &LatticeDb) -> Result<(), String> {
        if self.tables.contains_key(&def.name) {
            return Err(format!("table {} already exists", def.name));
        }
        // Persist to lattice-db.
        db.put_json(CATALOG_TABLE, &def.name, &def)
            .await
            .map_err(|e| format!("failed to persist table def: {e}"))?;

        self.tables.insert(def.name.clone(), def);
        Ok(())
    }

    /// Drop a table from the catalog (in memory + delete from lattice-db).
    pub async fn drop_table(&mut self, name: &str, db: &LatticeDb) -> Result<(), String> {
        if !self.tables.contains_key(name) {
            return Err(format!("table {name} does not exist"));
        }
        db.delete(CATALOG_TABLE, name)
            .await
            .map_err(|e| format!("failed to delete catalog entry: {e}"))?;
        self.tables.remove(name);
        Ok(())
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }
}

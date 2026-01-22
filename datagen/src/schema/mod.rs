//! TPC-DS table schema definitions.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

mod tables;
pub use tables::*;

/// TPC-DS table metadata
#[derive(Debug, Clone)]
pub struct TableSpec {
    pub name: &'static str,
    pub schema: Arc<Schema>,
    /// Number of rows per scale factor (SF=1)
    pub rows_per_sf: u64,
    /// Whether this is a fact table (larger, more rows)
    #[allow(dead_code)]
    pub is_fact_table: bool,
}

impl TableSpec {
    pub fn rows_for_scale(&self, scale_factor: u32) -> u64 {
        self.rows_per_sf * scale_factor as u64
    }
}

/// Get all TPC-DS table specifications
pub fn all_tables() -> Vec<TableSpec> {
    vec![
        // Fact tables (large)
        TableSpec {
            name: "store_sales",
            schema: store_sales_schema(),
            rows_per_sf: 2_880_000, // ~2.88M rows per SF
            is_fact_table: true,
        },
        TableSpec {
            name: "store_returns",
            schema: store_returns_schema(),
            rows_per_sf: 288_000,
            is_fact_table: true,
        },
        TableSpec {
            name: "catalog_sales",
            schema: catalog_sales_schema(),
            rows_per_sf: 1_440_000,
            is_fact_table: true,
        },
        TableSpec {
            name: "catalog_returns",
            schema: catalog_returns_schema(),
            rows_per_sf: 144_000,
            is_fact_table: true,
        },
        TableSpec {
            name: "web_sales",
            schema: web_sales_schema(),
            rows_per_sf: 720_000,
            is_fact_table: true,
        },
        TableSpec {
            name: "web_returns",
            schema: web_returns_schema(),
            rows_per_sf: 72_000,
            is_fact_table: true,
        },
        TableSpec {
            name: "inventory",
            schema: inventory_schema(),
            rows_per_sf: 11_745_000,
            is_fact_table: true,
        },
        // Dimension tables (smaller)
        TableSpec {
            name: "date_dim",
            schema: date_dim_schema(),
            rows_per_sf: 73049, // Fixed size
            is_fact_table: false,
        },
        TableSpec {
            name: "time_dim",
            schema: time_dim_schema(),
            rows_per_sf: 86400, // Fixed size
            is_fact_table: false,
        },
        TableSpec {
            name: "item",
            schema: item_schema(),
            rows_per_sf: 18000,
            is_fact_table: false,
        },
        TableSpec {
            name: "customer",
            schema: customer_schema(),
            rows_per_sf: 100_000,
            is_fact_table: false,
        },
        TableSpec {
            name: "customer_address",
            schema: customer_address_schema(),
            rows_per_sf: 50_000,
            is_fact_table: false,
        },
        TableSpec {
            name: "customer_demographics",
            schema: customer_demographics_schema(),
            rows_per_sf: 1_920_800, // Fixed
            is_fact_table: false,
        },
        TableSpec {
            name: "household_demographics",
            schema: household_demographics_schema(),
            rows_per_sf: 7200, // Fixed
            is_fact_table: false,
        },
        TableSpec {
            name: "store",
            schema: store_schema(),
            rows_per_sf: 12,
            is_fact_table: false,
        },
        TableSpec {
            name: "promotion",
            schema: promotion_schema(),
            rows_per_sf: 300,
            is_fact_table: false,
        },
        TableSpec {
            name: "warehouse",
            schema: warehouse_schema(),
            rows_per_sf: 5,
            is_fact_table: false,
        },
        TableSpec {
            name: "ship_mode",
            schema: ship_mode_schema(),
            rows_per_sf: 20, // Fixed
            is_fact_table: false,
        },
        TableSpec {
            name: "reason",
            schema: reason_schema(),
            rows_per_sf: 35, // Fixed
            is_fact_table: false,
        },
        TableSpec {
            name: "income_band",
            schema: income_band_schema(),
            rows_per_sf: 20, // Fixed
            is_fact_table: false,
        },
        TableSpec {
            name: "call_center",
            schema: call_center_schema(),
            rows_per_sf: 6,
            is_fact_table: false,
        },
        TableSpec {
            name: "catalog_page",
            schema: catalog_page_schema(),
            rows_per_sf: 11718,
            is_fact_table: false,
        },
        TableSpec {
            name: "web_site",
            schema: web_site_schema(),
            rows_per_sf: 30,
            is_fact_table: false,
        },
        TableSpec {
            name: "web_page",
            schema: web_page_schema(),
            rows_per_sf: 60,
            is_fact_table: false,
        },
    ]
}

/// Helper to create nullable field
fn nullable_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// Helper to create non-nullable field
fn required_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

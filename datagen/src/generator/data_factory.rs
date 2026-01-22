//! Data generation factory for creating table-specific generators.
//!
//! This module provides factory methods and reference data for TPC-DS
//! data generation.

use std::collections::HashMap;

/// Reference data for generating realistic TPC-DS values.
/// This struct provides lookup data for generating realistic values.
#[allow(dead_code)]
pub struct DataFactory {
    // Reference tables for lookup values
    states: Vec<&'static str>,
    cities: Vec<&'static str>,
    first_names: Vec<&'static str>,
    last_names: Vec<&'static str>,
}

impl Default for DataFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl DataFactory {
    pub fn new() -> Self {
        Self {
            states: vec![
                "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
                "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
                "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
                "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            ],
            cities: vec![
                "Midway",
                "Fairview",
                "Oak Grove",
                "Riverside",
                "Springfield",
                "Franklin",
                "Clinton",
                "Georgetown",
                "Salem",
                "Greenville",
            ],
            first_names: vec![
                "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
                "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph",
                "Jessica", "Thomas", "Sarah", "Charles", "Karen",
            ],
            last_names: vec![
                "Smith",
                "Johnson",
                "Williams",
                "Brown",
                "Jones",
                "Garcia",
                "Miller",
                "Davis",
                "Rodriguez",
                "Martinez",
            ],
        }
    }

    pub fn states(&self) -> &[&'static str] {
        &self.states
    }

    pub fn cities(&self) -> &[&'static str] {
        &self.cities
    }

    pub fn first_names(&self) -> &[&'static str] {
        &self.first_names
    }

    pub fn last_names(&self) -> &[&'static str] {
        &self.last_names
    }

    /// Get GMT offset for a state (approximate)
    pub fn gmt_offset_for_state(state: &str) -> f64 {
        match state {
            "HI" => -10.0,
            "AK" => -9.0,
            "WA" | "OR" | "CA" | "NV" => -8.0,
            "MT" | "ID" | "WY" | "UT" | "CO" | "AZ" | "NM" => -7.0,
            "ND" | "SD" | "NE" | "KS" | "OK" | "TX" | "MN" | "IA" | "MO" | "AR" | "LA" | "WI"
            | "IL" | "MS" | "AL" => -6.0,
            _ => -5.0, // Eastern time for remaining states
        }
    }

    /// Get state tax rate (approximate)
    pub fn tax_rate_for_state(state: &str) -> f64 {
        match state {
            "OR" | "MT" | "NH" | "DE" => 0.0,
            "CA" => 0.0725,
            "TN" | "LA" | "AR" | "WA" | "AL" => 0.09,
            "NY" | "NJ" | "CT" => 0.07,
            _ => 0.06,
        }
    }

    /// Create a map of table name to estimated row count per scale factor
    pub fn table_row_estimates() -> HashMap<&'static str, u64> {
        let mut map = HashMap::new();
        // Fact tables
        map.insert("store_sales", 2_880_000);
        map.insert("store_returns", 288_000);
        map.insert("catalog_sales", 1_440_000);
        map.insert("catalog_returns", 144_000);
        map.insert("web_sales", 720_000);
        map.insert("web_returns", 72_000);
        map.insert("inventory", 11_745_000);
        // Dimension tables
        map.insert("date_dim", 73049);
        map.insert("time_dim", 86400);
        map.insert("item", 18000);
        map.insert("customer", 100_000);
        map.insert("customer_address", 50_000);
        map.insert("customer_demographics", 1_920_800);
        map.insert("household_demographics", 7200);
        map.insert("store", 12);
        map.insert("promotion", 300);
        map.insert("warehouse", 5);
        map.insert("ship_mode", 20);
        map.insert("reason", 35);
        map.insert("income_band", 20);
        map.insert("call_center", 6);
        map.insert("catalog_page", 11718);
        map.insert("web_site", 30);
        map.insert("web_page", 60);
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_factory() {
        let factory = DataFactory::new();
        assert!(!factory.states().is_empty());
        assert!(!factory.cities().is_empty());
        assert!(!factory.first_names().is_empty());
        assert!(!factory.last_names().is_empty());
    }

    #[test]
    fn test_gmt_offsets() {
        assert_eq!(DataFactory::gmt_offset_for_state("CA"), -8.0);
        assert_eq!(DataFactory::gmt_offset_for_state("NY"), -5.0);
        assert_eq!(DataFactory::gmt_offset_for_state("HI"), -10.0);
    }

    #[test]
    fn test_tax_rates() {
        assert_eq!(DataFactory::tax_rate_for_state("OR"), 0.0);
        assert!(DataFactory::tax_rate_for_state("CA") > 0.0);
    }

    #[test]
    fn test_table_row_estimates() {
        let estimates = DataFactory::table_row_estimates();
        assert_eq!(estimates.get("store_sales"), Some(&2_880_000));
        assert_eq!(estimates.get("date_dim"), Some(&73049));
    }
}

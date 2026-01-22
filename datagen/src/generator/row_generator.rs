//! Row-level data generation for TPC-DS tables.
//!
//! This module generates realistic random data for each column type,
//! following TPC-DS data distribution patterns where possible.
//!
//! Performance optimizations:
//! - Static lookup tables to avoid per-row allocations
//! - Cached distributions for random number generation
//! - Pre-allocated buffers for string building
//! - Vectorized array construction where possible

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Schema};
use rand::distributions::{Alphanumeric, Distribution, Uniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// ============================================================================
// Static lookup tables - avoid per-row allocations
// ============================================================================

static NAMES: &[&str] = &[
    "James Smith", "Mary Johnson", "Robert Williams", "Patricia Brown",
    "John Jones", "Jennifer Garcia", "Michael Miller", "Linda Davis",
    "William Rodriguez", "Elizabeth Martinez",
];

static STATES: &[&str] = &[
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
];

static CITIES: &[&str] = &[
    "Midway", "Fairview", "Oak Grove", "Riverside", "Springfield",
    "Franklin", "Clinton", "Georgetown", "Salem", "Greenville",
    "Madison", "Arlington", "Bristol", "Chester", "Kingston",
];

static COUNTIES: &[&str] = &[
    "Williamson County", "Franklin County", "Washington County", "Jefferson County",
    "Madison County", "Jackson County", "Lincoln County", "Marion County",
];

static STREET_TYPES: &[&str] = &["Street", "Avenue", "Boulevard", "Drive", "Lane", "Road", "Way"];
static STREET_NAMES: &[&str] = &["Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Lake", "Forest", "Park"];
static DAYS: &[&str] = &["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
static MARITAL_STATUS: &[&str] = &["S", "M", "D", "W", "U"];
static EDUCATION: &[&str] = &["Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree", "Advanced Degree", "Unknown"];
static CREDIT_RATING: &[&str] = &["Low Risk", "Good", "High Risk", "Unknown"];
static BRANDS: &[&str] = &["brandmaxi", "brandcorp", "exportibrand", "importobrand", "edu packbrand", "namelessbrand", "amalgbrand", "corpbrand"];
static CATEGORIES: &[&str] = &["Women", "Men", "Children", "Shoes", "Music", "Books", "Home", "Electronics", "Sports", "Jewelry"];
static CLASSES: &[&str] = &["accessories", "dresses", "pants", "shirts", "shorts", "athletic", "infants", "newborn", "toddlers"];
static SIZES: &[&str] = &["small", "medium", "large", "extra large", "petite", "economy"];
static COLORS: &[&str] = &["red", "blue", "green", "yellow", "orange", "purple", "pink", "white", "black", "brown", "grey", "navy", "tan", "beige"];
static UNITS: &[&str] = &["Each", "Box", "Dozen", "Gross", "Pound", "Ounce", "Case", "Pallet"];
static CONTAINERS: &[&str] = &["Unknown", "Wrap Bag", "Wrap Box", "Wrap Case", "Wrap Tray"];
static SHIFTS: &[&str] = &["first", "second", "third"];
static SHIP_TYPES: &[&str] = &["REGULAR", "EXPRESS", "OVERNIGHT", "NEXT DAY", "LIBRARY"];
static SHIP_CODES: &[&str] = &["AIR", "SEA", "RAIL", "TRUCK", "SURFACE"];
static CARRIERS: &[&str] = &["FEDEX", "UPS", "USPS", "DHL", "AIRBORNE", "ZOUROS", "DIAMOND", "ALLIANCE"];
static REASONS: &[&str] = &["reason 1", "reason 2", "reason 3", "Did not like the color", "Did not fit", "Found a better price", "No longer needed", "Defective"];
static BUY_POTENTIAL: &[&str] = &["0-500", "501-1000", "1001-5000", "5001-10000", ">10000", "Unknown"];

/// Row generator for a specific table
pub struct RowGenerator {
    schema: Arc<Schema>,
    table_name: &'static str,
    // Cached distributions for hot paths
    date_dist: Uniform<i32>,
    null_dist: Uniform<f64>,
    // Pre-allocated buffer for ID generation (reserved for future use)
    #[allow(dead_code)]
    id_buffer: [u8; 16],
}

impl RowGenerator {
    pub fn new(schema: Arc<Schema>, table_name: &'static str) -> Self {
        Self {
            schema,
            table_name,
            date_dist: Uniform::new_inclusive(10227, 12418), // TPC-DS date range
            null_dist: Uniform::new(0.0, 1.0),
            id_buffer: [0u8; 16],
        }
    }

    /// Generate a batch of random rows
    #[inline]
    pub fn generate_batch(&self, batch_size: usize, start_row: u64) -> Result<RecordBatch> {
        let mut rng = StdRng::seed_from_u64(start_row);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

        for (field_idx, field) in self.schema.fields().iter().enumerate() {
            let array = self.generate_column(
                field.name(),
                field.data_type(),
                field.is_nullable(),
                batch_size,
                start_row,
                field_idx,
                &mut rng,
            )?;
            columns.push(array);
        }

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .context("Failed to create RecordBatch")
    }

    #[inline]
    fn generate_column(
        &self,
        name: &str,
        data_type: &DataType,
        nullable: bool,
        count: usize,
        start_row: u64,
        field_idx: usize,
        rng: &mut StdRng,
    ) -> Result<ArrayRef> {
        let null_prob = if nullable { 0.05 } else { 0.0 };

        match data_type {
            DataType::Int32 => {
                Ok(Arc::new(self.generate_int32(name, count, nullable, null_prob, rng)))
            }
            DataType::Int64 => Ok(Arc::new(self.generate_int64(
                name, count, nullable, null_prob, start_row, field_idx, rng,
            ))),
            DataType::Float64 => {
                Ok(Arc::new(self.generate_float64(name, count, nullable, null_prob, rng)))
            }
            DataType::Utf8 => {
                Ok(Arc::new(self.generate_utf8(name, count, nullable, null_prob, rng)))
            }
            DataType::Date32 => {
                Ok(Arc::new(self.generate_date32(count, nullable, null_prob, rng)))
            }
            _ => {
                Ok(Arc::new(self.generate_int64(
                    name, count, nullable, null_prob, start_row, field_idx, rng,
                )))
            }
        }
    }

    #[inline]
    fn generate_int32(
        &self,
        name: &str,
        count: usize,
        nullable: bool,
        null_prob: f64,
        rng: &mut StdRng,
    ) -> Int32Array {
        let (min, max) = self.int32_range(name);
        let dist = Uniform::new_inclusive(min, max);

        if nullable && null_prob > 0.0 {
            let mut values: Vec<Option<i32>> = Vec::with_capacity(count);
            for _ in 0..count {
                if self.null_dist.sample(rng) < null_prob {
                    values.push(None);
                } else {
                    values.push(Some(dist.sample(rng)));
                }
            }
            Int32Array::from(values)
        } else {
            // Fast path: no nulls - use primitive array directly
            let values: Vec<i32> = (0..count).map(|_| dist.sample(rng)).collect();
            Int32Array::from(values)
        }
    }

    #[inline]
    fn generate_int64(
        &self,
        name: &str,
        count: usize,
        nullable: bool,
        null_prob: f64,
        start_row: u64,
        field_idx: usize,
        rng: &mut StdRng,
    ) -> Int64Array {
        // Primary key / surrogate key columns - sequential values (fastest)
        if name.ends_with("_sk") && field_idx == 0 {
            let values: Vec<i64> = (0..count)
                .map(|i| (start_row + i as u64 + 1) as i64)
                .collect();
            return Int64Array::from(values);
        }

        // Ticket/order numbers - sequential
        if name.contains("ticket_number") || name.contains("order_number") {
            let values: Vec<i64> = (0..count)
                .map(|i| (start_row + i as u64 + 1) as i64)
                .collect();
            return Int64Array::from(values);
        }

        // Foreign key columns
        if name.ends_with("_sk") {
            let max_key = self.foreign_key_max(name);
            let dist = Uniform::new_inclusive(1, max_key);

            if nullable && null_prob > 0.0 {
                let mut values: Vec<Option<i64>> = Vec::with_capacity(count);
                for _ in 0..count {
                    if self.null_dist.sample(rng) < null_prob {
                        values.push(None);
                    } else {
                        values.push(Some(dist.sample(rng)));
                    }
                }
                return Int64Array::from(values);
            } else {
                let values: Vec<i64> = (0..count).map(|_| dist.sample(rng)).collect();
                return Int64Array::from(values);
            }
        }

        // Generic int64
        let dist = Uniform::new_inclusive(1i64, 1_000_000i64);
        if nullable && null_prob > 0.0 {
            let mut values: Vec<Option<i64>> = Vec::with_capacity(count);
            for _ in 0..count {
                if self.null_dist.sample(rng) < null_prob {
                    values.push(None);
                } else {
                    values.push(Some(dist.sample(rng)));
                }
            }
            Int64Array::from(values)
        } else {
            let values: Vec<i64> = (0..count).map(|_| dist.sample(rng)).collect();
            Int64Array::from(values)
        }
    }

    #[inline]
    fn generate_float64(
        &self,
        name: &str,
        count: usize,
        nullable: bool,
        null_prob: f64,
        rng: &mut StdRng,
    ) -> Float64Array {
        let (min, max) = self.float64_range(name);
        let dist = Uniform::new(min, max);

        if nullable && null_prob > 0.0 {
            let mut values: Vec<Option<f64>> = Vec::with_capacity(count);
            for _ in 0..count {
                if self.null_dist.sample(rng) < null_prob {
                    values.push(None);
                } else {
                    // Round to 2 decimal places
                    let val = (dist.sample(rng) * 100.0).round() / 100.0;
                    values.push(Some(val));
                }
            }
            Float64Array::from(values)
        } else {
            let values: Vec<f64> = (0..count)
                .map(|_| (dist.sample(rng) * 100.0).round() / 100.0)
                .collect();
            Float64Array::from(values)
        }
    }

    #[inline]
    fn generate_utf8(
        &self,
        name: &str,
        count: usize,
        nullable: bool,
        null_prob: f64,
        rng: &mut StdRng,
    ) -> arrow::array::StringArray {
        // Estimate average string length for capacity
        let avg_len = self.estimate_string_length(name);
        let mut builder = StringBuilder::with_capacity(count, count * avg_len);

        for _ in 0..count {
            if nullable && self.null_dist.sample(rng) < null_prob {
                builder.append_null();
            } else {
                // Use optimized string generation that avoids allocations where possible
                self.append_string_value(&mut builder, name, rng);
            }
        }

        builder.finish()
    }

    #[inline]
    fn generate_date32(
        &self,
        count: usize,
        nullable: bool,
        null_prob: f64,
        rng: &mut StdRng,
    ) -> Date32Array {
        if nullable && null_prob > 0.0 {
            let mut values: Vec<Option<i32>> = Vec::with_capacity(count);
            for _ in 0..count {
                if self.null_dist.sample(rng) < null_prob {
                    values.push(None);
                } else {
                    values.push(Some(self.date_dist.sample(rng)));
                }
            }
            Date32Array::from(values)
        } else {
            let values: Vec<i32> = (0..count).map(|_| self.date_dist.sample(rng)).collect();
            Date32Array::from(values)
        }
    }

    #[inline]
    fn estimate_string_length(&self, name: &str) -> usize {
        match name {
            n if n.ends_with("_id") => 16,
            n if n.contains("name") || n.contains("manager") => 16,
            n if n.contains("desc") || n.contains("description") => 30,
            n if n.contains("email") => 24,
            n if n.contains("url") => 28,
            n if n.contains("county") => 18,
            n if n.contains("city") => 12,
            n if n.contains("state") => 2,
            n if n.contains("zip") => 5,
            n if n.contains("gender") || n.contains("marital") => 1,
            _ => 10,
        }
    }

    /// Optimized string generation - appends directly to builder to avoid intermediate allocations
    #[inline]
    fn append_string_value(&self, builder: &mut StringBuilder, name: &str, rng: &mut StdRng) {
        match name {
            // ID fields - use stack buffer
            n if n.ends_with("_id") => {
                let mut buf = [0u8; 16];
                for b in &mut buf {
                    *b = rng.sample(Alphanumeric);
                }
                // AAAAAAAAAA + 6 chars from random
                let mut id = String::with_capacity(16);
                id.push_str("AAAAAAAAAA");
                id.push_str(std::str::from_utf8(&buf[..6]).unwrap_or("000000"));
                builder.append_value(&id);
            }

            // Static lookups - zero allocation
            n if n.contains("name") || n.contains("manager") => {
                builder.append_value(NAMES[rng.gen_range(0..NAMES.len())]);
            }
            n if n.contains("gender") => {
                builder.append_value(if rng.gen_bool(0.5) { "M" } else { "F" });
            }
            n if n.contains("marital_status") => {
                builder.append_value(MARITAL_STATUS[rng.gen_range(0..MARITAL_STATUS.len())]);
            }
            n if n.contains("education_status") => {
                builder.append_value(EDUCATION[rng.gen_range(0..EDUCATION.len())]);
            }
            n if n.contains("credit_rating") => {
                builder.append_value(CREDIT_RATING[rng.gen_range(0..CREDIT_RATING.len())]);
            }
            n if n.contains("day_name") => {
                builder.append_value(DAYS[rng.gen_range(0..DAYS.len())]);
            }
            n if n.contains("quarter_name") => {
                // Small allocation for formatted string
                let year = rng.gen_range(1998..=2003);
                let quarter = rng.gen_range(1..=4);
                let mut buf = String::with_capacity(6);
                use std::fmt::Write;
                let _ = write!(buf, "{}Q{}", year, quarter);
                builder.append_value(&buf);
            }
            n if n.contains("holiday") || n.contains("weekend") || n.contains("current") || n.contains("flag") => {
                builder.append_value(if rng.gen_bool(0.5) { "Y" } else { "N" });
            }
            n if n.contains("state") => {
                builder.append_value(STATES[rng.gen_range(0..STATES.len())]);
            }
            n if n.contains("city") => {
                builder.append_value(CITIES[rng.gen_range(0..CITIES.len())]);
            }
            n if n.contains("country") => {
                builder.append_value("United States");
            }
            n if n.contains("county") => {
                builder.append_value(COUNTIES[rng.gen_range(0..COUNTIES.len())]);
            }
            n if n.contains("street_type") => {
                builder.append_value(STREET_TYPES[rng.gen_range(0..STREET_TYPES.len())]);
            }
            n if n.contains("street_name") => {
                builder.append_value(STREET_NAMES[rng.gen_range(0..STREET_NAMES.len())]);
            }
            n if n.contains("street_number") => {
                let num = rng.gen_range(1..9999);
                let mut buf = itoa::Buffer::new();
                builder.append_value(buf.format(num));
            }
            n if n.contains("suite_number") => {
                let num = rng.gen_range(100..999);
                let mut s = String::with_capacity(10);
                use std::fmt::Write;
                let _ = write!(s, "Suite {}", num);
                builder.append_value(&s);
            }
            n if n.contains("zip") => {
                let zip = rng.gen_range(10000..99999);
                let mut buf = itoa::Buffer::new();
                let s = buf.format(zip);
                builder.append_value(s);
            }
            n if n.contains("email") => {
                let mut email = String::with_capacity(24);
                for _ in 0..8 {
                    email.push((rng.sample(Alphanumeric) as char).to_ascii_lowercase());
                }
                email.push_str("@example.com");
                builder.append_value(&email);
            }
            n if n.contains("url") => {
                let num = rng.gen_range(1..1000);
                let mut url = String::with_capacity(28);
                use std::fmt::Write;
                let _ = write!(url, "http://www.example{}.com", num);
                builder.append_value(&url);
            }
            n if n.contains("desc") || n.contains("description") => {
                static WORDS: &[&str] = &[
                    "High quality", "Premium", "Standard", "Basic", "Advanced", "Professional", "Consumer grade", "Industrial",
                ];
                let w1 = WORDS[rng.gen_range(0..WORDS.len())];
                let w2 = WORDS[rng.gen_range(0..WORDS.len())];
                let mut desc = String::with_capacity(32);
                desc.push_str(w1);
                desc.push(' ');
                desc.push_str(w2);
                desc.push_str(" product");
                builder.append_value(&desc);
            }
            n if n.contains("brand") => {
                builder.append_value(BRANDS[rng.gen_range(0..BRANDS.len())]);
            }
            n if n.contains("category") => {
                builder.append_value(CATEGORIES[rng.gen_range(0..CATEGORIES.len())]);
            }
            n if n.contains("class") && !n.contains("class_id") => {
                builder.append_value(CLASSES[rng.gen_range(0..CLASSES.len())]);
            }
            n if n.contains("size") => {
                builder.append_value(SIZES[rng.gen_range(0..SIZES.len())]);
            }
            n if n.contains("color") => {
                builder.append_value(COLORS[rng.gen_range(0..COLORS.len())]);
            }
            n if n.contains("units") => {
                builder.append_value(UNITS[rng.gen_range(0..UNITS.len())]);
            }
            n if n.contains("container") => {
                builder.append_value(CONTAINERS[rng.gen_range(0..CONTAINERS.len())]);
            }
            n if n.contains("am_pm") => {
                builder.append_value(if rng.gen_bool(0.5) { "AM" } else { "PM" });
            }
            n if n.contains("shift") => {
                builder.append_value(SHIFTS[rng.gen_range(0..SHIFTS.len())]);
            }
            n if n.contains("sm_type") || (n.contains("type") && self.table_name == "ship_mode") => {
                builder.append_value(SHIP_TYPES[rng.gen_range(0..SHIP_TYPES.len())]);
            }
            n if n.contains("sm_code") || (n.contains("code") && self.table_name == "ship_mode") => {
                builder.append_value(SHIP_CODES[rng.gen_range(0..SHIP_CODES.len())]);
            }
            n if n.contains("carrier") => {
                builder.append_value(CARRIERS[rng.gen_range(0..CARRIERS.len())]);
            }
            n if n.contains("reason") && n.contains("desc") => {
                builder.append_value(REASONS[rng.gen_range(0..REASONS.len())]);
            }
            n if n.contains("buy_potential") => {
                builder.append_value(BUY_POTENTIAL[rng.gen_range(0..BUY_POTENTIAL.len())]);
            }
            n if n.contains("hours") && self.table_name != "time_dim" => {
                builder.append_value("8AM-5PM");
            }
            // Default: short random alphanumeric
            _ => {
                let len = rng.gen_range(5..15);
                let s: String = (0..len).map(|_| rng.sample(Alphanumeric) as char).collect();
                builder.append_value(&s);
            }
        }
    }

    #[inline]
    fn int32_range(&self, name: &str) -> (i32, i32) {
        match name {
            n if n.contains("quantity") => (1, 100),
            n if n.contains("year") => (1998, 2003),
            n if n.contains("month") || n.ends_with("_moy") => (1, 12),
            n if n.contains("day") || n.ends_with("_dom") => (1, 31),
            n if n.ends_with("_dow") => (0, 6),
            n if n.ends_with("_qoy") => (1, 4),
            n if n.contains("hour") => (0, 23),
            n if n.contains("minute") || n.contains("second") => (0, 59),
            n if n.contains("seq") => (1, 1000),
            n if n.contains("count") => (0, 10),
            n if n.contains("employees") => (1, 500),
            n if n.contains("sq_ft") || n.contains("floor_space") => (1000, 100000),
            _ => (1, 1000),
        }
    }

    #[inline]
    fn float64_range(&self, name: &str) -> (f64, f64) {
        match name {
            n if n.contains("price") => (1.0, 500.0),
            n if n.contains("cost") => (0.5, 300.0),
            n if n.contains("amt") || n.contains("amount") => (0.0, 10000.0),
            n if n.contains("tax") => (0.0, 500.0),
            n if n.contains("discount") => (0.0, 200.0),
            n if n.contains("profit") || n.contains("loss") => (-5000.0, 5000.0),
            n if n.contains("gmt_offset") => (-12.0, 12.0),
            n if n.contains("percentage") => (0.0, 1.0),
            _ => (0.0, 1000.0),
        }
    }

    #[inline]
    fn foreign_key_max(&self, name: &str) -> i64 {
        match name {
            n if n.contains("date_sk") => 73049,
            n if n.contains("time_sk") => 86400,
            n if n.contains("item_sk") => 18000,
            n if n.contains("customer_sk") => 100_000,
            n if n.contains("cdemo_sk") => 1_920_800,
            n if n.contains("hdemo_sk") => 7200,
            n if n.contains("addr_sk") => 50_000,
            n if n.contains("store_sk") => 12,
            n if n.contains("promo_sk") => 300,
            n if n.contains("warehouse_sk") => 5,
            n if n.contains("ship_mode_sk") => 20,
            n if n.contains("reason_sk") => 35,
            n if n.contains("income_band_sk") => 20,
            n if n.contains("call_center_sk") => 6,
            n if n.contains("catalog_page_sk") => 11718,
            n if n.contains("web_site_sk") => 30,
            n if n.contains("web_page_sk") => 60,
            _ => 10000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::store_sales_schema;

    #[test]
    fn test_generate_batch() {
        let schema = store_sales_schema();
        let generator = RowGenerator::new(schema, "store_sales");
        let batch = generator.generate_batch(100, 0).unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_generate_multiple_batches() {
        let schema = store_sales_schema();
        let generator = RowGenerator::new(schema, "store_sales");

        let batch1 = generator.generate_batch(100, 0).unwrap();
        let batch2 = generator.generate_batch(100, 100).unwrap();

        assert_eq!(batch1.num_rows(), 100);
        assert_eq!(batch2.num_rows(), 100);
    }

    #[test]
    fn test_static_lookups_not_empty() {
        assert!(!NAMES.is_empty());
        assert!(!STATES.is_empty());
        assert!(!CITIES.is_empty());
        assert!(!DAYS.is_empty());
    }
}

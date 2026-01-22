//! TPC-DS table schema definitions.

use arrow::datatypes::{DataType, Schema};
use std::sync::Arc;

use super::{nullable_field, required_field};

// ============================================================================
// Fact Tables
// ============================================================================

pub fn store_sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        nullable_field("ss_sold_date_sk", DataType::Int64),
        nullable_field("ss_sold_time_sk", DataType::Int64),
        nullable_field("ss_item_sk", DataType::Int64),
        nullable_field("ss_customer_sk", DataType::Int64),
        nullable_field("ss_cdemo_sk", DataType::Int64),
        nullable_field("ss_hdemo_sk", DataType::Int64),
        nullable_field("ss_addr_sk", DataType::Int64),
        nullable_field("ss_store_sk", DataType::Int64),
        nullable_field("ss_promo_sk", DataType::Int64),
        required_field("ss_ticket_number", DataType::Int64),
        nullable_field("ss_quantity", DataType::Int32),
        nullable_field("ss_wholesale_cost", DataType::Float64),
        nullable_field("ss_list_price", DataType::Float64),
        nullable_field("ss_sales_price", DataType::Float64),
        nullable_field("ss_ext_discount_amt", DataType::Float64),
        nullable_field("ss_ext_sales_price", DataType::Float64),
        nullable_field("ss_ext_wholesale_cost", DataType::Float64),
        nullable_field("ss_ext_list_price", DataType::Float64),
        nullable_field("ss_ext_tax", DataType::Float64),
        nullable_field("ss_coupon_amt", DataType::Float64),
        nullable_field("ss_net_paid", DataType::Float64),
        nullable_field("ss_net_paid_inc_tax", DataType::Float64),
        nullable_field("ss_net_profit", DataType::Float64),
    ]))
}

pub fn store_returns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        nullable_field("sr_returned_date_sk", DataType::Int64),
        nullable_field("sr_return_time_sk", DataType::Int64),
        nullable_field("sr_item_sk", DataType::Int64),
        nullable_field("sr_customer_sk", DataType::Int64),
        nullable_field("sr_cdemo_sk", DataType::Int64),
        nullable_field("sr_hdemo_sk", DataType::Int64),
        nullable_field("sr_addr_sk", DataType::Int64),
        nullable_field("sr_store_sk", DataType::Int64),
        nullable_field("sr_reason_sk", DataType::Int64),
        required_field("sr_ticket_number", DataType::Int64),
        nullable_field("sr_return_quantity", DataType::Int32),
        nullable_field("sr_return_amt", DataType::Float64),
        nullable_field("sr_return_tax", DataType::Float64),
        nullable_field("sr_return_amt_inc_tax", DataType::Float64),
        nullable_field("sr_fee", DataType::Float64),
        nullable_field("sr_return_ship_cost", DataType::Float64),
        nullable_field("sr_refunded_cash", DataType::Float64),
        nullable_field("sr_reversed_charge", DataType::Float64),
        nullable_field("sr_store_credit", DataType::Float64),
        nullable_field("sr_net_loss", DataType::Float64),
    ]))
}

pub fn catalog_sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        nullable_field("cs_sold_date_sk", DataType::Int64),
        nullable_field("cs_sold_time_sk", DataType::Int64),
        nullable_field("cs_ship_date_sk", DataType::Int64),
        nullable_field("cs_bill_customer_sk", DataType::Int64),
        nullable_field("cs_bill_cdemo_sk", DataType::Int64),
        nullable_field("cs_bill_hdemo_sk", DataType::Int64),
        nullable_field("cs_bill_addr_sk", DataType::Int64),
        nullable_field("cs_ship_customer_sk", DataType::Int64),
        nullable_field("cs_ship_cdemo_sk", DataType::Int64),
        nullable_field("cs_ship_hdemo_sk", DataType::Int64),
        nullable_field("cs_ship_addr_sk", DataType::Int64),
        nullable_field("cs_call_center_sk", DataType::Int64),
        nullable_field("cs_catalog_page_sk", DataType::Int64),
        nullable_field("cs_ship_mode_sk", DataType::Int64),
        nullable_field("cs_warehouse_sk", DataType::Int64),
        nullable_field("cs_item_sk", DataType::Int64),
        nullable_field("cs_promo_sk", DataType::Int64),
        required_field("cs_order_number", DataType::Int64),
        nullable_field("cs_quantity", DataType::Int32),
        nullable_field("cs_wholesale_cost", DataType::Float64),
        nullable_field("cs_list_price", DataType::Float64),
        nullable_field("cs_sales_price", DataType::Float64),
        nullable_field("cs_ext_discount_amt", DataType::Float64),
        nullable_field("cs_ext_sales_price", DataType::Float64),
        nullable_field("cs_ext_wholesale_cost", DataType::Float64),
        nullable_field("cs_ext_list_price", DataType::Float64),
        nullable_field("cs_ext_tax", DataType::Float64),
        nullable_field("cs_coupon_amt", DataType::Float64),
        nullable_field("cs_ext_ship_cost", DataType::Float64),
        nullable_field("cs_net_paid", DataType::Float64),
        nullable_field("cs_net_paid_inc_tax", DataType::Float64),
        nullable_field("cs_net_paid_inc_ship", DataType::Float64),
        nullable_field("cs_net_paid_inc_ship_tax", DataType::Float64),
        nullable_field("cs_net_profit", DataType::Float64),
    ]))
}

pub fn catalog_returns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        nullable_field("cr_returned_date_sk", DataType::Int64),
        nullable_field("cr_returned_time_sk", DataType::Int64),
        nullable_field("cr_item_sk", DataType::Int64),
        nullable_field("cr_refunded_customer_sk", DataType::Int64),
        nullable_field("cr_refunded_cdemo_sk", DataType::Int64),
        nullable_field("cr_refunded_hdemo_sk", DataType::Int64),
        nullable_field("cr_refunded_addr_sk", DataType::Int64),
        nullable_field("cr_returning_customer_sk", DataType::Int64),
        nullable_field("cr_returning_cdemo_sk", DataType::Int64),
        nullable_field("cr_returning_hdemo_sk", DataType::Int64),
        nullable_field("cr_returning_addr_sk", DataType::Int64),
        nullable_field("cr_call_center_sk", DataType::Int64),
        nullable_field("cr_catalog_page_sk", DataType::Int64),
        nullable_field("cr_ship_mode_sk", DataType::Int64),
        nullable_field("cr_warehouse_sk", DataType::Int64),
        nullable_field("cr_reason_sk", DataType::Int64),
        required_field("cr_order_number", DataType::Int64),
        nullable_field("cr_return_quantity", DataType::Int32),
        nullable_field("cr_return_amount", DataType::Float64),
        nullable_field("cr_return_tax", DataType::Float64),
        nullable_field("cr_return_amt_inc_tax", DataType::Float64),
        nullable_field("cr_fee", DataType::Float64),
        nullable_field("cr_return_ship_cost", DataType::Float64),
        nullable_field("cr_refunded_cash", DataType::Float64),
        nullable_field("cr_reversed_charge", DataType::Float64),
        nullable_field("cr_store_credit", DataType::Float64),
        nullable_field("cr_net_loss", DataType::Float64),
    ]))
}

pub fn web_sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        nullable_field("ws_sold_date_sk", DataType::Int64),
        nullable_field("ws_sold_time_sk", DataType::Int64),
        nullable_field("ws_ship_date_sk", DataType::Int64),
        nullable_field("ws_item_sk", DataType::Int64),
        nullable_field("ws_bill_customer_sk", DataType::Int64),
        nullable_field("ws_bill_cdemo_sk", DataType::Int64),
        nullable_field("ws_bill_hdemo_sk", DataType::Int64),
        nullable_field("ws_bill_addr_sk", DataType::Int64),
        nullable_field("ws_ship_customer_sk", DataType::Int64),
        nullable_field("ws_ship_cdemo_sk", DataType::Int64),
        nullable_field("ws_ship_hdemo_sk", DataType::Int64),
        nullable_field("ws_ship_addr_sk", DataType::Int64),
        nullable_field("ws_web_page_sk", DataType::Int64),
        nullable_field("ws_web_site_sk", DataType::Int64),
        nullable_field("ws_ship_mode_sk", DataType::Int64),
        nullable_field("ws_warehouse_sk", DataType::Int64),
        nullable_field("ws_promo_sk", DataType::Int64),
        required_field("ws_order_number", DataType::Int64),
        nullable_field("ws_quantity", DataType::Int32),
        nullable_field("ws_wholesale_cost", DataType::Float64),
        nullable_field("ws_list_price", DataType::Float64),
        nullable_field("ws_sales_price", DataType::Float64),
        nullable_field("ws_ext_discount_amt", DataType::Float64),
        nullable_field("ws_ext_sales_price", DataType::Float64),
        nullable_field("ws_ext_wholesale_cost", DataType::Float64),
        nullable_field("ws_ext_list_price", DataType::Float64),
        nullable_field("ws_ext_tax", DataType::Float64),
        nullable_field("ws_coupon_amt", DataType::Float64),
        nullable_field("ws_ext_ship_cost", DataType::Float64),
        nullable_field("ws_net_paid", DataType::Float64),
        nullable_field("ws_net_paid_inc_tax", DataType::Float64),
        nullable_field("ws_net_paid_inc_ship", DataType::Float64),
        nullable_field("ws_net_paid_inc_ship_tax", DataType::Float64),
        nullable_field("ws_net_profit", DataType::Float64),
    ]))
}

pub fn web_returns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        nullable_field("wr_returned_date_sk", DataType::Int64),
        nullable_field("wr_returned_time_sk", DataType::Int64),
        nullable_field("wr_item_sk", DataType::Int64),
        nullable_field("wr_refunded_customer_sk", DataType::Int64),
        nullable_field("wr_refunded_cdemo_sk", DataType::Int64),
        nullable_field("wr_refunded_hdemo_sk", DataType::Int64),
        nullable_field("wr_refunded_addr_sk", DataType::Int64),
        nullable_field("wr_returning_customer_sk", DataType::Int64),
        nullable_field("wr_returning_cdemo_sk", DataType::Int64),
        nullable_field("wr_returning_hdemo_sk", DataType::Int64),
        nullable_field("wr_returning_addr_sk", DataType::Int64),
        nullable_field("wr_web_page_sk", DataType::Int64),
        nullable_field("wr_reason_sk", DataType::Int64),
        required_field("wr_order_number", DataType::Int64),
        nullable_field("wr_return_quantity", DataType::Int32),
        nullable_field("wr_return_amt", DataType::Float64),
        nullable_field("wr_return_tax", DataType::Float64),
        nullable_field("wr_return_amt_inc_tax", DataType::Float64),
        nullable_field("wr_fee", DataType::Float64),
        nullable_field("wr_return_ship_cost", DataType::Float64),
        nullable_field("wr_refunded_cash", DataType::Float64),
        nullable_field("wr_reversed_charge", DataType::Float64),
        nullable_field("wr_account_credit", DataType::Float64),
        nullable_field("wr_net_loss", DataType::Float64),
    ]))
}

pub fn inventory_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("inv_date_sk", DataType::Int64),
        required_field("inv_item_sk", DataType::Int64),
        required_field("inv_warehouse_sk", DataType::Int64),
        nullable_field("inv_quantity_on_hand", DataType::Int32),
    ]))
}

// ============================================================================
// Dimension Tables
// ============================================================================

pub fn date_dim_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("d_date_sk", DataType::Int64),
        required_field("d_date_id", DataType::Utf8),
        nullable_field("d_date", DataType::Date32),
        nullable_field("d_month_seq", DataType::Int32),
        nullable_field("d_week_seq", DataType::Int32),
        nullable_field("d_quarter_seq", DataType::Int32),
        nullable_field("d_year", DataType::Int32),
        nullable_field("d_dow", DataType::Int32),
        nullable_field("d_moy", DataType::Int32),
        nullable_field("d_dom", DataType::Int32),
        nullable_field("d_qoy", DataType::Int32),
        nullable_field("d_fy_year", DataType::Int32),
        nullable_field("d_fy_quarter_seq", DataType::Int32),
        nullable_field("d_fy_week_seq", DataType::Int32),
        nullable_field("d_day_name", DataType::Utf8),
        nullable_field("d_quarter_name", DataType::Utf8),
        nullable_field("d_holiday", DataType::Utf8),
        nullable_field("d_weekend", DataType::Utf8),
        nullable_field("d_following_holiday", DataType::Utf8),
        nullable_field("d_first_dom", DataType::Int32),
        nullable_field("d_last_dom", DataType::Int32),
        nullable_field("d_same_day_ly", DataType::Int32),
        nullable_field("d_same_day_lq", DataType::Int32),
        nullable_field("d_current_day", DataType::Utf8),
        nullable_field("d_current_week", DataType::Utf8),
        nullable_field("d_current_month", DataType::Utf8),
        nullable_field("d_current_quarter", DataType::Utf8),
        nullable_field("d_current_year", DataType::Utf8),
    ]))
}

pub fn time_dim_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("t_time_sk", DataType::Int64),
        required_field("t_time_id", DataType::Utf8),
        nullable_field("t_time", DataType::Int32),
        nullable_field("t_hour", DataType::Int32),
        nullable_field("t_minute", DataType::Int32),
        nullable_field("t_second", DataType::Int32),
        nullable_field("t_am_pm", DataType::Utf8),
        nullable_field("t_shift", DataType::Utf8),
        nullable_field("t_sub_shift", DataType::Utf8),
        nullable_field("t_meal_time", DataType::Utf8),
    ]))
}

pub fn item_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("i_item_sk", DataType::Int64),
        required_field("i_item_id", DataType::Utf8),
        nullable_field("i_rec_start_date", DataType::Date32),
        nullable_field("i_rec_end_date", DataType::Date32),
        nullable_field("i_item_desc", DataType::Utf8),
        nullable_field("i_current_price", DataType::Float64),
        nullable_field("i_wholesale_cost", DataType::Float64),
        nullable_field("i_brand_id", DataType::Int32),
        nullable_field("i_brand", DataType::Utf8),
        nullable_field("i_class_id", DataType::Int32),
        nullable_field("i_class", DataType::Utf8),
        nullable_field("i_category_id", DataType::Int32),
        nullable_field("i_category", DataType::Utf8),
        nullable_field("i_manufact_id", DataType::Int32),
        nullable_field("i_manufact", DataType::Utf8),
        nullable_field("i_size", DataType::Utf8),
        nullable_field("i_formulation", DataType::Utf8),
        nullable_field("i_color", DataType::Utf8),
        nullable_field("i_units", DataType::Utf8),
        nullable_field("i_container", DataType::Utf8),
        nullable_field("i_manager_id", DataType::Int32),
        nullable_field("i_product_name", DataType::Utf8),
    ]))
}

pub fn customer_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("c_customer_sk", DataType::Int64),
        required_field("c_customer_id", DataType::Utf8),
        nullable_field("c_current_cdemo_sk", DataType::Int64),
        nullable_field("c_current_hdemo_sk", DataType::Int64),
        nullable_field("c_current_addr_sk", DataType::Int64),
        nullable_field("c_first_shipto_date_sk", DataType::Int64),
        nullable_field("c_first_sales_date_sk", DataType::Int64),
        nullable_field("c_salutation", DataType::Utf8),
        nullable_field("c_first_name", DataType::Utf8),
        nullable_field("c_last_name", DataType::Utf8),
        nullable_field("c_preferred_cust_flag", DataType::Utf8),
        nullable_field("c_birth_day", DataType::Int32),
        nullable_field("c_birth_month", DataType::Int32),
        nullable_field("c_birth_year", DataType::Int32),
        nullable_field("c_birth_country", DataType::Utf8),
        nullable_field("c_login", DataType::Utf8),
        nullable_field("c_email_address", DataType::Utf8),
        nullable_field("c_last_review_date_sk", DataType::Int64),
    ]))
}

pub fn customer_address_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("ca_address_sk", DataType::Int64),
        required_field("ca_address_id", DataType::Utf8),
        nullable_field("ca_street_number", DataType::Utf8),
        nullable_field("ca_street_name", DataType::Utf8),
        nullable_field("ca_street_type", DataType::Utf8),
        nullable_field("ca_suite_number", DataType::Utf8),
        nullable_field("ca_city", DataType::Utf8),
        nullable_field("ca_county", DataType::Utf8),
        nullable_field("ca_state", DataType::Utf8),
        nullable_field("ca_zip", DataType::Utf8),
        nullable_field("ca_country", DataType::Utf8),
        nullable_field("ca_gmt_offset", DataType::Float64),
        nullable_field("ca_location_type", DataType::Utf8),
    ]))
}

pub fn customer_demographics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("cd_demo_sk", DataType::Int64),
        nullable_field("cd_gender", DataType::Utf8),
        nullable_field("cd_marital_status", DataType::Utf8),
        nullable_field("cd_education_status", DataType::Utf8),
        nullable_field("cd_purchase_estimate", DataType::Int32),
        nullable_field("cd_credit_rating", DataType::Utf8),
        nullable_field("cd_dep_count", DataType::Int32),
        nullable_field("cd_dep_employed_count", DataType::Int32),
        nullable_field("cd_dep_college_count", DataType::Int32),
    ]))
}

pub fn household_demographics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("hd_demo_sk", DataType::Int64),
        nullable_field("hd_income_band_sk", DataType::Int64),
        nullable_field("hd_buy_potential", DataType::Utf8),
        nullable_field("hd_dep_count", DataType::Int32),
        nullable_field("hd_vehicle_count", DataType::Int32),
    ]))
}

pub fn store_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("s_store_sk", DataType::Int64),
        required_field("s_store_id", DataType::Utf8),
        nullable_field("s_rec_start_date", DataType::Date32),
        nullable_field("s_rec_end_date", DataType::Date32),
        nullable_field("s_closed_date_sk", DataType::Int64),
        nullable_field("s_store_name", DataType::Utf8),
        nullable_field("s_number_employees", DataType::Int32),
        nullable_field("s_floor_space", DataType::Int32),
        nullable_field("s_hours", DataType::Utf8),
        nullable_field("s_manager", DataType::Utf8),
        nullable_field("s_market_id", DataType::Int32),
        nullable_field("s_geography_class", DataType::Utf8),
        nullable_field("s_market_desc", DataType::Utf8),
        nullable_field("s_market_manager", DataType::Utf8),
        nullable_field("s_division_id", DataType::Int32),
        nullable_field("s_division_name", DataType::Utf8),
        nullable_field("s_company_id", DataType::Int32),
        nullable_field("s_company_name", DataType::Utf8),
        nullable_field("s_street_number", DataType::Utf8),
        nullable_field("s_street_name", DataType::Utf8),
        nullable_field("s_street_type", DataType::Utf8),
        nullable_field("s_suite_number", DataType::Utf8),
        nullable_field("s_city", DataType::Utf8),
        nullable_field("s_county", DataType::Utf8),
        nullable_field("s_state", DataType::Utf8),
        nullable_field("s_zip", DataType::Utf8),
        nullable_field("s_country", DataType::Utf8),
        nullable_field("s_gmt_offset", DataType::Float64),
        nullable_field("s_tax_percentage", DataType::Float64),
    ]))
}

pub fn promotion_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("p_promo_sk", DataType::Int64),
        required_field("p_promo_id", DataType::Utf8),
        nullable_field("p_start_date_sk", DataType::Int64),
        nullable_field("p_end_date_sk", DataType::Int64),
        nullable_field("p_item_sk", DataType::Int64),
        nullable_field("p_cost", DataType::Float64),
        nullable_field("p_response_target", DataType::Int32),
        nullable_field("p_promo_name", DataType::Utf8),
        nullable_field("p_channel_dmail", DataType::Utf8),
        nullable_field("p_channel_email", DataType::Utf8),
        nullable_field("p_channel_catalog", DataType::Utf8),
        nullable_field("p_channel_tv", DataType::Utf8),
        nullable_field("p_channel_radio", DataType::Utf8),
        nullable_field("p_channel_press", DataType::Utf8),
        nullable_field("p_channel_event", DataType::Utf8),
        nullable_field("p_channel_demo", DataType::Utf8),
        nullable_field("p_channel_details", DataType::Utf8),
        nullable_field("p_purpose", DataType::Utf8),
        nullable_field("p_discount_active", DataType::Utf8),
    ]))
}

pub fn warehouse_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("w_warehouse_sk", DataType::Int64),
        required_field("w_warehouse_id", DataType::Utf8),
        nullable_field("w_warehouse_name", DataType::Utf8),
        nullable_field("w_warehouse_sq_ft", DataType::Int32),
        nullable_field("w_street_number", DataType::Utf8),
        nullable_field("w_street_name", DataType::Utf8),
        nullable_field("w_street_type", DataType::Utf8),
        nullable_field("w_suite_number", DataType::Utf8),
        nullable_field("w_city", DataType::Utf8),
        nullable_field("w_county", DataType::Utf8),
        nullable_field("w_state", DataType::Utf8),
        nullable_field("w_zip", DataType::Utf8),
        nullable_field("w_country", DataType::Utf8),
        nullable_field("w_gmt_offset", DataType::Float64),
    ]))
}

pub fn ship_mode_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("sm_ship_mode_sk", DataType::Int64),
        required_field("sm_ship_mode_id", DataType::Utf8),
        nullable_field("sm_type", DataType::Utf8),
        nullable_field("sm_code", DataType::Utf8),
        nullable_field("sm_carrier", DataType::Utf8),
        nullable_field("sm_contract", DataType::Utf8),
    ]))
}

pub fn reason_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("r_reason_sk", DataType::Int64),
        required_field("r_reason_id", DataType::Utf8),
        nullable_field("r_reason_desc", DataType::Utf8),
    ]))
}

pub fn income_band_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("ib_income_band_sk", DataType::Int64),
        nullable_field("ib_lower_bound", DataType::Int32),
        nullable_field("ib_upper_bound", DataType::Int32),
    ]))
}

pub fn call_center_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("cc_call_center_sk", DataType::Int64),
        required_field("cc_call_center_id", DataType::Utf8),
        nullable_field("cc_rec_start_date", DataType::Date32),
        nullable_field("cc_rec_end_date", DataType::Date32),
        nullable_field("cc_closed_date_sk", DataType::Int64),
        nullable_field("cc_open_date_sk", DataType::Int64),
        nullable_field("cc_name", DataType::Utf8),
        nullable_field("cc_class", DataType::Utf8),
        nullable_field("cc_employees", DataType::Int32),
        nullable_field("cc_sq_ft", DataType::Int32),
        nullable_field("cc_hours", DataType::Utf8),
        nullable_field("cc_manager", DataType::Utf8),
        nullable_field("cc_mkt_id", DataType::Int32),
        nullable_field("cc_mkt_class", DataType::Utf8),
        nullable_field("cc_mkt_desc", DataType::Utf8),
        nullable_field("cc_market_manager", DataType::Utf8),
        nullable_field("cc_division", DataType::Int32),
        nullable_field("cc_division_name", DataType::Utf8),
        nullable_field("cc_company", DataType::Int32),
        nullable_field("cc_company_name", DataType::Utf8),
        nullable_field("cc_street_number", DataType::Utf8),
        nullable_field("cc_street_name", DataType::Utf8),
        nullable_field("cc_street_type", DataType::Utf8),
        nullable_field("cc_suite_number", DataType::Utf8),
        nullable_field("cc_city", DataType::Utf8),
        nullable_field("cc_county", DataType::Utf8),
        nullable_field("cc_state", DataType::Utf8),
        nullable_field("cc_zip", DataType::Utf8),
        nullable_field("cc_country", DataType::Utf8),
        nullable_field("cc_gmt_offset", DataType::Float64),
        nullable_field("cc_tax_percentage", DataType::Float64),
    ]))
}

pub fn catalog_page_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("cp_catalog_page_sk", DataType::Int64),
        required_field("cp_catalog_page_id", DataType::Utf8),
        nullable_field("cp_start_date_sk", DataType::Int64),
        nullable_field("cp_end_date_sk", DataType::Int64),
        nullable_field("cp_department", DataType::Utf8),
        nullable_field("cp_catalog_number", DataType::Int32),
        nullable_field("cp_catalog_page_number", DataType::Int32),
        nullable_field("cp_description", DataType::Utf8),
        nullable_field("cp_type", DataType::Utf8),
    ]))
}

pub fn web_site_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("web_site_sk", DataType::Int64),
        required_field("web_site_id", DataType::Utf8),
        nullable_field("web_rec_start_date", DataType::Date32),
        nullable_field("web_rec_end_date", DataType::Date32),
        nullable_field("web_name", DataType::Utf8),
        nullable_field("web_open_date_sk", DataType::Int64),
        nullable_field("web_close_date_sk", DataType::Int64),
        nullable_field("web_class", DataType::Utf8),
        nullable_field("web_manager", DataType::Utf8),
        nullable_field("web_mkt_id", DataType::Int32),
        nullable_field("web_mkt_class", DataType::Utf8),
        nullable_field("web_mkt_desc", DataType::Utf8),
        nullable_field("web_market_manager", DataType::Utf8),
        nullable_field("web_company_id", DataType::Int32),
        nullable_field("web_company_name", DataType::Utf8),
        nullable_field("web_street_number", DataType::Utf8),
        nullable_field("web_street_name", DataType::Utf8),
        nullable_field("web_street_type", DataType::Utf8),
        nullable_field("web_suite_number", DataType::Utf8),
        nullable_field("web_city", DataType::Utf8),
        nullable_field("web_county", DataType::Utf8),
        nullable_field("web_state", DataType::Utf8),
        nullable_field("web_zip", DataType::Utf8),
        nullable_field("web_country", DataType::Utf8),
        nullable_field("web_gmt_offset", DataType::Float64),
        nullable_field("web_tax_percentage", DataType::Float64),
    ]))
}

pub fn web_page_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required_field("wp_web_page_sk", DataType::Int64),
        required_field("wp_web_page_id", DataType::Utf8),
        nullable_field("wp_rec_start_date", DataType::Date32),
        nullable_field("wp_rec_end_date", DataType::Date32),
        nullable_field("wp_creation_date_sk", DataType::Int64),
        nullable_field("wp_access_date_sk", DataType::Int64),
        nullable_field("wp_autogen_flag", DataType::Utf8),
        nullable_field("wp_customer_sk", DataType::Int64),
        nullable_field("wp_url", DataType::Utf8),
        nullable_field("wp_type", DataType::Utf8),
        nullable_field("wp_char_count", DataType::Int32),
        nullable_field("wp_link_count", DataType::Int32),
        nullable_field("wp_image_count", DataType::Int32),
        nullable_field("wp_max_ad_count", DataType::Int32),
    ]))
}

#!/usr/bin/env python3
"""
TPC-DS Data Generator using PySpark.

This script generates TPC-DS benchmark data in Parquet format using
distributed Spark processing. It's designed to run on a Dataproc cluster.

Usage:
    spark-submit tpcds_datagen.py \
        --scale-factor 100 \
        --output-path gs://bucket/tpcds-data/100G \
        --format parquet \
        --compression snappy \
        --parallelism 32
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DecimalType, DateType, TimestampType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# TPC-DS table definitions with row counts per scale factor
# Format: (table_name, base_rows, scale_multiplier)
TPCDS_TABLES = [
    ("call_center", 6, 1),
    ("catalog_page", 11718, 1),
    ("catalog_returns", 144067, 1000),
    ("catalog_sales", 1441548, 1000),
    ("customer", 100000, 1000),
    ("customer_address", 50000, 1000),
    ("customer_demographics", 1920800, 1),
    ("date_dim", 73049, 1),
    ("household_demographics", 7200, 1),
    ("income_band", 20, 1),
    ("inventory", 11745000, 1000),
    ("item", 18000, 1000),
    ("promotion", 300, 1000),
    ("reason", 35, 1),
    ("ship_mode", 20, 1),
    ("store", 12, 1000),
    ("store_returns", 287514, 1000),
    ("store_sales", 2879987, 1000),
    ("time_dim", 86400, 1),
    ("warehouse", 5, 1000),
    ("web_page", 60, 1000),
    ("web_returns", 71763, 1000),
    ("web_sales", 719384, 1000),
    ("web_site", 30, 1000),
]

# Reference data for generating realistic values
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

CITIES = [
    "Midway", "Fairview", "Centerville", "Springfield", "Georgetown",
    "Franklin", "Clinton", "Madison", "Salem", "Pleasant Valley",
    "Oakland", "Riverside", "Greenville", "Bristol", "Lakewood"
]


class TPCDSDataGenerator:
    """Generates TPC-DS data using PySpark."""

    def __init__(self, spark: SparkSession, scale_factor: int,
                 output_path: str, data_format: str, compression: str,
                 parallelism: int):
        self.spark = spark
        self.scale_factor = scale_factor
        self.output_path = output_path.rstrip('/')
        self.data_format = data_format
        self.compression = compression
        self.parallelism = parallelism

        # Broadcast reference data
        self.states_bc = spark.sparkContext.broadcast(STATES)
        self.cities_bc = spark.sparkContext.broadcast(CITIES)

    def get_row_count(self, table_name: str) -> int:
        """Calculate row count for a table at the given scale factor."""
        for name, base_rows, multiplier in TPCDS_TABLES:
            if name == table_name:
                if multiplier == 1:
                    return base_rows
                return base_rows * self.scale_factor
        return 0

    def generate_date_dim(self) -> DataFrame:
        """Generate date_dim table."""
        logger.info("Generating date_dim...")

        # Generate date range from 1998-01-01 to 2003-12-31
        start_date = date(1998, 1, 1)
        dates = []
        for i in range(73049):  # ~20 years of dates
            d = start_date + timedelta(days=i)
            dates.append((
                i + 1,  # d_date_sk
                f"{d.year:04d}{d.month:02d}{d.day:02d}",  # d_date_id
                d,  # d_date
                d.month,  # d_month_seq
                d.isocalendar()[1],  # d_week_seq
                (d.year - 1998) * 4 + (d.month - 1) // 3,  # d_quarter_seq
                d.year,  # d_year
                1 if d.weekday() < 5 else 0,  # d_dow (0=Mon, 6=Sun)
                d.timetuple().tm_yday,  # d_moy (day of year)
                d.day,  # d_dom
                (d.month - 1) // 3 + 1,  # d_qoy
                0,  # d_fy_year
                0,  # d_fy_quarter_seq
                0,  # d_fy_week_seq
                d.strftime("%A"),  # d_day_name
                d.strftime("%B"),  # d_month_name (will be truncated)
                "N" if d.weekday() < 5 else "Y",  # d_holiday
                "N" if d.weekday() < 5 else "Y",  # d_weekend
                "Y" if d == date(d.year, d.month, 1) else "N",  # d_following_holiday
                "Y",  # d_first_dom
                "N",  # d_last_dom
                "Y" if d == date(d.year, d.month, 1) else "N",  # d_same_day_ly
                "Y" if d == date(d.year, d.month, 1) else "N",  # d_same_day_lq
                "N",  # d_current_day
                "Y" if d.month == 1 else "N",  # d_current_week
                "Y" if d.month == 1 else "N",  # d_current_month
                "Y" if d.month <= 3 else "N",  # d_current_quarter
                "Y" if d.year == 2002 else "N",  # d_current_year
            ))

        schema = StructType([
            StructField("d_date_sk", IntegerType(), False),
            StructField("d_date_id", StringType(), False),
            StructField("d_date", DateType(), True),
            StructField("d_month_seq", IntegerType(), True),
            StructField("d_week_seq", IntegerType(), True),
            StructField("d_quarter_seq", IntegerType(), True),
            StructField("d_year", IntegerType(), True),
            StructField("d_dow", IntegerType(), True),
            StructField("d_moy", IntegerType(), True),
            StructField("d_dom", IntegerType(), True),
            StructField("d_qoy", IntegerType(), True),
            StructField("d_fy_year", IntegerType(), True),
            StructField("d_fy_quarter_seq", IntegerType(), True),
            StructField("d_fy_week_seq", IntegerType(), True),
            StructField("d_day_name", StringType(), True),
            StructField("d_month_name", StringType(), True),
            StructField("d_holiday", StringType(), True),
            StructField("d_weekend", StringType(), True),
            StructField("d_following_holiday", StringType(), True),
            StructField("d_first_dom", StringType(), True),
            StructField("d_last_dom", StringType(), True),
            StructField("d_same_day_ly", StringType(), True),
            StructField("d_same_day_lq", StringType(), True),
            StructField("d_current_day", StringType(), True),
            StructField("d_current_week", StringType(), True),
            StructField("d_current_month", StringType(), True),
            StructField("d_current_quarter", StringType(), True),
            StructField("d_current_year", StringType(), True),
        ])

        return self.spark.createDataFrame(dates, schema)

    def generate_time_dim(self) -> DataFrame:
        """Generate time_dim table."""
        logger.info("Generating time_dim...")

        times = []
        for i in range(86400):  # seconds in a day
            hour = i // 3600
            minute = (i % 3600) // 60
            second = i % 60
            times.append((
                i,  # t_time_sk
                f"{hour:02d}:{minute:02d}:{second:02d}",  # t_time_id
                i,  # t_time
                hour,  # t_hour
                minute,  # t_minute
                second,  # t_second
                "AM" if hour < 12 else "PM",  # t_am_pm
                "first" if hour < 6 else ("second" if hour < 12 else ("third" if hour < 18 else "fourth")),  # t_shift
                "overnight" if hour < 6 else ("breakfast" if hour < 9 else ("morning" if hour < 12 else "afternoon")),  # t_sub_shift
                "dinner" if 17 <= hour <= 20 else "other",  # t_meal_time
            ))

        schema = StructType([
            StructField("t_time_sk", IntegerType(), False),
            StructField("t_time_id", StringType(), False),
            StructField("t_time", IntegerType(), True),
            StructField("t_hour", IntegerType(), True),
            StructField("t_minute", IntegerType(), True),
            StructField("t_second", IntegerType(), True),
            StructField("t_am_pm", StringType(), True),
            StructField("t_shift", StringType(), True),
            StructField("t_sub_shift", StringType(), True),
            StructField("t_meal_time", StringType(), True),
        ])

        return self.spark.createDataFrame(times, schema)

    def generate_customer(self) -> DataFrame:
        """Generate customer table using distributed generation."""
        logger.info(f"Generating customer ({self.get_row_count('customer')} rows)...")

        num_rows = self.get_row_count('customer')
        num_partitions = max(self.parallelism, num_rows // 100000)

        df = self.spark.range(1, num_rows + 1, numPartitions=num_partitions)

        return df.select(
            F.col("id").alias("c_customer_sk"),
            F.concat(F.lit("AAAAAAAAA"), F.lpad(F.col("id").cast("string"), 8, "0")).alias("c_customer_id"),
            (F.col("id") % 7200 + 1).alias("c_current_cdemo_sk"),
            (F.col("id") % 50000 + 1).alias("c_current_hdemo_sk"),
            (F.col("id") % 50000 + 1).alias("c_current_addr_sk"),
            F.lit(1).alias("c_first_shipto_date_sk"),
            F.lit(1).alias("c_first_sales_date_sk"),
            F.lit("M").alias("c_salutation"),
            F.concat(F.lit("FirstName"), (F.col("id") % 100).cast("string")).alias("c_first_name"),
            F.concat(F.lit("LastName"), (F.col("id") % 100).cast("string")).alias("c_last_name"),
            F.lit("N").alias("c_preferred_cust_flag"),
            F.lit(1998).alias("c_birth_day"),
            F.lit(1).alias("c_birth_month"),
            (1950 + F.col("id") % 50).alias("c_birth_year"),
            F.lit("USA").alias("c_birth_country"),
            F.lit("login").alias("c_login"),
            F.concat(F.lit("email"), F.col("id").cast("string"), F.lit("@example.com")).alias("c_email_address"),
            F.lit(0).alias("c_last_review_date_sk"),
        )

    def generate_customer_address(self) -> DataFrame:
        """Generate customer_address table."""
        logger.info(f"Generating customer_address ({self.get_row_count('customer_address')} rows)...")

        num_rows = self.get_row_count('customer_address')
        num_partitions = max(self.parallelism, num_rows // 100000)

        df = self.spark.range(1, num_rows + 1, numPartitions=num_partitions)

        states_expr = F.array([F.lit(s) for s in STATES])
        cities_expr = F.array([F.lit(c) for c in CITIES])

        return df.select(
            F.col("id").alias("ca_address_sk"),
            F.concat(F.lit("AAAAAAAAA"), F.lpad(F.col("id").cast("string"), 8, "0")).alias("ca_address_id"),
            F.concat(F.lit("Street "), F.col("id").cast("string")).alias("ca_street_number"),
            F.lit("Main St").alias("ca_street_name"),
            F.lit("Suite").alias("ca_street_type"),
            F.concat(F.lit("Apt "), (F.col("id") % 100).cast("string")).alias("ca_suite_number"),
            F.element_at(cities_expr, (F.col("id") % 15 + 1).cast("int")).alias("ca_city"),
            F.lit("County").alias("ca_county"),
            F.element_at(states_expr, (F.col("id") % 50 + 1).cast("int")).alias("ca_state"),
            F.concat(F.lit("12345")).alias("ca_zip"),
            F.lit("United States").alias("ca_country"),
            F.lit(-5.0).cast(DecimalType(5, 2)).alias("ca_gmt_offset"),
            F.lit("Residential").alias("ca_location_type"),
        )

    def generate_item(self) -> DataFrame:
        """Generate item table."""
        logger.info(f"Generating item ({self.get_row_count('item')} rows)...")

        num_rows = self.get_row_count('item')
        num_partitions = max(self.parallelism, num_rows // 10000)

        df = self.spark.range(1, num_rows + 1, numPartitions=num_partitions)

        categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Music", "Toys"]
        categories_expr = F.array([F.lit(c) for c in categories])

        return df.select(
            F.col("id").alias("i_item_sk"),
            F.concat(F.lit("AAAAAAAAA"), F.lpad(F.col("id").cast("string"), 8, "0")).alias("i_item_id"),
            F.lit(1).alias("i_rec_start_date"),
            F.lit(None).cast(DateType()).alias("i_rec_end_date"),
            F.concat(F.lit("Item "), F.col("id").cast("string")).alias("i_item_desc"),
            (F.rand() * 1000).cast(DecimalType(7, 2)).alias("i_current_price"),
            (F.rand() * 500).cast(DecimalType(7, 2)).alias("i_wholesale_cost"),
            (F.col("id") % 1000 + 1).alias("i_brand_id"),
            F.concat(F.lit("Brand "), (F.col("id") % 100).cast("string")).alias("i_brand"),
            (F.col("id") % 500 + 1).alias("i_class_id"),
            F.lit("Class").alias("i_class"),
            (F.col("id") % 7 + 1).alias("i_category_id"),
            F.element_at(categories_expr, (F.col("id") % 7 + 1).cast("int")).alias("i_category"),
            (F.col("id") % 100 + 1).alias("i_manufact_id"),
            F.lit("Manufacturer").alias("i_manufact"),
            F.lit("S").alias("i_size"),
            F.lit(None).cast(StringType()).alias("i_formulation"),
            F.lit("Red").alias("i_color"),
            F.lit("Each").alias("i_units"),
            F.lit("Box").alias("i_container"),
            (F.col("id") % 100 + 1).alias("i_manager_id"),
            F.concat(F.lit("Product "), F.col("id").cast("string")).alias("i_product_name"),
        )

    def generate_store_sales(self) -> DataFrame:
        """Generate store_sales table (largest fact table)."""
        logger.info(f"Generating store_sales ({self.get_row_count('store_sales')} rows)...")

        num_rows = self.get_row_count('store_sales')
        num_partitions = max(self.parallelism * 4, num_rows // 1000000)

        # Limit date and item dimensions
        max_date_sk = 73049
        max_item_sk = self.get_row_count('item')
        max_customer_sk = self.get_row_count('customer')
        max_store_sk = max(1, self.get_row_count('store'))
        max_promo_sk = max(1, self.get_row_count('promotion'))

        df = self.spark.range(1, num_rows + 1, numPartitions=num_partitions)

        return df.select(
            (F.col("id") % max_date_sk + 1).alias("ss_sold_date_sk"),
            (F.col("id") % 86400).alias("ss_sold_time_sk"),
            F.col("id").alias("ss_item_sk"),
            (F.col("id") % max_customer_sk + 1).alias("ss_customer_sk"),
            (F.col("id") % 7200 + 1).alias("ss_cdemo_sk"),
            (F.col("id") % 7200 + 1).alias("ss_hdemo_sk"),
            (F.col("id") % 50000 + 1).alias("ss_addr_sk"),
            (F.col("id") % max_store_sk + 1).alias("ss_store_sk"),
            (F.col("id") % max_promo_sk + 1).alias("ss_promo_sk"),
            F.col("id").alias("ss_ticket_number"),
            (F.rand() * 10 + 1).cast(IntegerType()).alias("ss_quantity"),
            (F.rand() * 100).cast(DecimalType(7, 2)).alias("ss_wholesale_cost"),
            (F.rand() * 200).cast(DecimalType(7, 2)).alias("ss_list_price"),
            (F.rand() * 150).cast(DecimalType(7, 2)).alias("ss_sales_price"),
            (F.rand() * 50).cast(DecimalType(7, 2)).alias("ss_ext_discount_amt"),
            (F.rand() * 200).cast(DecimalType(7, 2)).alias("ss_ext_sales_price"),
            (F.rand() * 100).cast(DecimalType(7, 2)).alias("ss_ext_wholesale_cost"),
            (F.rand() * 250).cast(DecimalType(7, 2)).alias("ss_ext_list_price"),
            (F.rand() * 20).cast(DecimalType(7, 2)).alias("ss_ext_tax"),
            (F.rand() * 10).cast(DecimalType(7, 2)).alias("ss_coupon_amt"),
            (F.rand() * 200).cast(DecimalType(7, 2)).alias("ss_net_paid"),
            (F.rand() * 220).cast(DecimalType(7, 2)).alias("ss_net_paid_inc_tax"),
            (F.rand() * 150).cast(DecimalType(7, 2)).alias("ss_net_profit"),
        )

    def generate_store(self) -> DataFrame:
        """Generate store table."""
        logger.info(f"Generating store ({self.get_row_count('store')} rows)...")

        num_rows = max(1, self.get_row_count('store'))

        df = self.spark.range(1, num_rows + 1)

        states_expr = F.array([F.lit(s) for s in STATES])
        cities_expr = F.array([F.lit(c) for c in CITIES])

        return df.select(
            F.col("id").alias("s_store_sk"),
            F.concat(F.lit("AAAAAAAAA"), F.lpad(F.col("id").cast("string"), 8, "0")).alias("s_store_id"),
            F.lit(1).alias("s_rec_start_date"),
            F.lit(None).cast(DateType()).alias("s_rec_end_date"),
            F.lit(1).alias("s_closed_date_sk"),
            F.concat(F.lit("Store "), F.col("id").cast("string")).alias("s_store_name"),
            (F.col("id") % 10).alias("s_number_employees"),
            (F.col("id") % 3).alias("s_floor_space"),
            F.lit("24").alias("s_hours"),
            F.concat(F.lit("Manager "), F.col("id").cast("string")).alias("s_manager"),
            (F.col("id") % 10).alias("s_market_id"),
            F.lit("Market").alias("s_geography_class"),
            F.lit("Description").alias("s_market_desc"),
            F.concat(F.lit("Manager "), F.col("id").cast("string")).alias("s_market_manager"),
            (F.col("id") % 10).alias("s_division_id"),
            F.lit("Division").alias("s_division_name"),
            (F.col("id") % 5).alias("s_company_id"),
            F.lit("Company").alias("s_company_name"),
            F.concat(F.lit("Street "), F.col("id").cast("string")).alias("s_street_number"),
            F.lit("Main St").alias("s_street_name"),
            F.lit("Ave").alias("s_street_type"),
            F.lit("Suite 1").alias("s_suite_number"),
            F.element_at(cities_expr, (F.col("id") % 15 + 1).cast("int")).alias("s_city"),
            F.lit("County").alias("s_county"),
            F.element_at(states_expr, (F.col("id") % 50 + 1).cast("int")).alias("s_state"),
            F.lit("12345").alias("s_zip"),
            F.lit("United States").alias("s_country"),
            F.lit(-5.0).cast(DecimalType(5, 2)).alias("s_gmt_offset"),
            F.lit(0.08).cast(DecimalType(5, 2)).alias("s_tax_percentage"),
        )

    def write_table(self, df: DataFrame, table_name: str) -> None:
        """Write a DataFrame to the output path."""
        output = f"{self.output_path}/{table_name}"
        logger.info(f"Writing {table_name} to {output}...")

        writer = df.write.mode("overwrite")

        if self.data_format == "parquet":
            writer.option("compression", self.compression).parquet(output)
        elif self.data_format == "orc":
            writer.option("compression", self.compression).orc(output)
        else:
            raise ValueError(f"Unsupported format: {self.data_format}")

        logger.info(f"Finished writing {table_name}")

    def generate_all(self) -> None:
        """Generate all TPC-DS tables."""
        logger.info(f"Starting TPC-DS data generation: SF={self.scale_factor}, "
                   f"output={self.output_path}, format={self.data_format}")

        # Generate dimension tables
        self.write_table(self.generate_date_dim(), "date_dim")
        self.write_table(self.generate_time_dim(), "time_dim")
        self.write_table(self.generate_customer(), "customer")
        self.write_table(self.generate_customer_address(), "customer_address")
        self.write_table(self.generate_item(), "item")
        self.write_table(self.generate_store(), "store")

        # Generate fact table (store_sales is the largest)
        self.write_table(self.generate_store_sales(), "store_sales")

        logger.info("TPC-DS data generation complete!")


def main():
    parser = argparse.ArgumentParser(description="TPC-DS Data Generator")
    parser.add_argument("--scale-factor", type=int, required=True,
                       help="TPC-DS scale factor (GB)")
    parser.add_argument("--output-path", type=str, required=True,
                       help="Output path (GCS or local)")
    parser.add_argument("--format", type=str, default="parquet",
                       choices=["parquet", "orc"],
                       help="Output format")
    parser.add_argument("--compression", type=str, default="snappy",
                       choices=["snappy", "zstd", "lz4", "gzip", "none"],
                       help="Compression codec")
    parser.add_argument("--parallelism", type=int, default=0,
                       help="Parallelism level (0 = auto)")

    args = parser.parse_args()

    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"TPC-DS Data Generator SF={args.scale_factor}") \
        .getOrCreate()

    # Set parallelism
    parallelism = args.parallelism
    if parallelism == 0:
        parallelism = spark.sparkContext.defaultParallelism * 2

    compression = args.compression if args.compression != "none" else "uncompressed"

    try:
        generator = TPCDSDataGenerator(
            spark=spark,
            scale_factor=args.scale_factor,
            output_path=args.output_path,
            data_format=args.format,
            compression=compression,
            parallelism=parallelism,
        )
        generator.generate_all()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

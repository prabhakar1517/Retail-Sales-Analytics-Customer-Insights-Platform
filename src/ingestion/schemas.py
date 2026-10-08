"""
src/ingestion/schemas.py
------------------------
Explicit StructType schemas for Bronze Layer ingestion:
- Sales CSV schema
- Returns CSV schema
- Oracle CRM Customers schema
- MySQL Master schemas (Products, Stores, Suppliers, Promotions)

Rule: STRICTLY NO inferSchema. All fields typed with StructField.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)

# 1. Sales CSV Schema
SALES_RAW_SCHEMA = StructType([
    StructField("sale_id", StringType(), True),
    StructField("sale_date", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("promo_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("payment_mode", StringType(), True),
])

# 2. Returns CSV Schema
RETURNS_RAW_SCHEMA = StructType([
    StructField("return_id", StringType(), True),
    StructField("sale_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", StringType(), True),
])

# 3. Oracle CRM Customers Schema
ORACLE_CUSTOMERS_SCHEMA = StructType([
    StructField("CUSTOMER_ID", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("SEGMENT", StringType(), True),
    StructField("LOYALTY_TIER", StringType(), True),
    StructField("CREATED_AT", TimestampType(), True),
    StructField("UPDATED_AT", TimestampType(), True),
])

# 4. MySQL Suppliers Schema
MYSQL_SUPPLIERS_SCHEMA = StructType([
    StructField("supplier_id", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("is_active", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
])

# 5. MySQL Products Schema
MYSQL_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("is_active", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
])

# 6. MySQL Stores Schema
MYSQL_STORES_SCHEMA = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
    StructField("manager_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("opened_date", DateType(), True),
    StructField("is_active", IntegerType(), True),
])

# 7. MySQL Promotions Schema
MYSQL_PROMOTIONS_SCHEMA = StructType([
    StructField("promo_id", StringType(), True),
    StructField("promo_name", StringType(), True),
    StructField("promo_type", StringType(), True),
    StructField("discount_type", StringType(), True),
    StructField("discount_value", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("min_order_amount", DoubleType(), True),
    StructField("is_active", IntegerType(), True),
])

-- =============================================================================
-- Amazon Redshift Star Schema DDL
-- Schema: retail_dw
-- DISTKEY and SORTKEY strategies selected for high-throughput OLAP analytics
-- Data types aligned with Parquet binary encodings
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS retail_dw;

-- 1. dim_date (DISTSTYLE ALL because date is joined by every query)
CREATE TABLE IF NOT EXISTS retail_dw.dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    month_year VARCHAR(20) NOT NULL,
    quarter_number INT NOT NULL,
    quarter_name VARCHAR(10) NOT NULL,
    year_number INT NOT NULL,
    is_weekend INT NOT NULL,
    is_holiday INT NOT NULL,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

-- 2. dim_product (DISTSTYLE ALL for small dimension lookup)
CREATE TABLE IF NOT EXISTS retail_dw.dim_product (
    product_key INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(150),
    supplier_id VARCHAR(50),
    unit_price DOUBLE PRECISION,
    cost_price DOUBLE PRECISION,
    reorder_level INT,
    is_active BOOLEAN,
    PRIMARY KEY (product_key)
)
DISTSTYLE ALL
SORTKEY (product_key);

-- 3. dim_store (DISTSTYLE ALL)
CREATE TABLE IF NOT EXISTS retail_dw.dim_store (
    store_key INT NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(255),
    store_type VARCHAR(50),
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(50),
    manager_name VARCHAR(150),
    phone VARCHAR(50),
    square_footage INT,
    opened_date DATE,
    is_active BOOLEAN,
    PRIMARY KEY (store_key)
)
DISTSTYLE ALL
SORTKEY (store_key);

-- 4. dim_supplier (DISTSTYLE ALL)
CREATE TABLE IF NOT EXISTS retail_dw.dim_supplier (
    supplier_key INT NOT NULL,
    supplier_id VARCHAR(50) NOT NULL,
    supplier_name VARCHAR(255),
    contact_name VARCHAR(150),
    email VARCHAR(150),
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(50),
    lead_time_days INT,
    is_active BOOLEAN,
    PRIMARY KEY (supplier_key)
)
DISTSTYLE ALL
SORTKEY (supplier_key);

-- 5. dim_promotion (DISTSTYLE ALL)
CREATE TABLE IF NOT EXISTS retail_dw.dim_promotion (
    promo_key INT NOT NULL,
    promo_id VARCHAR(50) NOT NULL,
    promo_name VARCHAR(255),
    promo_type VARCHAR(50),
    discount_type VARCHAR(50),
    discount_value DOUBLE PRECISION,
    start_date DATE,
    end_date DATE,
    min_order_amount NUMERIC(10, 2),
    is_active BOOLEAN,
    PRIMARY KEY (promo_key)
)
DISTSTYLE ALL
SORTKEY (promo_key);

-- 6. dim_customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS retail_dw.dim_customer (
    customer_key INT NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    name VARCHAR(150),
    email VARCHAR(150),
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    segment VARCHAR(50),
    loyalty_tier VARCHAR(50),
    effective_date TIMESTAMP NOT NULL,
    expiry_date TIMESTAMP NOT NULL,
    is_current INT NOT NULL,
    PRIMARY KEY (customer_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (customer_id, effective_date);

-- 7. fact_sales (Large fact table: DISTKEY product_key, SORTKEY date_key)
CREATE TABLE IF NOT EXISTS retail_dw.fact_sales (
    sale_id VARCHAR(100) NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    promo_key INT NOT NULL,
    customer_key INT NOT NULL,
    sale_date TIMESTAMP NOT NULL,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    store_id VARCHAR(50),
    quantity INT NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL,
    gross_amount DOUBLE PRECISION NOT NULL,
    discount DOUBLE PRECISION NOT NULL,
    net_amount DOUBLE PRECISION NOT NULL,
    payment_mode VARCHAR(50),
    date_key INT NOT NULL,
    PRIMARY KEY (sale_id)
)
DISTSTYLE KEY
DISTKEY (product_key)
SORTKEY (date_key);

-- 8. fact_returns
CREATE TABLE IF NOT EXISTS retail_dw.fact_returns (
    return_id VARCHAR(100) NOT NULL,
    sale_id VARCHAR(100) NOT NULL,
    product_key INT NOT NULL,
    return_date TIMESTAMP NOT NULL,
    product_id VARCHAR(50),
    quantity INT NOT NULL,
    refund_amount DOUBLE PRECISION NOT NULL,
    reason VARCHAR(100),
    return_date_key INT NOT NULL,
    PRIMARY KEY (return_id)
)
DISTSTYLE KEY
DISTKEY (product_key)
SORTKEY (return_date_key);

-- 9. customer_rfm (Analytical Mart)
CREATE TABLE IF NOT EXISTS retail_dw.customer_rfm (
    customer_id VARCHAR(50) NOT NULL,
    last_order_date TIMESTAMP,
    frequency BIGINT NOT NULL,
    monetary DOUBLE PRECISION NOT NULL,
    recency INT NOT NULL,
    r_score INT NOT NULL,
    f_score INT NOT NULL,
    m_score INT NOT NULL,
    rfm_score VARCHAR(10) NOT NULL,
    rfm_segment VARCHAR(50) NOT NULL,
    _calculated_at TIMESTAMP,
    PRIMARY KEY (customer_id)
)
DISTSTYLE ALL
SORTKEY (r_score, f_score, m_score);

-- 10. etl_file_control (Operational ETL Control & Audit Log)
CREATE TABLE IF NOT EXISTS retail_dw.etl_file_control (
    batch_id VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    s3_path VARCHAR(500),
    rows_loaded INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id, target_table)
)
DISTSTYLE ALL
SORTKEY (executed_at);

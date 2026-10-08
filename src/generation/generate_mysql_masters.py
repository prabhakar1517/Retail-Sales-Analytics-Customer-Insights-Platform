"""
src/generation/generate_mysql_masters.py
----------------------------------------
Generates synthetic master data for MySQL tables:
- suppliers
- products
- stores
- promotions

Uses Faker with a fixed seed (42) for 100% reproducibility.
Deliberate defects (2-5%) injected according to specifications.
Can write to CSV in data/generated/masters/ and/or directly load to MySQL.
"""
import os
import random
import csv
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple

from faker import Faker
import mysql.connector

from src.utils.config_loader import get_config, get_path
from src.utils.logger import get_logger

logger = get_logger("generation.mysql_masters")

# Fixed categories, brands, store types, and channels
PRODUCT_CATEGORIES = [
    "Home & Kitchen", "Gifts & Novelties", "Stationery", "Seasonal & Holiday",
    "Toys & Games", "Garden & Outdoor", "Apparel & Accessories", "Bags & Luggage"
]

STORE_TYPES = ["Flagship", "Outlet", "High Street", "Mall", "Airport"]
REGIONS = ["Greater London", "South East", "North West", "Midlands", "Scotland", "Wales"]
DISCOUNT_TYPES = ["PERCENTAGE", "FLAT_AMOUNT", "BOGO"]


def get_faker() -> Faker:
    cfg = get_config()
    seed = cfg.get("generation", {}).get("faker_seed", 42)
    fake = Faker("en_GB")
    Faker.seed(seed)
    random.seed(seed)
    return fake


def generate_suppliers(num_suppliers: int = 100) -> List[Dict[str, Any]]:
    fake = get_faker()
    suppliers = []
    for i in range(1, num_suppliers + 1):
        supplier_id = f"SUP-{i:04d}"
        company = fake.company()
        suppliers.append({
            "supplier_id": supplier_id,
            "supplier_name": company,
            "contact_name": fake.name(),
            "email": fake.company_email(),
            "phone": fake.phone_number(),
            "address": fake.street_address().replace("\n", ", "),
            "city": fake.city(),
            "country": "United Kingdom",
            "lead_time_days": random.randint(3, 30),
            "is_active": 1 if random.random() > 0.05 else 0,
            "created_at": fake.date_time_between(start_date="-3y", end_date="-2y").isoformat()
        })
    return suppliers


def extract_raw_products(raw_csv_path: Path, max_products: int = 500, suppliers: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Extract distinct products from real online_retail_II.csv and enrich with synthetic attributes."""
    fake = get_faker()
    seen_codes = set()
    products = []
    supplier_ids = [s["supplier_id"] for s in suppliers] if suppliers else [f"SUP-{i:04d}" for i in range(1, 101)]

    if raw_csv_path.exists():
        with open(raw_csv_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("StockCode", "").strip()
                desc = row.get("Description", "").strip()
                try:
                    price = float(row.get("Price", "0"))
                except ValueError:
                    price = 0.0

                if code and not code.startswith("C") and code not in seen_codes and len(code) >= 3 and price > 0:
                    seen_codes.add(code)
                    cost = round(max(0.10, price * random.uniform(0.40, 0.70)), 2)
                    category = random.choice(PRODUCT_CATEGORIES)
                    supplier_id = random.choice(supplier_ids)
                    
                    products.append({
                        "product_id": code,
                        "product_name": desc if desc else f"Product {code}",
                        "category": category,
                        "brand": fake.company(),
                        "supplier_id": supplier_id,
                        "unit_price": round(price, 2),
                        "cost_price": cost,
                        "reorder_level": random.randint(10, 100),
                        "is_active": 1,
                        "created_at": "2009-11-01 00:00:00"
                    })
                if len(products) >= max_products:
                    break

    # If fewer products found than requested, fill with synthetic
    while len(products) < max_products:
        idx = len(products) + 1
        code = f"PROD{idx:05d}"
        price = round(random.uniform(1.50, 45.00), 2)
        cost = round(price * random.uniform(0.45, 0.70), 2)
        products.append({
            "product_id": code,
            "product_name": fake.catch_phrase(),
            "category": random.choice(PRODUCT_CATEGORIES),
            "brand": fake.company(),
            "supplier_id": random.choice(supplier_ids),
            "unit_price": price,
            "cost_price": cost,
            "reorder_level": random.randint(10, 100),
            "is_active": 1,
            "created_at": "2009-11-01 00:00:00"
        })

    return products


def generate_stores(num_stores: int = 50) -> List[Dict[str, Any]]:
    fake = get_faker()
    stores = []
    for i in range(1, num_stores + 1):
        store_id = f"STR-{i:03d}"
        city = fake.city()
        stores.append({
            "store_id": store_id,
            "store_name": f"{city} {random.choice(STORE_TYPES)} Store",
            "store_type": random.choice(STORE_TYPES),
            "city": city,
            "region": random.choice(REGIONS),
            "country": "United Kingdom",
            "manager_name": fake.name(),
            "phone": fake.phone_number(),
            "square_footage": random.randint(1200, 15000),
            "opened_date": fake.date_between(start_date="-5y", end_date="-2y").isoformat(),
            "is_active": 1
        })
    return stores


def generate_promotions(num_promos: int = 200) -> List[Dict[str, Any]]:
    fake = get_faker()
    promos = []
    # Always include standard NO_PROMO promotion
    promos.append({
        "promo_id": "PRM-0000",
        "promo_name": "Standard Everyday Price",
        "promo_type": "NONE",
        "discount_type": "PERCENTAGE",
        "discount_value": 0.0,
        "start_date": "2009-01-01",
        "end_date": "2012-12-31",
        "min_order_amount": 0.0,
        "is_active": 1
    })

    start_base = date(2009, 11, 1)
    for i in range(1, num_promos):
        p_id = f"PRM-{i:04d}"
        p_start = start_base + timedelta(days=random.randint(0, 700))
        duration = random.randint(3, 30)
        p_end = p_start + timedelta(days=duration)
        disc_type = random.choice(DISCOUNT_TYPES)
        disc_val = round(random.uniform(5.0, 30.0), 2) if disc_type == "PERCENTAGE" else round(random.uniform(1.0, 10.0), 2)

        promos.append({
            "promo_id": p_id,
            "promo_name": f"{fake.word().capitalize()} Special Sale",
            "promo_type": random.choice(["SEASONAL", "CLEARANCE", "HOLIDAY", "LOYALTY"]),
            "discount_type": disc_type,
            "discount_value": disc_val,
            "start_date": p_start.isoformat(),
            "end_date": p_end.isoformat(),
            "min_order_amount": round(random.choice([0.0, 15.0, 25.0, 50.0]), 2),
            "is_active": 1
        })
    return promos


def inject_master_defects(
    suppliers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    stores: List[Dict[str, Any]],
    promotions: List[Dict[str, Any]],
    defect_rate: float = 0.03
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Inject 2-5% deliberate defects (null IDs, negative costs/prices, invalid dates, inactive records)."""
    # 1. Product defects: negative prices, null supplier, extreme costs
    for p in products:
        if random.random() < defect_rate:
            choice = random.choice(["neg_price", "null_supplier", "high_cost"])
            if choice == "neg_price":
                p["unit_price"] = -p["unit_price"]
            elif choice == "null_supplier":
                p["supplier_id"] = "SUP-9999"  # Non-existent supplier FK defect
            elif choice == "high_cost":
                p["cost_price"] = round(p["unit_price"] * 2.5, 2)  # cost > price anomaly

    # 2. Store defects
    for s in stores:
        if random.random() < defect_rate:
            s["square_footage"] = -abs(s["square_footage"])

    # 3. Promotion defects: end date earlier than start date
    for pr in promotions:
        if pr["promo_id"] != "PRM-0000" and random.random() < defect_rate:
            pr["end_date"] = "2008-01-01"  # Invalid end date defect

    return suppliers, products, stores, promotions


def write_csv(data: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not data:
        return
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
    logger.info("Saved CSV: %s (%d rows)", output_path, len(data))


def load_to_mysql(
    suppliers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    stores: List[Dict[str, Any]],
    promotions: List[Dict[str, Any]]
) -> None:
    """Connect to MySQL and create/populate master tables."""
    cfg = get_config()
    db_cfg = cfg["mysql"]
    
    conn = mysql.connector.connect(
        host=db_cfg["host"],
        port=int(db_cfg["port"]),
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        autocommit=True
    )
    cursor = conn.cursor()

    # DDL
    ddl_suppliers = """
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id VARCHAR(50) PRIMARY KEY,
        supplier_name VARCHAR(255) NOT NULL,
        contact_name VARCHAR(150),
        email VARCHAR(150),
        phone VARCHAR(50),
        address VARCHAR(255),
        city VARCHAR(100),
        country VARCHAR(50),
        lead_time_days INT,
        is_active TINYINT(1) DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    """

    ddl_products = """
    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(50) PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        category VARCHAR(100),
        brand VARCHAR(150),
        supplier_id VARCHAR(50),
        unit_price DECIMAL(10, 2),
        cost_price DECIMAL(10, 2),
        reorder_level INT,
        is_active TINYINT(1) DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    """

    ddl_stores = """
    CREATE TABLE IF NOT EXISTS stores (
        store_id VARCHAR(50) PRIMARY KEY,
        store_name VARCHAR(255) NOT NULL,
        store_type VARCHAR(50),
        city VARCHAR(100),
        region VARCHAR(100),
        country VARCHAR(50),
        manager_name VARCHAR(150),
        phone VARCHAR(50),
        square_footage INT,
        opened_date DATE,
        is_active TINYINT(1) DEFAULT 1
    ) ENGINE=InnoDB;
    """

    ddl_promotions = """
    CREATE TABLE IF NOT EXISTS promotions (
        promo_id VARCHAR(50) PRIMARY KEY,
        promo_name VARCHAR(255) NOT NULL,
        promo_type VARCHAR(50),
        discount_type VARCHAR(50),
        discount_value DECIMAL(10, 2),
        start_date DATE,
        end_date DATE,
        min_order_amount DECIMAL(10, 2),
        is_active TINYINT(1) DEFAULT 1
    ) ENGINE=InnoDB;
    """

    for ddl in [ddl_suppliers, ddl_products, ddl_stores, ddl_promotions]:
        cursor.execute(ddl)

    # Insert helper
    def batch_upsert(table: str, rows: List[Dict[str, Any]], pk: str):
        if not rows:
            return
        cols = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join([f"{c}=VALUES({c})" for c in cols if c != pk])
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        values = [[r[c] for c in cols] for r in rows]
        cursor.executemany(sql, values)
        logger.info("Upserted %d records into MySQL table: %s", len(rows), table)

    batch_upsert("suppliers", suppliers, "supplier_id")
    batch_upsert("products", products, "product_id")
    batch_upsert("stores", stores, "store_id")
    batch_upsert("promotions", promotions, "promo_id")

    cursor.close()
    conn.close()
    logger.info("MySQL master tables loaded successfully.")


def run_generate_mysql_masters(load_db: bool = True) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    cfg = get_config()
    raw_path = get_path("raw_data")
    dest_dir = get_path("generated") / "masters"
    dest_dir.mkdir(parents=True, exist_ok=True)

    gen_cfg = cfg.get("generation", {})
    num_suppliers = gen_cfg.get("num_suppliers", 100)
    num_products = gen_cfg.get("num_products", 500)
    num_stores = gen_cfg.get("num_stores", 50)
    num_promotions = gen_cfg.get("num_promotions", 200)
    defect_rate = gen_cfg.get("defect_rate", 0.03)

    logger.info("Generating synthetic MySQL master data (seed=%d)...", gen_cfg.get("faker_seed", 42))
    suppliers = generate_suppliers(num_suppliers)
    products = extract_raw_products(raw_path, num_products, suppliers)
    stores = generate_stores(num_stores)
    promotions = generate_promotions(num_promotions)

    suppliers, products, stores, promotions = inject_master_defects(
        suppliers, products, stores, promotions, defect_rate
    )

    write_csv(suppliers, dest_dir / "suppliers.csv")
    write_csv(products, dest_dir / "products.csv")
    write_csv(stores, dest_dir / "stores.csv")
    write_csv(promotions, dest_dir / "promotions.csv")

    if load_db:
        try:
            load_to_mysql(suppliers, products, stores, promotions)
        except Exception as e:
            logger.warning("Could not directly load into MySQL DB: %s (CSV backups written)", e)

    return suppliers, products, stores, promotions


if __name__ == "__main__":
    run_generate_mysql_masters(load_db=True)

"""
src/generation/generate_oracle_crm.py
-------------------------------------
Generates synthetic CRM.CUSTOMERS data for Oracle XE database.
Uses distinct Customer IDs extracted from the real Online Retail II CSV,
enriched with synthetic customer profiles (Faker seed 42):
- customer_id (integer / string formatted)
- name, email, phone, city, country, segment, loyalty_tier
- created_at, updated_at (watermark tracking column)

Injects 2-5% deliberate defects:
- Duplicate customers with slight spelling variations
- Malformed emails
- Null fields
- Inconsistent loyalty tiers

Saves CSV backup to data/generated/masters/crm_customers.csv and populates Oracle XE table CRM.CUSTOMERS.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple

from faker import Faker
import oracledb

from src.utils.config_loader import get_config, get_path
from src.utils.logger import get_logger

logger = get_logger("generation.oracle_crm")

SEGMENTS = ["Consumer", "Corporate", "Small Business", "Wholesale"]
LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]


def get_faker() -> Faker:
    cfg = get_config()
    seed = cfg.get("generation", {}).get("faker_seed", 42)
    fake = Faker("en_GB")
    Faker.seed(seed)
    random.seed(seed)
    return fake


def extract_customer_ids(raw_csv_path: Path, max_customers: int = 5000) -> List[str]:
    """Extract unique non-null Customer IDs from Online Retail II dataset."""
    seen = set()
    cust_ids = []
    if raw_csv_path.exists():
        with open(raw_csv_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row.get("Customer ID", "").strip()
                if cid:
                    if cid.endswith(".0"):
                        cid = cid[:-2]
                    if cid not in seen:
                        seen.add(cid)
                        cust_ids.append(cid)
                if len(cust_ids) >= max_customers:
                    break
    
    # Fill up if fewer than max_customers
    curr_id = 19000
    while len(cust_ids) < max_customers:
        cid = str(curr_id)
        if cid not in seen:
            seen.add(cid)
            cust_ids.append(cid)
        curr_id += 1

    return cust_ids


def generate_crm_customers(cust_ids: List[str], defect_rate: float = 0.03) -> List[Dict[str, Any]]:
    fake = get_faker()
    customers = []
    base_created = datetime(2009, 1, 1)

    for cid in cust_ids:
        c_time = base_created + timedelta(days=random.randint(0, 300))
        # Updated timestamp within 18 months of creation (for incremental watermark)
        u_time = c_time + timedelta(days=random.randint(0, 500))
        name = fake.name()
        email = f"{name.lower().replace(' ', '.')}.{random.randint(10, 99)}@{fake.free_email_domain()}"

        customers.append({
            "customer_id": cid,
            "name": name,
            "email": email,
            "phone": fake.phone_number(),
            "city": fake.city(),
            "country": "United Kingdom" if random.random() < 0.92 else fake.country(),
            "segment": random.choice(SEGMENTS),
            "loyalty_tier": random.choice(LOYALTY_TIERS),
            "created_at": c_time.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": u_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    # Deliberate defect injection (2-5%)
    # 1. Malformed emails and null phones
    # 2. Duplicate customer IDs with modified spelling/details
    duplicate_records = []
    for c in customers:
        if random.random() < defect_rate:
            choice = random.choice(["bad_email", "null_city", "duplicate_spelling"])
            if choice == "bad_email":
                c["email"] = c["email"].replace("@", "_at_")  # Invalid email format
            elif choice == "null_city":
                c["city"] = None
            elif choice == "duplicate_spelling":
                # Create a duplicate customer record with slight typo in name
                dup = dict(c)
                dup["name"] = dup["name"] + " Jr."
                dup["updated_at"] = (datetime.strptime(c["updated_at"], "%Y-%m-%d %H:%M:%S") + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
                duplicate_records.append(dup)

    logger.info("Generated %d base CRM customers and %d duplicate/altered records.", len(customers), len(duplicate_records))
    return customers + duplicate_records


def write_csv(data: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not data:
        return
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
    logger.info("Saved CRM CSV: %s (%d rows)", output_path, len(data))


def load_to_oracle(customers: List[Dict[str, Any]]) -> None:
    """Connect to Oracle XE using python-oracledb and populate CRM.CUSTOMERS."""
    cfg = get_config()
    ora_cfg = cfg["oracle"]

    dsn = f"{ora_cfg['host']}:{ora_cfg['port']}/{ora_cfg['service']}"
    
    # Connect with configured user or SYS
    conn = oracledb.connect(
        user=ora_cfg["user"],
        password=ora_cfg["password"],
        dsn=dsn
    )
    cursor = conn.cursor()

    # DDL
    ddl = """
    BEGIN
        EXECUTE IMMEDIATE '
        CREATE TABLE CUSTOMERS (
            customer_id VARCHAR2(50),
            name VARCHAR2(150),
            email VARCHAR2(150),
            phone VARCHAR2(50),
            city VARCHAR2(100),
            country VARCHAR2(100),
            segment VARCHAR2(50),
            loyalty_tier VARCHAR2(50),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
                RAISE;
            END IF;
    END;
    """
    cursor.execute(ddl)

    # Clean existing
    cursor.execute("TRUNCATE TABLE CUSTOMERS")

    sql_insert = """
    INSERT INTO CUSTOMERS (
        customer_id, name, email, phone, city, country, segment, loyalty_tier, created_at, updated_at
    ) VALUES (
        :customer_id, :name, :email, :phone, :city, :country, :segment, :loyalty_tier,
        TO_TIMESTAMP(:created_at, 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP(:updated_at, 'YYYY-MM-DD HH24:MI:SS')
    )
    """

    data_tuples = [
        {
            "customer_id": c["customer_id"],
            "name": c["name"],
            "email": c["email"],
            "phone": c["phone"] or "",
            "city": c["city"] or "",
            "country": c["country"],
            "segment": c["segment"],
            "loyalty_tier": c["loyalty_tier"],
            "created_at": c["created_at"],
            "updated_at": c["updated_at"]
        }
        for c in customers
    ]

    cursor.executemany(sql_insert, data_tuples)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded %d customer records into Oracle CRM.CUSTOMERS table successfully.", len(customers))


def run_generate_oracle_crm(load_db: bool = True) -> List[Dict[str, Any]]:
    cfg = get_config()
    raw_path = get_path("raw_data")
    dest_path = get_path("generated") / "masters" / "crm_customers.csv"

    gen_cfg = cfg.get("generation", {})
    num_customers = gen_cfg.get("num_customers", 5000)
    defect_rate = gen_cfg.get("defect_rate", 0.03)

    logger.info("Extracting customer IDs and generating CRM attributes (seed=%d)...", gen_cfg.get("faker_seed", 42))
    cust_ids = extract_customer_ids(raw_path, num_customers)
    customers = generate_crm_customers(cust_ids, defect_rate)

    write_csv(customers, dest_path)

    if load_db:
        try:
            load_to_oracle(customers)
        except Exception as e:
            logger.warning("Could not directly load into Oracle XE DB: %s (CSV backups written)", e)

    return customers


if __name__ == "__main__":
    run_generate_oracle_crm(load_db=True)

"""
src/generation/generate_daily_csv.py
------------------------------------
Processes the raw Online Retail II dataset (1,067,371 rows) and partitions it
into daily sales and returns feeds with realistic synthetic enrichment:
- Daily Sales CSV: data/generated/sales/sales_YYYYMMDD.csv
    Columns: sale_id, sale_date, store_id, product_id, customer_id, promo_id, quantity, unit_price, discount, payment_mode
- Daily Returns CSV: data/generated/returns/returns_YYYYMMDD.csv
    Columns: return_id, sale_id, return_date, product_id, quantity, reason, refund_amount

Injects 2-5% deliberate defects:
- Duplicate sale_ids
- Missing/Null store_id, product_id, sale_id
- Negative or zero quantities and unit_prices on regular sales
- Non-existent product/store IDs
- Unparseable date formats

Supports --sample flag (default 50,000 rows) for rapid local testing on laptop.
"""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.utils.config_loader import get_config, get_path
from src.utils.logger import get_logger

logger = get_logger("generation.daily_csv")

RETURN_REASONS = [
    "DEFECTIVE_ITEM", "WRONG_ITEM_SENT", "CUSTOMER_CHANGED_MIND",
    "DAMAGED_IN_TRANSIT", "NOT_AS_DESCRIBED", "ORDERED_BY_MISTAKE"
]

PAYMENT_MODES = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "STORE_CREDIT"]


def parse_args():
    parser = argparse.ArgumentParser(description="Partition raw retail data into daily sales and returns CSV feeds")
    parser.add_argument("--sample", action="store_true", help="Run on a sample of rows for rapid dev testing")
    parser.add_argument("--sample-rows", type=int, default=50000, help="Number of rows to sample if --sample is set")
    parser.add_argument("--max-days", type=int, default=None, help="Limit number of output days generated (for fast tests)")
    return parser.parse_args()


def load_master_keys() -> tuple[List[str], List[str], List[str]]:
    """Load valid store_ids, product_ids, and promo_ids from generated master CSVs."""
    masters_dir = get_path("generated") / "masters"
    store_ids = []
    product_ids = []
    promo_ids = ["PRM-0000"]

    if (masters_dir / "stores.csv").exists():
        with open(masters_dir / "stores.csv", mode="r", encoding="utf-8") as f:
            store_ids = [row["store_id"] for row in csv.DictReader(f) if row.get("store_id")]

    if (masters_dir / "products.csv").exists():
        with open(masters_dir / "products.csv", mode="r", encoding="utf-8") as f:
            product_ids = [row["product_id"] for row in csv.DictReader(f) if row.get("product_id")]

    if (masters_dir / "promotions.csv").exists():
        with open(masters_dir / "promotions.csv", mode="r", encoding="utf-8") as f:
            promo_ids = [row["promo_id"] for row in csv.DictReader(f) if row.get("promo_id")]

    # Defaults if masters not yet generated
    if not store_ids:
        store_ids = [f"STR-{i:03d}" for i in range(1, 51)]
    if not product_ids:
        product_ids = [f"PROD{i:05d}" for i in range(1, 501)]

    return store_ids, product_ids, promo_ids


def run_generate_daily_csv(sample: bool = False, sample_rows: int = 50000, max_days: Optional[int] = None):
    cfg = get_config()
    seed = cfg.get("generation", {}).get("faker_seed", 42)
    defect_rate = cfg.get("generation", {}).get("defect_rate", 0.03)
    random.seed(seed)

    raw_csv_path = get_path("raw_data")
    sales_out_dir = get_path("generated_sales")
    returns_out_dir = get_path("generated_returns")

    sales_out_dir.mkdir(parents=True, exist_ok=True)
    returns_out_dir.mkdir(parents=True, exist_ok=True)

    store_ids, product_ids, promo_ids = load_master_keys()

    logger.info("Starting daily CSV partition from %s (sample=%s)...", raw_csv_path, sample)

    # Buffers partitioned by date string: YYYYMMDD -> list of rows
    sales_by_date: Dict[str, List[Dict[str, Any]]] = {}
    returns_by_date: Dict[str, List[Dict[str, Any]]] = {}

    sale_counter = 1
    return_counter = 1
    total_processed = 0

    with open(raw_csv_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_processed += 1
            if sample and total_processed > sample_rows:
                break

            invoice = row.get("Invoice", "").strip()
            stock_code = row.get("StockCode", "").strip()
            quantity_str = row.get("Quantity", "0").strip()
            price_str = row.get("Price", "0").strip()
            customer_str = row.get("Customer ID", "").strip()
            date_str = row.get("InvoiceDate", "").strip()

            if not date_str:
                continue

            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                except ValueError:
                    continue

            day_key = dt.strftime("%Y%m%d")
            sale_date = dt.strftime("%Y-%m-%d %H:%M:%S")

            try:
                qty = int(quantity_str)
            except ValueError:
                qty = 1

            try:
                price = float(price_str)
            except ValueError:
                price = 0.0

            cust_id = customer_str
            if cust_id.endswith(".0"):
                cust_id = cust_id[:-2]

            # Invoices starting with "C" or negative quantity are cancellations / returns
            is_cancellation = invoice.startswith("C") or qty < 0

            if is_cancellation:
                ret_qty = abs(qty) if qty != 0 else 1
                ret_id = f"RET-{dt.strftime('%Y%m')}-{return_counter:07d}"
                return_counter += 1

                # Link to hypothetical sale_id or use invoice
                origin_sale_id = f"SAL-{invoice.replace('C', '')}-{stock_code}"
                refund = round(ret_qty * max(0.50, price), 2)
                reason = random.choice(RETURN_REASONS)

                ret_row = {
                    "return_id": ret_id,
                    "sale_id": origin_sale_id,
                    "return_date": sale_date,
                    "product_id": stock_code,
                    "quantity": ret_qty,
                    "reason": reason,
                    "refund_amount": refund
                }

                # Defect injection on returns (2-5%)
                if random.random() < defect_rate:
                    defect = random.choice(["negative_qty", "null_sale_id", "bad_date"])
                    if defect == "negative_qty":
                        ret_row["quantity"] = -ret_qty
                    elif defect == "null_sale_id":
                        ret_row["sale_id"] = ""
                    elif defect == "bad_date":
                        ret_row["return_date"] = "INVALID_DATE"

                returns_by_date.setdefault(day_key, []).append(ret_row)

            else:
                sale_id = f"SAL-{invoice}-{stock_code}" if invoice else f"SAL-{day_key}-{sale_counter:07d}"
                sale_counter += 1

                store_id = random.choice(store_ids)
                promo_id = random.choice(promo_ids) if random.random() < 0.25 else "PRM-0000"
                discount = round(random.uniform(0.05, 0.25) * price * qty, 2) if promo_id != "PRM-0000" else 0.0
                pay_mode = random.choice(PAYMENT_MODES)

                sale_row = {
                    "sale_id": sale_id,
                    "sale_date": sale_date,
                    "store_id": store_id,
                    "product_id": stock_code,
                    "customer_id": cust_id,
                    "promo_id": promo_id,
                    "quantity": qty,
                    "unit_price": round(price, 2),
                    "discount": discount,
                    "payment_mode": pay_mode
                }

                # Defect injection on sales (2-5%)
                if random.random() < defect_rate:
                    defect = random.choice(["negative_qty", "null_product", "bad_date", "duplicate_id", "zero_price"])
                    if defect == "negative_qty":
                        sale_row["quantity"] = -abs(qty) if qty != 0 else -1
                    elif defect == "null_product":
                        sale_row["product_id"] = ""
                    elif defect == "bad_date":
                        sale_row["sale_date"] = "2009/99/99 00:00:00"
                    elif defect == "zero_price":
                        sale_row["unit_price"] = 0.0
                    elif defect == "duplicate_id":
                        # Add duplicate row
                        sales_by_date.setdefault(day_key, []).append(dict(sale_row))

                sales_by_date.setdefault(day_key, []).append(sale_row)

    # Write daily partitions
    days_to_write = sorted(list(sales_by_date.keys() | returns_by_date.keys()))
    if max_days:
        days_to_write = days_to_write[:max_days]

    sales_file_count = 0
    returns_file_count = 0
    total_sales_rows = 0
    total_returns_rows = 0

    sales_fields = ["sale_id", "sale_date", "store_id", "product_id", "customer_id", "promo_id", "quantity", "unit_price", "discount", "payment_mode"]
    returns_fields = ["return_id", "sale_id", "return_date", "product_id", "quantity", "reason", "refund_amount"]

    for d in days_to_write:
        s_rows = sales_by_date.get(d, [])
        if s_rows:
            fpath = sales_out_dir / f"sales_{d}.csv"
            with open(fpath, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=sales_fields)
                writer.writeheader()
                writer.writerows(s_rows)
            sales_file_count += 1
            total_sales_rows += len(s_rows)

        r_rows = returns_by_date.get(d, [])
        if r_rows:
            fpath = returns_out_dir / f"returns_{d}.csv"
            with open(fpath, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=returns_fields)
                writer.writeheader()
                writer.writerows(r_rows)
            returns_file_count += 1
            total_returns_rows += len(r_rows)

    logger.info(
        "Partitioning completed: %d daily sales files (%d rows), %d daily returns files (%d rows).",
        sales_file_count, total_sales_rows, returns_file_count, total_returns_rows
    )
    return sales_file_count, returns_file_count


if __name__ == "__main__":
    args = parse_args()
    run_generate_daily_csv(sample=args.sample, sample_rows=args.sample_rows, max_days=args.max_days)

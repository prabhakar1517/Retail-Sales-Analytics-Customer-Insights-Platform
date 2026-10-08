"""
Validation Script: Phase 8 - Power BI Integration & BI Deliverables
Verifies that all M scripts, DAX measures, Theme JSON, and Visual Build Guides exist and meet criteria.
"""
import sys
import os
import json
from pathlib import Path

def validate_phase8():
    project_root = Path(__file__).resolve().parent.parent
    power_bi_dir = project_root / "power_bi"
    
    print("=" * 70)
    print("PHASE 8 VALIDATION: Power BI Integration & BI Deliverables")
    print("=" * 70)
    
    passed_tests = 0
    total_tests = 4
    
    # Check 1: Power Query M scripts
    m_path = power_bi_dir / "power_query_m.txt"
    if m_path.exists():
        content = m_path.read_text(encoding="utf-8")
        required_tables = ["dim_date", "dim_product", "dim_customer", "fact_sales", "fact_returns", "customer_rfm"]
        missing_tables = [tbl for tbl in required_tables if tbl not in content]
        if not missing_tables:
            print("[PASS] Check 1: Power Query M scripts contain all required warehouse tables.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 1: Power Query M missing tables: {missing_tables}")
    else:
        print("[FAIL] Check 1: power_bi/power_query_m.txt does not exist.")
        
    # Check 2: DAX measures
    dax_path = power_bi_dir / "dax_measures.txt"
    if dax_path.exists():
        dax_content = dax_path.read_text(encoding="utf-8")
        required_measures = [
            "Total Sales", "Units Sold", "Orders", "Avg Order Value",
            "Total Returns", "Return Rate %", "Sales LY", "YoY Growth %",
            "Promo Sales", "Promo Share %"
        ]
        missing_measures = [m for m in required_measures if m not in dax_content]
        if not missing_measures:
            print("[PASS] Check 2: DAX measures file contains all required business measures.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 2: Missing DAX measures: {missing_measures}")
    else:
        print("[FAIL] Check 2: power_bi/dax_measures.txt does not exist.")
        
    # Check 3: Theme JSON validity
    theme_path = power_bi_dir / "retail_theme.json"
    if theme_path.exists():
        try:
            with open(theme_path, "r", encoding="utf-8") as f:
                theme_data = json.load(f)
            if "name" in theme_data and "dataColors" in theme_data and len(theme_data["dataColors"]) >= 5:
                print(f"[PASS] Check 3: Power BI theme JSON is valid (Theme: '{theme_data['name']}', {len(theme_data['dataColors'])} colors).")
                passed_tests += 1
            else:
                print("[FAIL] Check 3: Theme JSON missing expected keys or colors.")
        except Exception as e:
            print(f"[FAIL] Check 3: JSON parsing error in theme file: {e}")
    else:
        print("[FAIL] Check 3: power_bi/retail_theme.json does not exist.")
        
    # Check 4: Dashboard Build Guide covers all 4 pages
    guide_path = power_bi_dir / "dashboard_build_guide.md"
    if guide_path.exists():
        guide_content = guide_path.read_text(encoding="utf-8")
        pages = ["Page 1: Executive Overview", "Page 2: Product & Supplier", "Page 3: Customer Insights", "Page 4: Promotions & Returns"]
        missing_pages = [p for p in pages if p not in guide_content]
        if not missing_pages:
            print("[PASS] Check 4: Dashboard Build Guide contains instructions for all 4 required report pages.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 4: Build guide missing sections: {missing_pages}")
    else:
        print("[FAIL] Check 4: power_bi/dashboard_build_guide.md does not exist.")
        
    print("-" * 70)
    print(f"Phase 8 Validation Score: {passed_tests}/{total_tests} checks passed.")
    if passed_tests == total_tests:
        print(">>> ALL PHASE 8 VALIDATIONS PASSED SUCCESSFULY! <<<")
        return 0
    else:
        print(">>> SOME PHASE 8 VALIDATIONS FAILED! <<<")
        return 1

if __name__ == "__main__":
    sys.exit(validate_phase8())

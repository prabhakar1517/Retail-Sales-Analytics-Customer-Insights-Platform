"""
Validation Script: Phase 9 - Comprehensive Documentation & Academic Artifacts
Verifies that architecture documents, data dictionaries, viva preparation guide, and root README exist and meet criteria.
"""
import sys
import os
from pathlib import Path

def validate_phase9():
    project_root = Path(__file__).resolve().parent.parent
    docs_dir = project_root / "docs"
    
    print("=" * 70)
    print("PHASE 9 VALIDATION: Comprehensive Documentation & Academic Artifacts")
    print("=" * 70)
    
    passed_tests = 0
    total_tests = 4
    
    # Check 1: docs/architecture.md
    arch_path = docs_dir / "architecture.md"
    if arch_path.exists():
        content = arch_path.read_text(encoding="utf-8")
        required_keywords = ["mermaid", "flowchart", "Bronze", "Silver", "Gold", "DISTSTYLE", "SORTKEY", "MERGE"]
        missing_kw = [kw for kw in required_keywords if kw.lower() not in content.lower()]
        if not missing_kw:
            print("[PASS] Check 1: docs/architecture.md exists and contains diagrams, layer flows, and Redshift design.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 1: architecture.md missing expected topics: {missing_kw}")
    else:
        print("[FAIL] Check 1: docs/architecture.md does not exist.")
        
    # Check 2: docs/data_dictionary.md
    dict_path = docs_dir / "data_dictionary.md"
    if dict_path.exists():
        content = dict_path.read_text(encoding="utf-8")
        required_tables = ["bronze_sales_raw", "bronze_crm_customers_raw", "sales_silver", "dim_date", "dim_customer", "fact_sales", "customer_rfm"]
        missing_tables = [t for t in required_tables if t not in content]
        if not missing_tables and "REJ_" in content:
            print("[PASS] Check 2: docs/data_dictionary.md contains schemas, rejection codes, and all medallion layers.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 2: data_dictionary.md missing expected tables: {missing_tables}")
    else:
        print("[FAIL] Check 2: docs/data_dictionary.md does not exist.")
        
    # Check 3: docs/viva_preparation_guide.md
    viva_path = docs_dir / "viva_preparation_guide.md"
    if viva_path.exists():
        content = viva_path.read_text(encoding="utf-8")
        topics = ["PySpark", "Medallion", "Star Schema", "SCD", "Redshift", "RFM", "Power BI"]
        missing_topics = [top for top in topics if top.lower() not in content.lower()]
        if not missing_topics:
            print("[PASS] Check 3: docs/viva_preparation_guide.md covers all core technical and academic topics.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 3: viva_preparation_guide.md missing topics: {missing_topics}")
    else:
        print("[FAIL] Check 3: docs/viva_preparation_guide.md does not exist.")
        
    # Check 4: Root README.md
    readme_path = project_root / "README.md"
    if readme_path.exists():
        content = readme_path.read_text(encoding="utf-8")
        required_sections = ["Project Brief", "Architecture", "Technology Stack", "Repository Structure", "Quickstart"]
        missing_sec = [s for s in required_sections if s.lower() not in content.lower()]
        if not missing_sec:
            print("[PASS] Check 4: Root README.md is comprehensive with setup guides, stack table, and architecture.")
            passed_tests += 1
        else:
            print(f"[FAIL] Check 4: README.md missing expected sections: {missing_sec}")
    else:
        print("[FAIL] Check 4: Root README.md does not exist.")
        
    print("-" * 70)
    print(f"Phase 9 Validation Score: {passed_tests}/{total_tests} checks passed.")
    if passed_tests == total_tests:
        print(">>> ALL PHASE 9 VALIDATIONS PASSED SUCCESSFULY! <<<")
        return 0
    else:
        print(">>> SOME PHASE 9 VALIDATIONS FAILED! <<<")
        return 1

if __name__ == "__main__":
    sys.exit(validate_phase9())

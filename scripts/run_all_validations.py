"""
Master Validation Suite: Phases 1 through 9
Runs all 9 validation scripts in sequence and reports an overall platform health status.
"""
import sys
import subprocess
from pathlib import Path

def run_all_validations():
    project_root = Path(__file__).resolve().parent.parent
    scripts_dir = project_root / "scripts"
    
    phases = [
        ("Phase 1: Environment & Scaffolding", scripts_dir / "validate_phase1.py"),
        ("Phase 2: Synthetic Data Generation", scripts_dir / "validate_phase2.py"),
        ("Phase 3: Bronze Ingestion Layer", scripts_dir / "validate_phase3.py"),
        ("Phase 4: Silver Cleansing & Rejects", scripts_dir / "validate_phase4.py"),
        ("Phase 5: Gold Star Schema & RFM", scripts_dir / "validate_phase5.py"),
        ("Phase 6: Redshift DDL & S3 Loader", scripts_dir / "validate_phase6.py"),
        ("Phase 7: Central Logging, Tests & DAG", scripts_dir / "validate_phase7.py"),
        ("Phase 8: Power BI Deliverables", scripts_dir / "validate_phase8.py"),
        ("Phase 9: Comprehensive Documentation", scripts_dir / "validate_phase9.py"),
    ]
    
    print("=" * 80)
    print("       RETAIL SALES ANALYTICS & CUSTOMER INSIGHTS PLATFORM")
    print("                 END-TO-END MASTER VALIDATION SUITE")
    print("=" * 80)
    
    results = []
    
    for name, script_path in phases:
        if not script_path.exists():
            print(f"[MISSING] {name} - Script not found: {script_path.name}")
            results.append((name, "FAIL (Missing script)"))
            continue
            
        print(f"\n>>> Running {name} ({script_path.name})...")
        proc = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
        print(proc.stdout.strip())
        if proc.stderr.strip():
            print("STDERR:", proc.stderr.strip())
            
        if proc.returncode == 0:
            results.append((name, "PASS"))
        else:
            results.append((name, "FAIL"))
            
    print("\n" + "=" * 80)
    print("                     OVERALL VALIDATION SUMMARY")
    print("=" * 80)
    all_passed = True
    for name, status in results:
        status_str = f"[{status}]"
        print(f"{name:<45} {status_str:>32}")
        if status != "PASS":
            all_passed = False
            
    print("-" * 80)
    if all_passed:
        print(">>> CONGRATULATIONS! ALL 9 PROJECT PHASES HAVE PASSED 100% SUCCESFULLY! <<<")
        return 0
    else:
        print(">>> ATTENTION: One or more phases encountered failures. Inspect logs above. <<<")
        return 1

if __name__ == "__main__":
    sys.exit(run_all_validations())

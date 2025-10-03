\
"""
runner.py
Kör alla skript i ./scripts i kontrollerad ordning, loggar utfall,
och säkerställer att CSV-resultat migreras till .xlsx under ./data.

- Exkluderar: runner.py, excel_utils.py, __init__.py
- Ordning: prefix 00_, 01_, ... annars alfabetisk
- .env: laddas om filen finns
"""

from __future__ import annotations
import importlib.util
import io
import os
import sys
import subprocess
import traceback
from pathlib import Path
from datetime import datetime
from typing import List

import pandas as pd
from dotenv import load_dotenv

# Lägg till repo-rot på sys.path
ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
DATA = ROOT / "data"

# Ladda .env om den finns
dotenv_path = ROOT / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)

# Lokal import
from excel_utils import append_df

LOG_PATH = DATA / "run.log"
EXCLUDE = {"runner.py", "excel_utils.py", "__init__.py"}


def log(msg: str):
    msg2 = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(msg2, flush=True)
    DATA.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(msg2 + "\n")


def discover_scripts() -> List[Path]:
    files = [p for p in SCRIPTS.glob("*.py") if p.name not in EXCLUDE]
    # Simple prioritetsordning: 00_, 01_, ... annars alfabetisk
    def sort_key(p: Path):
        name = p.name
        if name[:3].isdigit() and name[2] == "_":
            try:
                return (0, int(name[:2]), name)
            except ValueError:
                pass
        return (1, 99, name)
    return sorted(files, key=sort_key)


def try_import_and_run(module_path: Path) -> bool:
    """Försök importera modul och köra main() om den finns. Faller tillbaka till subprocess."""
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)  # type: ignore
        if hasattr(mod, "main") and callable(mod.main):
            log(f"Kör main() i {module_path.name}")
            mod.main()
            return True
        else:
            return False
    except Exception:
        log(f"Import/main misslyckades för {module_path.name}, kör som subprocess:")
        log(traceback.format_exc())
        return False


def run_subprocess(module_path: Path) -> int:
    cmd = [sys.executable, str(module_path)]
    proc = subprocess.run(cmd, cwd=str(SCRIPTS))
    return proc.returncode


def migrate_csv_outputs():
    """
    Hitta nya/uppdaterade CSV:er i repo-roten eller i ./data och migrera till Excel-format.
    - Skapar ./data/<basename>.xlsx
    - Fliknamn = <basename> (utan .csv)
    - Append DF-rader
    """
    # Sök i både root och data
    candidates = list(ROOT.glob("*.csv")) + list(DATA.glob("*.csv"))
    for csv_path in candidates:
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            continue
        if df.empty:
            continue

        base = csv_path.stem
        xlsx_path = DATA / f"{base}.xlsx"
        sheet_name = base
        log(f"Migrerar CSV → XLSX: {csv_path.name} → {xlsx_path.name} [{sheet_name}] ({len(df)} rader)")
        append_df(str(xlsx_path), sheet_name, df)
        # Ta inte bort ursprunglig CSV automatiskt (för säkerhets skull)


def main():
    DATA.mkdir(parents=True, exist_ok=True)
    LOG_PATH.touch(exist_ok=True)

    scripts = discover_scripts()
    if not scripts:
        log("Inga skript hittades i ./scripts.")
        sys.exit(0)

    failures = 0
    for path in scripts:
        log(f"Startar: {path.name}")
        ok = try_import_and_run(path)
        rc = 0
        if not ok:
            rc = run_subprocess(path)
        if rc != 0:
            failures += 1
            log(f"❌ Fel i {path.name} (exit code {rc})")
        else:
            log(f"✅ Klar: {path.name}")

    # Efter alla körningar: migrera CSV-resultat till Excel
    migrate_csv_outputs()

    if failures:
        log(f"Färdig med fel: {failures} skript misslyckades.")
        sys.exit(1)
    else:
        log("Alla skript kördes utan fel.")
        sys.exit(0)


if __name__ == "__main__":
    main()

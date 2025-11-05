#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup
- UC-first flow, auto-uses Serverless SQL (Statements API) when Default Storage blocks local CREATE CATALOG.
- Zero manual inputs: discovers host, token, and a Serverless warehouse automatically.
- If no Serverless warehouse is found, falls back to the 'workspace' catalog.
"""

import os, sys, glob, shutil, json, time
import requests

print("""
╔══════════════════════════════════════════════════════════════╗
║                DevSecOps Demo - Installing...                ║
╚══════════════════════════════════════════════════════════════╝
""")

# ---------------- Preflight ----------------
try:
    spark  # noqa
    dbutils  # noqa
except NameError:
    print("ERROR  Run this in a Databricks notebook (needs spark + dbutils).")
    sys.exit(1)

# ---------------- Resolve REPO_PATH ----------------
if "REPO_PATH" not in globals():
    demo_dirs = [d for d in os.listdir("/tmp") if d.startswith("demo_")]
    if demo_dirs:
        demo_dirs.sort(key=lambda x: os.path.getmtime(f"/tmp/{x}"), reverse=True)
        REPO_PATH = f"/tmp/{demo_dirs[0]}"
    else:
        print("ERROR  Cannot determine REPO_PATH. Pass it in the exec namespace.")
        sys.exit(1)
print(f"Installing from: {REPO_PATH}")

# ---------------- Config (override via exec globals if desired) ----------------
TARGET_CATALOG = globals().get("CATALOG", "DevSecOps_Labs")
SCHEMA         = globals().get("SCHEMA",  "Agent_Bricks_Lab")
VOLUME         = globals().get("VOLUME",  "meijer_store_transcripts")
TABLE          = globals().get("TABLE",   "meijer_store_tickets")

# If Serverless cannot be used and UC catalog creation is blocked, use this
FALLBACK_EXISTING_CATALOG = globals().get("FALLBACK_EXISTING_CATALOG", "workspace")

def q(x: str) -> str:
    return f"`{x}`"

# ---------------- Helpers: host + token + serverless discovery ----------------
def _workspace_host() -> str:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        host = ctx.browserHostName().get()
        if not host.startswith("https://"):
            host = "https://" + host
        return host.rstrip("/")
    except Exception:
        h = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        return h

def _context_token() -> str:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tok = ctx.apiToken().get()
        return tok or ""
    except Exception:
        return ""

def _pat() -> str:
    try:
        return dbutils.secrets.get("oneclick", "pat")
    except Exception:
        return os.environ.get("DATABRICKS_TOKEN", "")

HOST = _workspace_host()
TOKEN = _context_token() or _pat()

def _api_headers():
    if not HOST or not TOKEN:
        raise RuntimeError("Missing HOST or API token. Provide a PAT in secret oneclick/pat or DATABRICKS_TOKEN.")
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def discover_serverless_warehouse_id() -> str | None:
    try:
        r = requests.get(f"{HOST}/api/2.0/sql/warehouses", headers=_api_headers(), timeout=30)
        r.raise_for_status()
        data = r.json()
        for w in data.get("warehouses", []):
            if w.get("is_serverless") or w.get("enable_serverless_compute"):
                if w.get("state") in (None, "RUNNING", "STOPPED"):
                    return w.get("id")
        return None
    except Exception:
        return None

def run_serverless_sql(sql_text: str, warehouse_id: str, timeout_sec: int = 120) -> None:
    payload = {
        "statement": sql_text,
        "warehouse_id": warehouse_id,
        "wait_timeout": "5s",
        "on_wait_timeout": "CONTINUE",
    }
    r = requests.post(f"{HOST}/api/2.0/sql/statements", headers=_api_headers(), data=json.dumps(payload), timeout=30)
    r.raise_for_status()
    stmt_id = r.json()["statement_id"]
    get_url = f"{HOST}/api/2.0/sql/statements/{stmt_id}"
    t0 = time.time()
    while True:
        g = requests.get(get_url, headers=_api_headers(), timeout=30)
        g.raise_for_status()
        out = g.json()
        s = out["status"]["state"]
        if s in ("FINISHED", "CANCELED"):
            return
        if s in ("FAILED", "ERROR"):
            err = out["status"].get("error", {})
            raise RuntimeError(f"Serverless SQL failed: {err.get('message','unknown error')}")
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(f"Timed out waiting for SQL statement: {sql_text[:80]}...")
        time.sleep(2)

# ---------------- Step 1: Ensure catalog ----------------
print("\n[1/4] Creating Unity Catalog assets...")

chosen_catalog = TARGET_CATALOG
created_on_serverless = False

def _needs_default_storage_fix(e_msg: str) -> bool:
    return ("Metastore storage root URL does not exist" in e_msg) or ("Default Storage" in e_msg)

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(chosen_catalog)}")
    print(f"  OK   Catalog ready: {chosen_catalog} (local)")
except Exception as e:
    msg = str(e)
    if _needs_default_storage_fix(msg):
        print("  INFO Local CREATE CATALOG blocked by Default Storage policy.")
        try:
            wh_id = discover_serverless_warehouse_id()
            if wh_id:
                print(f"  INFO Using Serverless warehouse: {wh_id}")
                run_serverless_sql(f"CREATE CATALOG IF NOT EXISTS {q(chosen_catalog)};", wh_id)
                created_on_serverless = True
                print(f"  OK   Catalog ready: {chosen_catalog} (serverless)")
            else:
                print("  WARN No Serverless SQL warehouse found; falling back to workspace catalog.")
                chosen_catalog = FALLBACK_EXISTING_CATALOG
        except Exception as ee:
            print(f"  WARN Serverless CREATE CATALOG failed: {str(ee)[:220]}")
            print(f"  INFO Falling back to existing catalog: {FALLBACK_EXISTING_CATALOG}")
            chosen_catalog = FALLBACK_EXISTING_CATALOG
    else:
        print(f"  WARN CREATE CATALOG failed: {msg[:220]}")
        print(f"  INFO Falling back to existing catalog: {FALLBACK_EXISTING_CATALOG}")
        chosen_catalog = FALLBACK_EXISTING_CATALOG

# Ensure catalog usable
try:
    spark.sql(f"USE CATALOG {q(chosen_catalog)}")
except Exception as e:
    print(f"ERROR  USE CATALOG {chosen_catalog} failed: {str(e)[:220]}")
    sys.exit(1)

# Create schema
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(chosen_catalog)}.{q(SCHEMA)}")
    print(f"  OK   Schema ready: {chosen_catalog}.{SCHEMA}")
except Exception as e:
    print(f"ERROR  CREATE SCHEMA failed: {str(e)[:220]}")
    sys.exit(1)

# Create volume
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(chosen_catalog)}.{q(SCHEMA)}.{q(VOLUME)}")
    print(f"  OK   Volume ready: {chosen_catalog}.{SCHEMA}.{VOLUME}")
except Exception as e:
    print(f"ERROR  CREATE VOLUME failed: {str(e)[:220]}")
    sys.exit(1)

# ---------------- Step 2: Import PDFs ----------------
print("\n[2/4] Importing PDFs...")
pdfs = glob.glob(os.path.join(REPO_PATH, "data", "pdfs", "*.pdf"))
vol_path = f"/Volumes/{chosen_catalog}/{SCHEMA}/{VOLUME}"
success = 0
failed = 0

if not pdfs:
    print(f"  WARN No PDFs found at {REPO_PATH}/data/pdfs/")
else:
    print(f"  Copying {len(pdfs)} PDF(s) to {vol_path}")
    for i, pdf in enumerate(pdfs, start=1):
        name = os.path.basename(pdf)
        try:
            dbutils.fs.cp(f"file:{pdf}", f"{vol_path}/{name}", recurse=False)
            success += 1
            if i % 50 == 0:
                print(f"  ... {i}/{len(pdfs)}")
        except Exception as e:
            failed += 1
            print(f"  WARN Failed {name}: {str(e)[:140]}")
    print(f"  OK   Imported {success}/{len(pdfs)} PDF(s). Failed: {failed}")

# ---------------- Step 3: Create table ----------------
print("\n[3/4] Creating table from parquet...")
parquet_glob = os.path.join(REPO_PATH, "data", "table", "*.parquet")
count = 0
try:
    df = spark.read.parquet(f"file:{parquet_glob}")
except Exception as e:
    print(f"ERROR  Could not read parquet: {str(e)[:200]}")
    df = None

if df is not None:
    full_table = f"{q(chosen_catalog)}.{q(SCHEMA)}.{q(TABLE)}"
    try:
        df.write.mode("overwrite").saveAsTable(full_table)
        count = spark.table(full_table).count()
        print(f"  OK   Table {full_table} with {count:,} rows")
    except Exception as e:
        print(f"ERROR  Writing table failed: {str(e)[:200]}")

# ---------------- Step 4: Cleanup ----------------
print("\n[4/4] Cleaning up temporary files...")
try:
    if REPO_PATH.startswith("/tmp/demo_") and os.path.isdir(REPO_PATH):
        shutil.rmtree(REPO_PATH)
        print("  OK   Temporary checkout removed")
    else:
        print("  INFO Skip cleanup: non-temporary path")
except Exception as e:
    print(f"  WARN Cleanup skipped: {str(e)[:160]}")

# ---------------- Summary ----------------
mode = "serverless+local" if created_on_serverless else "local_only" if chosen_catalog == TARGET_CATALOG else f"fallback:{chosen_catalog}"
print(f"""
╔══════════════════════════════════════════════════════════════╗
║                        Setup Complete                        ║
╚══════════════════════════════════════════════════════════════╝
Mode     {mode}
Catalog  {chosen_catalog}
Schema   {SCHEMA}
Volume   {VOLUME}  (imported {success}{f", {failed} failed" if failed else ""})
Table    {TABLE}   ({count:,} rows)

Try
  SELECT * FROM {chosen_catalog}.{SCHEMA}.{TABLE} LIMIT 10;

PDFs
  /Volumes/{chosen_catalog}/{SCHEMA}/{VOLUME}/
""")

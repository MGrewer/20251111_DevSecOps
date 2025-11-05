#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup (hardened)
- Environment checks
- Unity Catalog detection
- Privilege-aware catalog creation with fallback
- Safe identifier quoting
- Robust errors and clear logging
"""

import os, sys, time, glob, shutil, traceback

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║                DevSecOps Demo - Installing...                ║
╚══════════════════════════════════════════════════════════════╝
"""
print(BANNER.strip())

# -------- Preflight: environment checks --------
try:
    spark  # noqa: F821
    dbutils  # noqa: F821
except NameError:
    print("ERROR  Environment not detected. Run this in a Databricks notebook where 'spark' and 'dbutils' exist.")
    sys.exit(1)

# -------- Resolve REPO_PATH --------
if "REPO_PATH" not in globals():
    # Fallback: find the most recent /tmp/demo_* dir
    demo_dirs = [d for d in os.listdir("/tmp") if d.startswith("demo_")]
    if demo_dirs:
        demo_dirs.sort(key=lambda x: os.path.getmtime(f"/tmp/{x}"), reverse=True)
        REPO_PATH = f"/tmp/{demo_dirs[0]}"
    else:
        print("ERROR  Cannot determine REPO_PATH. Pass it in the exec namespace or create /tmp/demo_*.")
        sys.exit(1)

print(f"Installing from: {REPO_PATH}")

# -------- Configurable identifiers (can be overridden via exec globals) --------
CATALOG = globals().get("CATALOG", "DevSecOps_Labs")
SCHEMA  = globals().get("SCHEMA",  "Agent_Bricks_Lab")
VOLUME  = globals().get("VOLUME",  "meijer_store_transcripts")
TABLE   = globals().get("TABLE",   "meijer_store_tickets")

def q(ident: str) -> str:
    """Backtick-quote a SQL identifier."""
    return f"`{ident}`"

# -------- Detect Unity Catalog availability and current user --------
try:
    current_user = spark.sql("SELECT current_user()").first()[0]
    _ = spark.sql("SHOW CATALOGS").collect()
    uc_available = True
except Exception as e:
    uc_available = False
    print("WARN   Unity Catalog may not be available on this cluster.")
    print(f"       Detail: {str(e)[:200]}")
    # This demo requires UC for CREATE CATALOG and CREATE VOLUME
    print("ERROR  Unity Catalog not available. Attach a UC-enabled cluster and retry.")
    sys.exit(1)

# -------- 1. Create UC assets with robust handling --------
print("\n[1/4] Creating Unity Catalog assets...")

using_fallback = False
chosen_catalog = CATALOG
chosen_schema = SCHEMA

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(chosen_catalog)}")
    print(f"  OK   Catalog ready: {chosen_catalog}")
except Exception as e:
    print(f"ERROR  Could not create or access catalog {chosen_catalog}: {str(e)[:200]}")
    print("INFO   Falling back to users catalog and your user schema.")
    chosen_catalog = "users"
    chosen_schema = current_user  # often an email
    using_fallback = True

# Create schema
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(chosen_catalog)}.{q(chosen_schema)}")
    print(f"  OK   Schema ready: {chosen_catalog}.{chosen_schema}")
except Exception as e:
    print(f"ERROR  Could not create schema {chosen_catalog}.{chosen_schema}: {str(e)[:200]}")
    sys.exit(1)

# Create volume
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(chosen_catalog)}.{q(chosen_schema)}.{q(VOLUME)}")
    print(f"  OK   Volume ready: {chosen_catalog}.{chosen_schema}.{VOLUME}")
except Exception as e:
    print(f"ERROR  Could not create volume {chosen_catalog}.{chosen_schema}.{VOLUME}: {str(e)[:200]}")
    print("       Ensure the cluster is UC enabled and you have CREATE VOLUME on the schema.")
    sys.exit(1)

# -------- 2. Import PDFs into the volume --------
print("\n[2/4] Importing PDFs...")
pdfs_dir = os.path.join(REPO_PATH, "data", "pdfs")
pdfs = glob.glob(os.path.join(pdfs_dir, "*.pdf"))
vol_path = f"/Volumes/{chosen_catalog}/{chosen_schema}/{VOLUME}"

success = 0
failed = 0

if not pdfs:
    print(f"WARN   No PDFs found at {pdfs_dir}")
else:
    print(f"  Found {len(pdfs)} PDFs to import into {vol_path}")
    for i, pdf in enumerate(pdfs, start=1):
        name = os.path.basename(pdf)
        try:
            dbutils.fs.cp(f"file:{pdf}", f"{vol_path}/{name}")
            success += 1
            if i % 50 == 0:
                print(f"  ... Progress {i}/{len(pdfs)}")
        except Exception as e:
            failed += 1
            # continue while logging a concise error
            print(f"  WARN  Failed to copy {name}: {str(e)[:120]}")
    print(f"  OK   Imported {success}/{len(pdfs)} PDFs. Failed: {failed}")

# -------- 3. Create Delta table from staged parquet --------
print("\n[3/4] Creating Delta table...")
table_path = os.path.join(REPO_PATH, "data", "table", "*.parquet")
count = 0

try:
    df = spark.read.parquet(f"file:{table_path}")
except Exception as e:
    print(f"ERROR  Could not read parquet from {table_path}: {str(e)[:200]}")
    df = None

if df is not None:
    full_table_name = f"{q(chosen_catalog)}.{q(chosen_schema)}.{q(TABLE)}"
    try:
        df.write.mode("overwrite").saveAsTable(full_table_name)
        count = spark.table(full_table_name).count()
        print(f"  OK   Table created {chosen_catalog}.{chosen_schema}.{TABLE} with {count:,} rows")
    except Exception as e:
        print(f"ERROR  Table creation failed: {str(e)[:200]}")

# -------- 4. Cleanup temp checkout --------
print("\n[4/4] Cleaning up temporary files...")
try:
    # Only remove if we created a /tmp/demo_* path
    if REPO_PATH.startswith("/tmp/demo_") and os.path.isdir(REPO_PATH):
        shutil.rmtree(REPO_PATH)
        print("  OK   Temporary files removed")
    else:
        print("INFO   Skipped cleanup, REPO_PATH is not a disposable /tmp/demo_* folder")
except Exception as e:
    print(f"WARN   Cleanup skipped due to permission or other issue: {str(e)[:160]}")

# -------- Summary --------
summary = f"""
╔══════════════════════════════════════════════════════════════╗
║                        Setup Complete                        ║
╚══════════════════════════════════════════════════════════════╝

Created in Unity Catalog
  Catalog  {chosen_catalog}{"  (fallback from " + CATALOG + ")" if using_fallback else ""}
  Schema   {chosen_schema}
  Volume   {VOLUME}  imported {success} PDFs{"  with " + str(failed) + " failures" if failed else ""}
  Table    {TABLE}   {count:,} rows

Quick Test
  SELECT * FROM {chosen_catalog}.{chosen_schema}.{TABLE} LIMIT 10;

PDFs Location
  /Volumes/{chosen_catalog}/{chosen_schema}/{VOLUME}/
"""
print(summary.strip())

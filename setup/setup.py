#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup (No Repos Required)
This script auto-downloads from GitHub and installs everything
"""

import os, sys, time, glob, subprocess, shutil

print("""
╔══════════════════════════════════════════════════════════════╗
║         DevSecOps Demo - Installing...                      ║
╚══════════════════════════════════════════════════════════════╝
""")

# Determine where we're running from
# This handles being run from any cloned location
current_dir = os.path.dirname(os.path.abspath(__file__))
if "/setup" in current_dir:
    # We're in the setup directory, go up one level
    REPO_PATH = os.path.dirname(current_dir)
else:
    # We're at repo root or somewhere else
    REPO_PATH = current_dir

# Check if data exists where we expect it
if not os.path.exists(f"{REPO_PATH}/data"):
    # Need to clone from GitHub
    print("📥 Downloading from GitHub...")
    REPO_PATH = f"/tmp/devSecops_{int(time.time())}"  # Unique path each time
    
    try:
        subprocess.run([
            "git", "clone", "--depth", "1",
            "https://github.com/MGrewer/20251111_DevSecOps", 
            REPO_PATH
        ], check=True, capture_output=True)
        print("✓ Downloaded files")
    except Exception as e:
        print(f"❌ Download failed: {e}")
        sys.exit(1)

print(f"Installing from: {REPO_PATH}")

# Hardcoded configuration - no config.json needed
CATALOG = "DevSecOps_Labs"
SCHEMA = "Agent_Bricks_Lab"
VOLUME = "meijer_store_transcripts"
TABLE = "meijer_store_tickets"

# 1. Create UC assets
print("\n[1/4] Creating Unity Catalog assets...")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"  ✓ Catalog: {CATALOG}")
except:
    print(f"  ℹ️ Using existing: {CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"  ✓ Schema: {SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"  ✓ Volume: {VOLUME}")

# 2. Import PDFs
print("\n[2/4] Importing PDFs...")
pdfs = glob.glob(f"{REPO_PATH}/data/pdfs/*.pdf")
vol_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
success = 0

if not pdfs:
    print(f"  ⚠️ No PDFs found at {REPO_PATH}/data/pdfs/")
else:
    print(f"  Found {len(pdfs)} PDFs to import")
    
    for i, pdf in enumerate(pdfs):
        try:
            name = os.path.basename(pdf)
            dbutils.fs.cp(f"file:{pdf}", f"{vol_path}/{name}")
            success += 1
            if (i+1) % 50 == 0:
                print(f"  Progress: {i+1}/{len(pdfs)} files...")
        except:
            pass
    
    print(f"  ✓ Imported {success}/{len(pdfs)} PDFs")

# 3. Create table
print("\n[3/4] Creating Delta table...")
try:
    df = spark.read.parquet(f"file:{REPO_PATH}/data/table/*.parquet")
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
    count = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}").count()
    print(f"  ✓ Table created: {count:,} rows")
except Exception as e:
    print(f"  ❌ Table creation failed: {str(e)[:100]}")
    count = 0

# 4. Clean up - wrapped in try/except to avoid permission errors
print("\n[4/4] Cleaning up temporary files...")
if REPO_PATH.startswith("/tmp/"):
    try:
        shutil.rmtree(REPO_PATH)
        print("  ✓ Temporary files cleaned")
    except:
        print("  ℹ️ Skipped cleanup (permission issue - files will auto-clean)")

print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ✅ Setup Complete!                       ║
╚══════════════════════════════════════════════════════════════╝

📦 Created in Unity Catalog:
  • Catalog: {CATALOG}
  • Schema:  {SCHEMA}
  • Volume:  {VOLUME} ({success} PDFs)
  • Table:   {TABLE} ({count:,} rows)

📝 Quick Test:
  SELECT * FROM {CATALOG}.{SCHEMA}.{TABLE} LIMIT 10;
  
📂 PDFs Location:
  /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/

🚀 Happy coding!
""")

# Check we're in Databricks
try:
    spark
    dbutils
except:
    print("❌ Run this in a Databricks notebook!")
    sys.exit(1)
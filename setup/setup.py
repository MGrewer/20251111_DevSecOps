#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup (No Repos Required)
This script auto-downloads from GitHub and installs everything
"""

import os, sys, json, time, glob, subprocess, shutil

print("""
╔══════════════════════════════════════════════════════════════╗
║         DevSecOps Demo - Installing...                      ║
╚══════════════════════════════════════════════════════════════╝
""")

# Check if running locally or need to download
if os.path.exists("./config.json"):
    REPO_PATH = os.getcwd()
    print(f"Installing from: {REPO_PATH}")
else:
    # Download from GitHub
    print("📥 Downloading from GitHub...")
    REPO_PATH = "/tmp/devSecops_demo"
    
    if os.path.exists(REPO_PATH):
        shutil.rmtree(REPO_PATH)
    
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

# Load config
with open(f"{REPO_PATH}/config.json", 'r') as f:
    config = json.load(f)

CATALOG = config["catalog"]
SCHEMA = config["schema"]
VOLUME = config["volume"]
TABLE = config["table"]

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

for i, pdf in enumerate(pdfs):
    try:
        name = os.path.basename(pdf)
        dbutils.fs.cp(f"file:{pdf}", f"{vol_path}/{name}")
        success += 1
        if (i+1) % 50 == 0:
            print(f"  {i+1}/{len(pdfs)} files...")
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
    print(f"  ❌ Failed: {str(e)[:100]}")
    count = 0

# 4. Clean up
if REPO_PATH == "/tmp/devSecops_demo":
    shutil.rmtree(REPO_PATH)

print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ✅ Setup Complete!                        ║
╚══════════════════════════════════════════════════════════════╝

📦 Created:
  • {CATALOG}.{SCHEMA}.{VOLUME} ({success} PDFs)
  • {CATALOG}.{SCHEMA}.{TABLE} ({count:,} rows)

📝 Try:
  SELECT * FROM {CATALOG}.{SCHEMA}.{TABLE} LIMIT 10;
""")

# Check we're in Databricks
try:
    spark
    dbutils
except:
    print("❌ Run this in a Databricks notebook!")
    sys.exit(1)
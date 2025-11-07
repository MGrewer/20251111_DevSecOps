#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup
Sets up Unity Catalog assets, volumes, and starter table
for the DevSecOps Labs environment per README specification.
"""

import os, sys, glob, shutil, time

# ╔════════════════════════════════════════════════════════════════════╗
# ║                      0. PRE-FLIGHT CHECKS                          ║
# ╚════════════════════════════════════════════════════════════════════╝
print("""
╔════════════════════════════════════════════════════════════════════╗
║                    DevSecOps Labs - Installing                     ║
╚════════════════════════════════════════════════════════════════════╝
""")

try:
    spark  # noqa
    dbutils  # noqa
except NameError:
    print(" [ FAIL ]  Must be run inside a Databricks notebook environment.")
    sys.exit(1)

if "REPO_PATH" not in globals():
    demo_dirs = [d for d in os.listdir("/tmp") if d.startswith("demo_")]
    if not demo_dirs:
        print(" [ FAIL ]  No repository found; REPO_PATH not set.")
        sys.exit(1)
    demo_dirs.sort(key=lambda x: os.path.getmtime(f"/tmp/{x}"), reverse=True)
    REPO_PATH = f"/tmp/{demo_dirs[0]}"

print(f" [ INFO ]  Installing from: {REPO_PATH}")

# ---------------------------------------------------------------------
# 1. CONFIGURATION (per README)
# ---------------------------------------------------------------------
CATALOG         = "devsecops_labs"
AGENT_SCHEMA    = "agent_bricks_lab"
DEMAND_SCHEMA   = "demand_sensing"
PDFS_VOLUME     = "meijer_store_transcripts"
RAW_VOLUME      = "raw_data"
DATA_VOLUME     = "data"
TABLE_NAME      = "meijer_store_tickets"

q = lambda x: f"`{x}`"

def divider(title):
    print(f"\n{'-'*72}\n{title}\n{'-'*72}")

# ---------------------------------------------------------------------
divider("1. CREATING UNITY CATALOG ASSETS")
# ---------------------------------------------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(PDFS_VOLUME)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(RAW_VOLUME)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}.{q(DATA_VOLUME)}")

pdfs_vol_fs = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{PDFS_VOLUME}"
raw_vol_fs  = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
data_vol_fs = f"/Volumes/{CATALOG}/{DEMAND_SCHEMA}/{DATA_VOLUME}"

print(f" [ OK ]  Catalog  → {CATALOG}")
print(f" [ OK ]  Schemas  → {AGENT_SCHEMA}, {DEMAND_SCHEMA}")
print(f" [ OK ]  Volumes  → {pdfs_vol_fs}, {raw_vol_fs}, {data_vol_fs}")

# ---------------------------------------------------------------------
divider("2. IMPORTING PDFS TO meijer_store_transcripts")
# ---------------------------------------------------------------------
pdf_dir = os.path.join(REPO_PATH, "data", "pdfs")
pdfs = sorted(glob.glob(os.path.join(pdf_dir, "*.pdf")))
copied = 0
if not pdfs:
    print(f" [ WARN ]  No PDFs found at {pdf_dir}")
else:
    os.makedirs(f"/dbfs{pdfs_vol_fs}", exist_ok=True)
    try:
        for i, src in enumerate(pdfs, start=1):
            dbutils.fs.cp(f"file:{src}", f"{pdfs_vol_fs}/{os.path.basename(src)}")
            copied += 1
            if i % 50 == 0:
                print(f"   ... copied {i}/{len(pdfs)}")
        print(f" [ OK ]  Imported {copied}/{len(pdfs)} PDFs (fast path)")
    except Exception as e:
        print(f" [ INFO ] Fast path blocked ({str(e)[:100]}). Retrying via stream copy...")
        for src in pdfs:
            dst = f"/dbfs{pdfs_vol_fs}/{os.path.basename(src)}"
            with open(src, "rb") as rf, open(dst, "wb") as wf:
                shutil.copyfileobj(rf, wf, 1024 * 1024)
            copied += 1
        print(f" [ OK ]  Imported {copied} PDFs via fallback stream copy")

# ---------------------------------------------------------------------
divider("3. COPYING RAW SOURCE FILES TO raw_data AND demand_sensing.data")
# ---------------------------------------------------------------------
raw_src_root = os.path.join(REPO_PATH, "data", "raw")
if not os.path.isdir(raw_src_root):
    print(f" [ WARN ]  No raw source directory found: {raw_src_root}")
else:
    subdirs = [d for d in os.listdir(raw_src_root) if os.path.isdir(os.path.join(raw_src_root, d))]
    for name in subdirs:
        src_dir = os.path.join(raw_src_root, name)
        agent_dest = f"/dbfs{raw_vol_fs}/{name}"
        demand_dest = f"/dbfs{data_vol_fs}/raw/{name}"
        os.makedirs(agent_dest, exist_ok=True)
        os.makedirs(demand_dest, exist_ok=True)

        files = [f for f in glob.glob(os.path.join(src_dir, "*")) if os.path.isfile(f)]
        if not files:
            print(f" [ INFO ]  raw/{name} is empty")
            continue

        for fp in files:
            base = os.path.basename(fp)
            with open(fp, "rb") as rf:
                buf = rf.read()
            open(f"{agent_dest}/{base}", "wb").write(buf)
            open(f"{demand_dest}/{base}", "wb").write(buf)
        print(f" [ OK ]  raw/{name} → {raw_vol_fs}/{name}  and  {data_vol_fs}/raw/{name}")

# ---------------------------------------------------------------------
divider("4. CREATING TABLE meijer_store_tickets")
# ---------------------------------------------------------------------
tickets_raw_dir = os.path.join(raw_src_root, "tickets")
def read_tickets():
    pat = os.path.join(tickets_raw_dir, "*")
    try:
        return spark.read.parquet(f"file:{pat}")
    except: pass
    try:
        return spark.read.options(header=True, inferSchema=True).csv(f"file:{pat}")
    except: pass
    try:
        return spark.read.json(f"file:{pat}")
    except: pass
    pq_dir = os.path.join(REPO_PATH, "data", "table")
    if glob.glob(os.path.join(pq_dir, "*.parquet")):
        return spark.read.parquet(f"file:{os.path.join(pq_dir, '*.parquet')}")
    return None

df = read_tickets()
if df is None:
    print(" [ WARN ]  Could not read tickets dataset. Table creation skipped.")
else:
    dest = f"{raw_vol_fs}/tables/tickets"
    os.makedirs(f"/dbfs{dest}", exist_ok=True)
    df.write.mode("overwrite").format("delta").save(dest)
    full_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(TABLE_NAME)}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{dest}'")
    count = spark.table(full_table).count()
    print(f" [ OK ]  Created {full_table} with {count:,} rows")

# ---------------------------------------------------------------------
print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                         SETUP COMPLETE                             ║
╚════════════════════════════════════════════════════════════════════╝
Catalog : {CATALOG}
Schemas : {AGENT_SCHEMA}, {DEMAND_SCHEMA}
Volumes :
  - {CATALOG}.{AGENT_SCHEMA}.{PDFS_VOLUME}         (PDFs)
  - {CATALOG}.{AGENT_SCHEMA}.{RAW_VOLUME}          (source + tickets)
  - {CATALOG}.{DEMAND_SCHEMA}.{DATA_VOLUME}        (lab data)
Table   : {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}
""")

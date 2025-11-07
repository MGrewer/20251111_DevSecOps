#!/usr/bin/env python
"""
DevSecOps Demo - One Click Setup and Cleanup (Serverless-safe)
- Bootstrap guarantees REPO_PATH and optional MODE dispatch.
- Uses only dbutils.fs paths (no /dbfs FUSE), so it works on Serverless.
- Creates the exact UC layout per README.
- Copies PDFs and raw files.
- Creates one table: devsecops_labs.agent_bricks_lab.meijer_store_tickets.
"""

# ╔════════════════════════════════════════════════════════════════════╗
# ║                      0. BOOTSTRAP AND DISPATCH                     ║
# ╚════════════════════════════════════════════════════════════════════╝
import os, sys, time, shutil

MODE       = globals().get("MODE", "setup")     # "setup" or "cleanup"
REPO_URL   = globals().get("REPO_URL", "https://github.com/MGrewer/20251111_DevSecOps")
BRANCH     = globals().get("BRANCH", "main")
REPO_WS    = globals().get("REPO_WS_NAME", "DevSecOps")  # /Workspace/Repos/<user>/DevSecOps

def _ctx_user():
    try:
        return spark.sql("select current_user()").first()[0]
    except Exception:
        return None

def _exists_ws_repo(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def _ensure_repo_path():
    # 1) Prefer Workspace Repo if available
    user = _ctx_user()
    if user:
        ws_repo = f"/Workspace/Repos/{user}/{REPO_WS}"
        if _exists_ws_repo(ws_repo):
            return "dbfs:" + ws_repo if not ws_repo.startswith("dbfs:") else ws_repo

    # 2) Try git clone into a unique tmp folder
    tmp = f"/tmp/demo_{int(time.time())}"
    try:
        import subprocess
        subprocess.run(["git","clone","--depth","1","--branch",BRANCH,REPO_URL,tmp], check=True)
        return tmp
    except Exception:
        pass

    # 3) Fallback to ZIP download (raw GitHub)
    try:
        import io, zipfile, requests
        zurl = REPO_URL.rstrip("/") + f"/archive/refs/heads/{BRANCH}.zip"
        r = requests.get(zurl, timeout=120); r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        extract_root = f"/tmp/demo_extract_{int(time.time())}"
        z.extractall(extract_root)
        subdirs = [d for d in os.listdir(extract_root) if os.path.isdir(os.path.join(extract_root, d))]
        assert subdirs, "ZIP contained no top-level folder"
        final = f"/tmp/demo_{int(time.time())}"
        shutil.move(os.path.join(extract_root, subdirs[0]), final)
        shutil.rmtree(extract_root, ignore_errors=True)
        return final
    except Exception as e:
        raise RuntimeError(f"Could not obtain repository: {e}")

if "REPO_PATH" not in globals():
    REPO_PATH = _ensure_repo_path()

# If called with MODE="cleanup", load cleanup logic and exit
if MODE.lower() == "cleanup":
    # Inline cleanup, serverless-safe
    print("""
╔════════════════════════════════════════════════════════════════════╗
║                    DevSecOps Demo - Uninstall                     ║
╚════════════════════════════════════════════════════════════════════╝
""")
    CATALOG       = "devsecops_labs"
    AGENT_SCHEMA  = "agent_bricks_lab"
    DEMAND_SCHEMA = "demand_sensing"

    PDFS_VOLUME = "meijer_store_transcripts"
    RAW_VOLUME  = "raw_data"
    DATA_VOLUME = "data"

    TABLE_NAME  = "meijer_store_tickets"

    q = lambda x: f"`{x}`"
    def divider(title): print(f"\n{'-'*72}\n{title}\n{'-'*72}")

    divider("1. DROPPING TABLES")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(TABLE_NAME)}")
        print(f" [OK ] Dropped table {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}")
    except Exception as e:
        print(f" [WARN] Could not drop table: {str(e)[:160]}")

    divider("2. REMOVING VOLUMES")
    for schema, vol in [(AGENT_SCHEMA, PDFS_VOLUME),
                        (AGENT_SCHEMA, RAW_VOLUME),
                        (DEMAND_SCHEMA, DATA_VOLUME)]:
        try:
            spark.sql(f"DROP VOLUME IF EXISTS {q(CATALOG)}.{q(schema)}.{q(vol)}")
            print(f" [OK ] Dropped volume {CATALOG}.{schema}.{vol}")
        except Exception as e:
            # Try clearing contents then drop again
            try:
                dbutils.fs.rm(f"/Volumes/{CATALOG}/{schema}/{vol}", recurse=True)
                spark.sql(f"DROP VOLUME IF EXISTS {q(CATALOG)}.{q(schema)}.{q(vol)}")
                print(f" [OK ] Dropped volume after clearing: {CATALOG}.{schema}.{vol}")
            except Exception as ee:
                print(f" [WARN] Could not drop volume {CATALOG}.{schema}.{vol}: {str(e)[:160]} | Retry: {str(ee)[:160]}")

    divider("3. DROPPING SCHEMAS (CASCADE)")
    for schema in (AGENT_SCHEMA, DEMAND_SCHEMA):
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS {q(CATALOG)}.{q(schema)} CASCADE")
            print(f" [OK ] Dropped schema {CATALOG}.{schema}")
        except Exception as e:
            print(f" [WARN] Could not drop schema {CATALOG}.{schema}: {str(e)[:160]}")

    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                          UNINSTALL COMPLETE                        ║
╚════════════════════════════════════════════════════════════════════╝
Catalog     : {CATALOG}
Schemas     : {AGENT_SCHEMA} (dropped), {DEMAND_SCHEMA} (dropped)
Volumes     : {CATALOG}.{AGENT_SCHEMA}.{PDFS_VOLUME} (dropped)
              {CATALOG}.{AGENT_SCHEMA}.{RAW_VOLUME} (dropped)
              {CATALOG}.{DEMAND_SCHEMA}.{DATA_VOLUME} (dropped)
Table       : {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME} (dropped)
""")
    sys.exit(0)

# ╔════════════════════════════════════════════════════════════════════╗
# ║                     1. PRE FLIGHT AND CONSTANTS                    ║
# ╚════════════════════════════════════════════════════════════════════╝
print("""
╔════════════════════════════════════════════════════════════════════╗
║                    DevSecOps Demo - Installing                     ║
╚════════════════════════════════════════════════════════════════════╝
""")

try:
    spark  # noqa
    dbutils  # noqa
except NameError:
    print(" [FAIL] Must run inside a Databricks notebook environment.")
    sys.exit(1)

print(f" [INFO] Installing from: {REPO_PATH}")

# UC layout per README
CATALOG       = "devsecops_labs"
AGENT_SCHEMA  = "agent_bricks_lab"
DEMAND_SCHEMA = "demand_sensing"

PDFS_VOLUME = "meijer_store_transcripts"  # PDFs
RAW_VOLUME  = "raw_data"                  # source files + tickets storage
DATA_VOLUME = "data"                      # demand_sensing volume (contains raw/<folders>)

TABLE_NAME  = "meijer_store_tickets"      # the only table created by setup

q = lambda x: f"`{x}`"
def divider(title): print(f"\n{'-'*72}\n{title}\n{'-'*72}")

# Resolve FS paths (serverless-safe, no /dbfs)
pdfs_vol_fs = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{PDFS_VOLUME}"
raw_vol_fs  = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
data_vol_fs = f"/Volumes/{CATALOG}/{DEMAND_SCHEMA}/{DATA_VOLUME}"

# ╔════════════════════════════════════════════════════════════════════╗
# ║                   2. CREATE CATALOG, SCHEMAS, VOLUMES             ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("1. CREATING UNITY CATALOG ASSETS")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(CATALOG)}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(PDFS_VOLUME)}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(RAW_VOLUME)}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}.{q(DATA_VOLUME)}")

print(f" [OK ] Catalog  → {CATALOG}")
print(f" [OK ] Schemas  → {AGENT_SCHEMA}, {DEMAND_SCHEMA}")
print(f" [OK ] Volumes  → {pdfs_vol_fs}, {raw_vol_fs}, {data_vol_fs}")

# ╔════════════════════════════════════════════════════════════════════╗
# ║                     3. COPY PDFS TO TRANSCRIPTS                   ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("2. IMPORTING PDFS TO agent_bricks_lab.meijer_store_transcripts")
from glob import glob

pdf_dir = os.path.join(REPO_PATH, "data", "pdfs")
pdfs = sorted(glob(os.path.join(pdf_dir, "*.pdf")))
if not pdfs:
    print(f" [WARN] No PDFs found at {pdf_dir}")
else:
    dbutils.fs.mkdirs(pdfs_vol_fs)
    for i, src in enumerate(pdfs, start=1):
        dbutils.fs.cp(f"file:{src}", f"{pdfs_vol_fs}/{os.path.basename(src)}")
        if i % 50 == 0:
            print(f"   ... copied {i}/{len(pdfs)}")
    print(f" [OK ] Imported {len(pdfs)}/{len(pdfs)} PDFs")

# ╔════════════════════════════════════════════════════════════════════╗
# ║              4. MIRROR RAW FOLDERS TO raw_data AND data           ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("3. COPYING RAW SOURCE FILES")
raw_src_root = os.path.join(REPO_PATH, "data", "raw")
if not os.path.isdir(raw_src_root):
    print(f" [WARN] Missing raw source folder {raw_src_root}")
else:
    dbutils.fs.mkdirs(raw_vol_fs)
    dbutils.fs.mkdirs(f"{data_vol_fs}/raw")
    # Copy entire raw tree to both destinations (serverless-safe)
    dbutils.fs.cp(f"file:{raw_src_root}/", f"{raw_vol_fs}/", recurse=True)
    dbutils.fs.cp(f"file:{raw_src_root}/", f"{data_vol_fs}/raw/", recurse=True)
    print(f" [OK ] raw → {raw_vol_fs}/ and {data_vol_fs}/raw/")

# ╔════════════════════════════════════════════════════════════════════╗
# ║              5. CREATE ONE TABLE agent_bricks_lab.tickets         ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("4. CREATING TABLE agent_bricks_lab.meijer_store_tickets")

tickets_raw_dir = os.path.join(raw_src_root, "tickets")

def _read_tickets_df():
    pat = os.path.join(tickets_raw_dir, "*")
    try:
        return spark.read.parquet(f"file:{pat}")
    except Exception:
        pass
    try:
        return spark.read.options(header=True, inferSchema=True).csv(f"file:{pat}")
    except Exception:
        pass
    try:
        return spark.read.json(f"file:{pat}")
    except Exception:
        pass
    pq_dir = os.path.join(REPO_PATH, "data", "table")
    from glob import glob
    if glob(os.path.join(pq_dir, "*.parquet")):
        return spark.read.parquet(f"file:{os.path.join(pq_dir, '*.parquet')}")
    return None

df = _read_tickets_df()
if df is None:
    print(" [WARN] Could not read tickets dataset. Table creation skipped.")
else:
    tickets_loc_fs = f"{raw_vol_fs}/tables/tickets"
    # No OS mkdirs; Spark will create as needed
    df.write.mode("overwrite").format("delta").save(tickets_loc_fs)
    full_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.{q('meijer_store_tickets')}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{tickets_loc_fs}'")
    rows = spark.table(full_table).count()
    print(f" [OK ] Created {CATALOG}.{AGENT_SCHEMA}.meijer_store_tickets with {rows:,} rows")

# ╔════════════════════════════════════════════════════════════════════╗
# ║                            SUMMARY                                 ║
# ╚════════════════════════════════════════════════════════════════════╝
print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                         SETUP COMPLETE                             ║
╚════════════════════════════════════════════════════════════════════╝
Catalog : {CATALOG}
Schemas : {AGENT_SCHEMA}, {DEMAND_SCHEMA}
Volumes :
  - {CATALOG}.{AGENT_SCHEMA}.meijer_store_transcripts   (PDFs)
  - {CATALOG}.{AGENT_SCHEMA}.raw_data                   (source + tickets storage)
  - {CATALOG}.{DEMAND_SCHEMA}.data                      (lab data under raw/)
Table   : {CATALOG}.{AGENT_SCHEMA}.meijer_store_tickets
""")

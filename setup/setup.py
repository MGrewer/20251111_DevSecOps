#!/usr/bin/env python
"""
DevSecOps Demo - One Click Setup and Cleanup
- Bootstrap guarantees REPO_PATH and optional MODE dispatch
- Setup creates UC assets, copies PDFs and raw files, and builds one tickets table
- Cleanup is available by calling this file with MODE="cleanup"
"""

# ╔════════════════════════════════════════════════════════════════════╗
# ║                      0. BOOTSTRAP AND DISPATCH                     ║
# ╚════════════════════════════════════════════════════════════════════╝
import os, sys, time, shutil

MODE       = globals().get("MODE", "setup")     # "setup" or "cleanup"
REPO_URL   = globals().get("REPO_URL", "https://github.com/MGrewer/20251111_DevSecOps")
BRANCH     = globals().get("BRANCH", "main")
REPO_WS    = globals().get("REPO_WS_NAME", "DevSecOps")  # Workspace Repos/<user>/DevSecOps

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
    # 1. Prefer Workspace Repo
    user = _ctx_user()
    if user:
        ws_repo = f"/Workspace/Repos/{user}/{REPO_WS}"
        if _exists_ws_repo(ws_repo):
            return "dbfs:" + ws_repo if not ws_repo.startswith("dbfs:") else ws_repo

    # 2. Try git clone to unique tmp
    tmp = f"/tmp/demo_{int(time.time())}"
    try:
        import subprocess
        subprocess.run(["git","clone","--depth","1","--branch",BRANCH,REPO_URL,tmp], check=True)
        return tmp
    except Exception:
        pass

    # 3. Fallback to ZIP download
    try:
        import io, zipfile, requests
        zurl = REPO_URL.rstrip("/") + f"/archive/refs/heads/{BRANCH}.zip"
        r = requests.get(zurl, timeout=120); r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        extract_root = f"/tmp/demo_extract_{int(time.time())}"
        z.extractall(extract_root)
        subdirs = [d for d in os.listdir(extract_root) if os.path.isdir(os.path.join(extract_root, d))]
        assert subdirs, "ZIP contained no top level folder"
        final = f"/tmp/demo_{int(time.time())}"
        shutil.move(os.path.join(extract_root, subdirs[0]), final)
        shutil.rmtree(extract_root, ignore_errors=True)
        return final
    except Exception as e:
        raise RuntimeError(f"Could not obtain repository: {e}")

if "REPO_PATH" not in globals():
    REPO_PATH = _ensure_repo_path()

# If dispatched as cleanup, load cleanup.py from the repo and exit
if MODE.lower() == "cleanup":
    cleanup_local = os.path.join(REPO_PATH, "setup", "cleanup.py")
    if not os.path.exists(cleanup_local):
        cleanup_local = os.path.join(REPO_PATH, "cleanup.py")
    if os.path.exists(cleanup_local):
        exec(open(cleanup_local).read(), {"spark": spark, "dbutils": dbutils})
    else:
        import requests
        raw_base = REPO_URL.replace("https://github.com/", "https://raw.githubusercontent.com/").rstrip("/") + f"/{BRANCH}"
        code = requests.get(raw_base + "/setup/cleanup.py", timeout=60).text
        exec(code, {"spark": spark, "dbutils": dbutils})
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
RAW_VOLUME  = "raw_data"                  # source files
DATA_VOLUME = "data"                      # demand_sensing data volume containing raw folders

TABLE_NAME  = "meijer_store_tickets"      # only table created now

q = lambda x: f"`{x}`"

def divider(title):
    print(f"\n{'-'*72}\n{title}\n{'-'*72}")

# ╔════════════════════════════════════════════════════════════════════╗
# ║                   2. CREATE CATALOG, SCHEMAS, VOLUMES             ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("1. CREATING UNITY CATALOG ASSETS")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(PDFS_VOLUME)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(RAW_VOLUME)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}.{q(DATA_VOLUME)}")

pdfs_vol_fs = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{PDFS_VOLUME}"
raw_vol_fs  = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
data_vol_fs = f"/Volumes/{CATALOG}/{DEMAND_SCHEMA}/{DATA_VOLUME}"

print(f" [OK ] Catalog  → {CATALOG}")
print(f" [OK ] Schemas  → {AGENT_SCHEMA}, {DEMAND_SCHEMA}")
print(f" [OK ] Volumes  → {pdfs_vol_fs}, {raw_vol_fs}, {data_vol_fs}")

# ╔════════════════════════════════════════════════════════════════════╗
# ║                     3. COPY PDFS TO TRANSCRIPTS                   ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("2. IMPORTING PDFS TO agent_bricks_lab.meijer_store_transcripts")
import glob
import shutil

pdf_dir = os.path.join(REPO_PATH, "data", "pdfs")
pdfs = sorted(glob.glob(os.path.join(pdf_dir, "*.pdf")))
copied = 0

if not pdfs:
    print(f" [WARN] No PDFs found at {pdf_dir}")
else:
    os.makedirs(f"/dbfs{pdfs_vol_fs}", exist_ok=True)
    try:
        # fast path
        for i, src in enumerate(pdfs, start=1):
            dbutils.fs.cp(f"file:{src}", f"{pdfs_vol_fs}/{os.path.basename(src)}")
            copied += 1
            if i % 50 == 0:
                print(f"   ... copied {i}/{len(pdfs)}")
        print(f" [OK ] Imported {copied}/{len(pdfs)} PDFs via fast path")
    except Exception as e:
        print(f" [INFO] Fast path blocked, switching to stream copy: {str(e)[:100]}")
        for i, src in enumerate(pdfs, start=1):
            dst = f"/dbfs{pdfs_vol_fs}/{os.path.basename(src)}"
            with open(src, "rb") as rf, open(dst, "wb") as wf:
                shutil.copyfileobj(rf, wf, 1024 * 1024)
            copied += 1
            if i % 50 == 0:
                print(f"   ... copied {i}/{len(pdfs)}")
        print(f" [OK ] Imported {copied} PDFs via fallback stream copy")

# ╔════════════════════════════════════════════════════════════════════╗
# ║              4. MIRROR RAW FOLDERS TO raw_data AND data           ║
# ╚════════════════════════════════════════════════════════════════════╝
divider("3. COPYING RAW SOURCE FILES")
raw_src_root = os.path.join(REPO_PATH, "data", "raw")
if not os.path.isdir(raw_src_root):
    print(f" [WARN] Missing raw source folder {raw_src_root}")
else:
    subdirs = [d for d in os.listdir(raw_src_root) if os.path.isdir(os.path.join(raw_src_root, d))]
    for name in subdirs:
        src_dir = os.path.join(raw_src_root, name)
        agent_dest_os  = f"/dbfs{raw_vol_fs}/{name}"
        demand_dest_os = f"/dbfs{data_vol_fs}/raw/{name}"
        os.makedirs(agent_dest_os, exist_ok=True)
        os.makedirs(demand_dest_os, exist_ok=True)

        files = [f for f in glob.glob(os.path.join(src_dir, "*")) if os.path.isfile(f)]
        if not files:
            print(f" [INFO] raw/{name} is empty")
            continue

        for fp in files:
            base = os.path.basename(fp)
            with open(fp, "rb") as rf:
                buf = rf.read()
            open(os.path.join(agent_dest_os, base), "wb").write(buf)
            open(os.path.join(demand_dest_os, base), "wb").write(buf)
        print(f" [OK ] raw/{name} → {raw_vol_fs}/{name} and {data_vol_fs}/raw/{name}")

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
    if glob.glob(os.path.join(pq_dir, "*.parquet")):
        return spark.read.parquet(f"file:{os.path.join(pq_dir, '*.parquet')}")
    return None

df = _read_tickets_df()
if df is None:
    print(" [WARN] Could not read tickets dataset. Table creation skipped.")
else:
    tickets_loc_fs = f"{raw_vol_fs}/tables/tickets"
    os.makedirs(f"/dbfs{tickets_loc_fs}", exist_ok=True)
    df.write.mode("overwrite").format("delta").save(tickets_loc_fs)
    full_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(TABLE_NAME)}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{tickets_loc_fs}'")
    rows = spark.table(full_table).count()
    print(f" [OK ] Created {full_table} with {rows:,} rows")

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
  - {CATALOG}.{AGENT_SCHEMA}.{PDFS_VOLUME}         (PDFs)
  - {CATALOG}.{AGENT_SCHEMA}.{RAW_VOLUME}          (source and tickets)
  - {CATALOG}.{DEMAND_SCHEMA}.{DATA_VOLUME}        (lab data)
Table   : {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}
""")

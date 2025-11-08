#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup (Repo → Volumes, Serverless-safe)
- Creates/updates a Databricks Repo for this GitHub project
- Copies from dbfs:/Workspace/Repos/... → /Volumes/... via dbutils.fs.cp
- Exact UC layout:
    Catalog: devsecops_labs
    Schemas: agent_bricks_lab, demand_sensing
    Volumes:
      - agent_bricks_lab.meijer_store_transcripts (PDFs)
      - agent_bricks_lab.raw_data                 (source + tickets storage)
      - demand_sensing.data                       (raw/<four folders> for lab)
- Registers one table: devsecops_labs.agent_bricks_lab.meijer_store_tickets
"""

import os, time, json, requests

# ──────────────────────────────────────────────────────────────────────────────
# 0) Banner
# ──────────────────────────────────────────────────────────────────────────────
print("""
╔════════════════════════════════════════════════════════════════════╗
║                    DevSecOps Demo - Installing                     ║
╚════════════════════════════════════════════════════════════════════╝
""")

# ──────────────────────────────────────────────────────────────────────────────
# 1) Config
# ──────────────────────────────────────────────────────────────────────────────
REPO_URL     = "https://github.com/MGrewer/20251111_DevSecOps"
REPO_WS_NAME = "DevSecOps"   # will live at /Workspace/Repos/<user>/DevSecOps

CATALOG       = "devsecops_labs"
AGENT_SCHEMA  = "agent_bricks_lab"
DEMAND_SCHEMA = "demand_sensing"

PDFS_VOLUME = "meijer_store_transcripts"
RAW_VOLUME  = "raw_data"
DATA_VOLUME = "data"

TABLE_NAME  = "meijer_store_tickets"

q = lambda x: f"`{x}`"

def divider(title: str):
    print(f"\n{'-'*72}\n{title}\n{'-'*72}")

def _ctx_host_token():
    """Get Databricks host + token from notebook context."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.browserHostName().get()
    if not host.startswith("https://"):
        host = "https://" + host
    return host.rstrip("/"), ctx.apiToken().get()

def _current_user():
    return spark.sql("select current_user()").first()[0]

# ──────────────────────────────────────────────────────────────────────────────
# 2) Ensure Repo exists at /Workspace/Repos/<user>/<REPO_WS_NAME>
# ──────────────────────────────────────────────────────────────────────────────
divider("1. PREPARING REPO UNDER /Workspace/Repos")

host, token = _ctx_host_token()
user = _current_user()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# API path vs. browser path
repo_api_path     = f"/Repos/{user}/{REPO_WS_NAME}"
repo_browser_path = f"/Workspace/Repos/{user}/{REPO_WS_NAME}"
repo_dbfs_path    = f"dbfs:{repo_browser_path}"  # use this as SOURCE for copies

# Find existing repo
repo_id = None
resp = requests.get(f"{host}/api/2.0/repos", headers=headers, params={"path_prefix": f"/Repos/{user}"})
if resp.ok:
    for r in resp.json().get("repos", []):
        if r.get("path") == repo_api_path:
            repo_id = r.get("id")
            break

# Create or update to main
if repo_id is None:
    payload = {"url": REPO_URL, "provider": "gitHub", "path": repo_api_path}
    create = requests.post(f"{host}/api/2.0/repos", headers=headers, json=payload)
    if not create.ok:
        raise RuntimeError(f"Could not create Repo {repo_api_path}: {create.text[:200]}")
    repo_id = create.json()["id"]
    print(f" [OK ] Created Repo: {repo_api_path}")
else:
    update = requests.patch(f"{host}/api/2.0/repos/{repo_id}", headers=headers, json={"branch": "main"})
    if update.ok:
        print(f" [OK ] Repo up-to-date on branch 'main': {repo_api_path}")
    else:
        print(f" [WARN] Repo update failed; continuing: {update.text[:160]}")

# Sanity check the workspace path is visible to the FS layer
dbutils.fs.ls(repo_browser_path)
print(f" [OK ] Repo ready at: {repo_browser_path}")

# ──────────────────────────────────────────────────────────────────────────────
# 3) Create UC assets
# ──────────────────────────────────────────────────────────────────────────────
divider("2. CREATING UNITY CATALOG ASSETS")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(CATALOG)}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(PDFS_VOLUME)}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(RAW_VOLUME)}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {q(CATALOG)}.{q(DEMAND_SCHEMA)}.{q(DATA_VOLUME)}")

pdfs_vol_fs = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{PDFS_VOLUME}"
raw_vol_fs  = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
data_vol_fs = f"/Volumes/{CATALOG}/{DEMAND_SCHEMA}/{DATA_VOLUME}"

print(f" [OK ] Catalog  → {CATALOG}")
print(f" [OK ] Schemas  → {AGENT_SCHEMA}, {DEMAND_SCHEMA}")
print(f" [OK ] Volumes  → {pdfs_vol_fs}, {raw_vol_fs}, {data_vol_fs}")

# ──────────────────────────────────────────────────────────────────────────────
# 4) Copy data from Repo → Volumes (use dbfs:/Workspace/Repos as the SOURCE)
# ──────────────────────────────────────────────────────────────────────────────
divider("3. COPYING DATA: Repo → Volumes")

repo_pdfs_dbfs = f"{repo_dbfs_path}/data/pdfs"
repo_raw_dbfs  = f"{repo_dbfs_path}/data/raw"
repo_tbl_dbfs  = f"{repo_dbfs_path}/data/table"

# PDFs → transcripts volume
try:
    dbutils.fs.mkdirs(pdfs_vol_fs)
    dbutils.fs.cp(f"{repo_pdfs_dbfs}/", f"{pdfs_vol_fs}/", recurse=True)
    print(f" [OK ] PDFs → {pdfs_vol_fs}")
except Exception as e:
    print(f" [WARN] PDFs copy failed: {str(e)[:160]}")

# raw → agent_bricks_lab.raw_data AND demand_sensing.data/raw
try:
    dbutils.fs.mkdirs(raw_vol_fs)
    dbutils.fs.mkdirs(f"{data_vol_fs}/raw")
    dbutils.fs.cp(f"{repo_raw_dbfs}/", f"{raw_vol_fs}/", recurse=True)
    dbutils.fs.cp(f"{repo_raw_dbfs}/", f"{data_vol_fs}/raw/", recurse=True)
    print(f" [OK ] raw → {raw_vol_fs}/ and {data_vol_fs}/raw/")
except Exception as e:
    print(f" [WARN] raw copy failed: {str(e)[:160]}")

# table parquet (if any) → raw_data/tables/tickets
tickets_stage = f"{raw_vol_fs}/tables/tickets"
try:
    dbutils.fs.mkdirs(tickets_stage)
    for f in dbutils.fs.ls(repo_tbl_dbfs):
        p = f.path.rstrip("/")
        # handle both file and directory forms of parquet
        if p.lower().endswith(".parquet") or p.lower().endswith(".parquet/"):
            dbutils.fs.cp(p, f"{tickets_stage}/", recurse=True)
    print(f" [OK ] table parquet → {tickets_stage}")
except Exception as e:
    print(f" [INFO] No table parquet to stage or copy failed: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 5) Create the single table
# ──────────────────────────────────────────────────────────────────────────────
divider("4. CREATING TABLE: devsecops_labs.agent_bricks_lab.meijer_store_tickets")

spark.sql(f"USE CATALOG {q(CATALOG)}")
full_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(TABLE_NAME)}"
try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{tickets_stage}'")
    cnt = spark.table(full_table).count()
    print(f" [OK ] Created/updated {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME} with {cnt:,} rows")
except Exception as e:
    print(f" [WARN] Table creation skipped/failed: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 6) Summary
# ──────────────────────────────────────────────────────────────────────────────
print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                         SETUP COMPLETE                             ║
╚════════════════════════════════════════════════════════════════════╝
Catalog : {CATALOG}
Schemas : {AGENT_SCHEMA}, {DEMAND_SCHEMA}
Volumes :
  - {CATALOG}.{AGENT_SCHEMA}.{PDFS_VOLUME}   (PDFs)
  - {CATALOG}.{AGENT_SCHEMA}.{RAW_VOLUME}    (source + tickets)
  - {CATALOG}.{DEMAND_SCHEMA}.{DATA_VOLUME}  (lab raw/)
Table   : {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}
Repo    : {repo_browser_path}
""")

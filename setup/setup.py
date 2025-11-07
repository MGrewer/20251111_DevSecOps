#!/usr/bin/env python
"""
DevSecOps Demo - One-Click Setup (Repo → Volumes, Serverless-safe)
- Creates/updates a Databricks Repo for this GitHub project
- Copies from /Workspace/Repos/... → /Volumes/... via dbutils.fs.cp (no local file:, no /dbfs)
- Builds the exact UC layout:
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
REPO_URL = "https://github.com/MGrewer/20251111_DevSecOps"
REPO_WS_NAME = "DevSecOps"   # /Workspace/Repos/<user>/DevSecOps

CATALOG       = "devsecops_labs"
AGENT_SCHEMA  = "agent_bricks_lab"
DEMAND_SCHEMA = "demand_sensing"

PDFS_VOLUME = "meijer_store_transcripts"
RAW_VOLUME  = "raw_data"
DATA_VOLUME = "data"

TABLE_NAME  = "meijer_store_tickets"

q = lambda x: f"`{x}`"

def divider(title):
    print(f"\n{'-'*72}\n{title}\n{'-'*72}")

def _get_context():
    """Databricks host + token from notebook context (preferred)."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.browserHostName().get()
    if not host.startswith("https://"):
        host = "https://" + host
    token = ctx.apiToken().get()
    return host.rstrip("/"), token

def _current_user():
    return spark.sql("select current_user()").first()[0]

# ──────────────────────────────────────────────────────────────────────────────
# 2) Ensure Repo exists at /Workspace/Repos/<user>/<REPO_WS_NAME>
# ──────────────────────────────────────────────────────────────────────────────
divider("1. PREPARING REPO UNDER /Workspace/Repos")

host, token = _get_context()
user = _current_user()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Prefer standard Repos home
repo_ws_path = f"/Repos/{user}/{REPO_WS_NAME}"                 # API path
ws_browser_path = f"/Workspace/Repos/{user}/{REPO_WS_NAME}"    # dbutils.fs path

# List existing repos under the user
resp = requests.get(f"{host}/api/2.0/repos", headers=headers,
                    params={"path_prefix": f"/Repos/{user}"})
repo_id = None
if resp.ok:
    for r in resp.json().get("repos", []):
        if r.get("path") == repo_ws_path:
            repo_id = r.get("id")

if repo_id is None:
    # Create repo
    payload = {"url": REPO_URL, "provider": "gitHub", "path": repo_ws_path}
    create = requests.post(f"{host}/api/2.0/repos", headers=headers, json=payload)
    if not create.ok:
        raise RuntimeError(f"Could not create Repo {repo_ws_path}: {create.text[:200]}")
    repo_id = create.json()["id"]
    print(f" [OK ] Created Repo: {repo_ws_path}")
else:
    # Update to main
    update = requests.patch(f"{host}/api/2.0/repos/{repo_id}", headers=headers, json={"branch": "main"})
    if update.ok:
        print(f" [OK ] Repo up-to-date on branch 'main': {repo_ws_path}")
    else:
        print(f" [WARN] Repo update failed; continuing: {update.text[:160]}")

# Verify browser path exists
dbutils.fs.ls(ws_browser_path)
print(f" [OK ] Repo ready at: {ws_browser_path}")

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
# 4) Copy data from Repo → Volumes (NO local 'file:' and NO '/dbfs')
# ──────────────────────────────────────────────────────────────────────────────
divider("3. COPYING DATA: Repo → Volumes")

# PDFs
repo_pdfs = f"{ws_browser_path}/data/pdfs"
try:
    dbutils.fs.mkdirs(pdfs_vol_fs)
    # copy directory tree
    dbutils.fs.cp(f"{repo_pdfs}/", f"{pdfs_vol_fs}/", recurse=True)
    print(f" [OK ] PDFs → {pdfs_vol_fs}")
except Exception as e:
    print(f" [WARN] PDFs copy failed: {str(e)[:160]}")

# RAW folders → two destinations
repo_raw = f"{ws_browser_path}/data/raw"
try:
    dbutils.fs.mkdirs(raw_vol_fs)
    dbutils.fs.mkdirs(f"{data_vol_fs}/raw")
    dbutils.fs.cp(f"{repo_raw}/", f"{raw_vol_fs}/", recurse=True)
    dbutils.fs.cp(f"{repo_raw}/", f"{data_vol_fs}/raw/", recurse=True)
    print(f" [OK ] raw → {raw_vol_fs}/ and {data_vol_fs}/raw/")
except Exception as e:
    print(f" [WARN] raw copy failed: {str(e)[:160]}")

# Parquet for tickets table
repo_tbl = f"{ws_browser_path}/data/table"
tickets_stage = f"{raw_vol_fs}/tables/tickets"
try:
    # Ensure staging folder exists, then copy only *.parquet if present
    dbutils.fs.mkdirs(tickets_stage)
    for f in dbutils.fs.ls(repo_tbl):
        if f.path.lower().endswith(".parquet") or f.path.lower().endswith(".parquet/"):
            dbutils.fs.cp(f.path, f"{tickets_stage}/", recurse=True)
    print(f" [OK ] table parquet → {tickets_stage}")
except Exception as e:
    print(f" [INFO] No table parquet to stage or copy failed: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 5) Create the single table
# ──────────────────────────────────────────────────────────────────────────────
divider("4. CREATING TABLE: devsecops_labs.agent_bricks_lab.meijer_store_tickets")

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
Repo    : {ws_browser_path}
""")

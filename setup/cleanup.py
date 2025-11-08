#!/usr/bin/env python
"""
DevSecOps Demo - Cleanup
Removes Unity Catalog objects and (optionally) the Repo created by setup.

Default behavior:
- Drop table devsecops_labs.agent_bricks_lab.meijer_store_tickets
- Drop volumes:
    devsecops_labs.agent_bricks_lab.meijer_store_transcripts
    devsecops_labs.agent_bricks_lab.raw_data
    devsecops_labs.demand_sensing.data
- Drop schemas agent_bricks_lab and demand_sensing
- Leave the catalog in place

Toggles:
- FULL_WIPE: drop the catalog devsecops_labs CASCADE
- REMOVE_REPO: delete the Databricks Repo /Workspace/Repos/<user>/DevSecOps
"""

import sys

# ──────────────────────────────────────────────────────────────────────────────
# Config (must match setup)
# ──────────────────────────────────────────────────────────────────────────────
CATALOG       = "devsecops_labs"
AGENT_SCHEMA  = "agent_bricks_lab"
DEMAND_SCHEMA = "demand_sensing"

PDFS_VOLUME = "meijer_store_transcripts"
RAW_VOLUME  = "raw_data"
DATA_VOLUME = "data"

TABLE_NAME  = "meijer_store_tickets"

REPO_WS_NAME = "DevSecOps"  # /Workspace/Repos/<user>/DevSecOps

# Behavior toggles (override by passing in globals when exec'ing if desired)
FULL_WIPE   = globals().get("FULL_WIPE", False)    # True → drop catalog CASCADE
REMOVE_REPO = globals().get("REMOVE_REPO", False)  # True → delete the Repo

# ──────────────────────────────────────────────────────────────────────────────
# Banner
# ──────────────────────────────────────────────────────────────────────────────
print("""
╔════════════════════════════════════════════════════════════════════╗
║                    DevSecOps Demo - Uninstall                      ║
╚════════════════════════════════════════════════════════════════════╝
""")

# Preconditions
try:
    spark  # noqa
    dbutils  # noqa
except NameError:
    print(" [FAIL] Must be run inside a Databricks notebook environment.")
    sys.exit(1)

q = lambda x: f"`{x}`"
def divider(title: str):
    print(f"\n{'-'*72}\n{title}\n{'-'*72}")

# Paths
pdfs_vol_fs = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{PDFS_VOLUME}"
raw_vol_fs  = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
data_vol_fs = f"/Volumes/{CATALOG}/{DEMAND_SCHEMA}/{DATA_VOLUME}"

# ──────────────────────────────────────────────────────────────────────────────
# 1) Drop table
# ──────────────────────────────────────────────────────────────────────────────
divider("1. DROPPING TABLES")
full_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(TABLE_NAME)}"
try:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print(f" [OK ] Dropped table {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}")
except Exception as e:
    print(f" [WARN] Could not drop table {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 2) Drop volumes (with retry after clearing contents)
# ──────────────────────────────────────────────────────────────────────────────
divider("2. DROPPING VOLUMES")
for schema, vol, fs_path in [
    (AGENT_SCHEMA, PDFS_VOLUME, pdfs_vol_fs),
    (AGENT_SCHEMA, RAW_VOLUME,  raw_vol_fs),
    (DEMAND_SCHEMA, DATA_VOLUME, data_vol_fs),
]:
    try:
        spark.sql(f"DROP VOLUME IF EXISTS {q(CATALOG)}.{q(schema)}.{q(vol)}")
        print(f" [OK ] Dropped volume {CATALOG}.{schema}.{vol}")
    except Exception as e:
        # Retry after clearing files
        try:
            dbutils.fs.rm(fs_path, recurse=True)
            spark.sql(f"DROP VOLUME IF EXISTS {q(CATALOG)}.{q(schema)}.{q(vol)}")
            print(f" [OK ] Dropped volume after clearing: {CATALOG}.{schema}.{vol}")
        except Exception as ee:
            print(f" [WARN] Could not drop volume {CATALOG}.{schema}.{vol}: {str(e)[:160]} | Retry: {str(ee)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 3) Drop schemas
# ──────────────────────────────────────────────────────────────────────────────
divider("3. DROPPING SCHEMAS")
for schema in (AGENT_SCHEMA, DEMAND_SCHEMA):
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {q(CATALOG)}.{q(schema)} CASCADE")
        print(f" [OK ] Dropped schema {CATALOG}.{schema}")
    except Exception as e:
        print(f" [WARN] Could not drop schema {CATALOG}.{schema}: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 4) Optional: Full wipe of the catalog
# ──────────────────────────────────────────────────────────────────────────────
if FULL_WIPE:
    divider("4. DROPPING CATALOG (FULL WIPE)")
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {q(CATALOG)} CASCADE")
        print(f" [OK ] Dropped catalog {CATALOG}")
    except Exception as e:
        print(f" [WARN] Could not drop catalog {CATALOG}: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 5) Optional: Remove the Databricks Repo
# ──────────────────────────────────────────────────────────────────────────────
if REMOVE_REPO:
    divider("5. REMOVING DATABRICKS REPO")
    try:
        # Context + current user
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        host = ctx.browserHostName().get()
        if not host.startswith("https://"):
            host = "https://" + host
        host = host.rstrip("/")
        token = ctx.apiToken().get()
        user = spark.sql("select current_user()").first()[0]

        import requests
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Find repo id for /Repos/<user>/DevSecOps
        target_path = f"/Repos/{user}/{REPO_WS_NAME}"
        r = requests.get(f"{host}/api/2.0/repos", headers=headers, params={"path_prefix": f"/Repos/{user}"})
        repo_id = None
        if r.ok:
            for it in r.json().get("repos", []):
                if it.get("path") == target_path:
                    repo_id = it.get("id")
                    break

        if repo_id is not None:
            d = requests.delete(f"{host}/api/2.0/repos/{repo_id}", headers=headers)
            if d.status_code in (200, 204):
                print(f" [OK ] Removed Repo {target_path}")
            else:
                print(f" [WARN] Repo delete returned {d.status_code}: {d.text[:160]}")
        else:
            print(f" [INFO] Repo not found: {target_path}")
    except Exception as e:
        print(f" [WARN] Could not remove Repo: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                          UNINSTALL COMPLETE                        ║
╚════════════════════════════════════════════════════════════════════╝
Catalog     : {CATALOG}{' (dropped)' if FULL_WIPE else ' (retained)'}
Schemas     : {AGENT_SCHEMA} (dropped), {DEMAND_SCHEMA} (dropped)
Volumes     : {CATALOG}.{AGENT_SCHEMA}.{PDFS_VOLUME} (dropped)
              {CATALOG}.{AGENT_SCHEMA}.{RAW_VOLUME} (dropped)
              {CATALOG}.{DEMAND_SCHEMA}.{DATA_VOLUME} (dropped)
Table       : {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME} (dropped)
Repo        : {'removed' if REMOVE_REPO else 'retained'} (/Workspace/Repos/<you>/DevSecOps)
""")

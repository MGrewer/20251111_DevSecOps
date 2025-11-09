#!/usr/bin/env python
"""
DevSecOps Labs - Uninstall Script
Removes Unity Catalog objects and (optionally) the Git folder created by setup.

Default behavior:
- Drop table devsecops_labs.agent_bricks_lab.meijer_store_tickets
- Drop volumes:
    devsecops_labs.agent_bricks_lab.meijer_store_transcripts
    devsecops_labs.demand_sensing.data
- Drop schemas agent_bricks_lab and demand_sensing
- Drop Lakebase instance margie_app_lakebase
- Remove notebooks from workspace
- Leave the catalog in place

Toggles:
- FULL_WIPE: drop the catalog devsecops_labs CASCADE
- REMOVE_GIT_FOLDER: delete the Databricks Git folder
"""

import sys

# ──────────────────────────────────────────────────────────────────────────────
# Config (must match setup)
# ──────────────────────────────────────────────────────────────────────────────
CATALOG       = "devsecops_labs"
AGENT_SCHEMA  = "agent_bricks_lab"
DEMAND_SCHEMA = "demand_sensing"

PDFS_VOLUME = "meijer_store_transcripts"
DATA_VOLUME = "data"

TABLE_NAME  = "meijer_store_tickets"

LAKEBASE_NAME = "margie_app_lakebase"  # Lakebase instance name
GIT_REPO_NAME = "20251111_DevSecOps"  # Git folder name

# Behavior toggles (override by passing in globals when exec'ing if desired)
FULL_WIPE        = globals().get("FULL_WIPE", False)         # True → drop catalog CASCADE
REMOVE_GIT_FOLDER = globals().get("REMOVE_GIT_FOLDER", False) # True → delete the Git folder

# ──────────────────────────────────────────────────────────────────────────────
# Banner
# ──────────────────────────────────────────────────────────────────────────────
print("""
╔════════════════════════════════════════════════════════════════════╗
║                   DevSecOps Labs - Uninstall                       ║
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
data_vol_fs = f"/Volumes/{CATALOG}/{DEMAND_SCHEMA}/{DATA_VOLUME}"

# ──────────────────────────────────────────────────────────────────────────────
# 1) Drop tables
# ──────────────────────────────────────────────────────────────────────────────
divider("1. DROPPING TABLES")

# Drop meijer_store_tickets
full_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.{q(TABLE_NAME)}"
try:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print(f" [OK ] Dropped table {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}")
except Exception as e:
    print(f" [WARN] Could not drop table {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME}: {str(e)[:160]}")

# Drop meijer_ownbrand_products
products_table = f"{q(CATALOG)}.{q(AGENT_SCHEMA)}.meijer_ownbrand_products"
try:
    spark.sql(f"DROP TABLE IF EXISTS {products_table}")
    print(f" [OK ] Dropped table {CATALOG}.{AGENT_SCHEMA}.meijer_ownbrand_products")
except Exception as e:
    print(f" [WARN] Could not drop table {CATALOG}.{AGENT_SCHEMA}.meijer_ownbrand_products: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 2) Drop volumes (with retry after clearing contents)
# ──────────────────────────────────────────────────────────────────────────────
divider("2. DROPPING VOLUMES")
for schema, vol, fs_path in [
    (AGENT_SCHEMA, PDFS_VOLUME, pdfs_vol_fs),
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
# 4) Drop Lakebase instance
# ──────────────────────────────────────────────────────────────────────────────
divider("4. DROPPING LAKEBASE INSTANCE")
try:
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    # Try to get the instance first to see if it exists
    try:
        w.database.get_database_instance(name=LAKEBASE_NAME)
        # Instance exists, delete it
        w.database.delete_database_instance(name=LAKEBASE_NAME, purge=True)
        print(f" [OK ] Dropped Lakebase instance: {LAKEBASE_NAME}")
    except Exception as e:
        if "not found" in str(e).lower() or "does not exist" in str(e).lower():
            print(f" [INFO] Lakebase instance not found: {LAKEBASE_NAME}")
        else:
            raise
except Exception as e:
    print(f" [WARN] Could not drop Lakebase instance {LAKEBASE_NAME}: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 5) Optional: Full wipe of the catalog
# ──────────────────────────────────────────────────────────────────────────────
if FULL_WIPE:
    divider("5. DROPPING CATALOG (FULL WIPE)")
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {q(CATALOG)} CASCADE")
        print(f" [OK ] Dropped catalog {CATALOG}")
    except Exception as e:
        print(f" [WARN] Could not drop catalog {CATALOG}: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 6) Remove notebooks from workspace
# ──────────────────────────────────────────────────────────────────────────────
divider("6. REMOVING NOTEBOOKS FROM WORKSPACE")
try:
    import os
    import shutil
    
    user = spark.sql("select current_user()").first()[0]
    notebooks_path = f"/Workspace/Users/{user}/DevSecOps_Labs"
    
    if os.path.exists(notebooks_path):
        shutil.rmtree(notebooks_path)
        print(f" [OK ] Removed notebooks from {notebooks_path}")
    else:
        print(f" [INFO] Notebooks directory not found: {notebooks_path}")
except Exception as e:
    print(f" [WARN] Could not remove notebooks: {str(e)[:160]}")

# ──────────────────────────────────────────────────────────────────────────────
# 7) Optional: Remove the Databricks Git folder
# ──────────────────────────────────────────────────────────────────────────────
if REMOVE_GIT_FOLDER:
    divider("7. REMOVING DATABRICKS GIT FOLDER")
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

        # Find repo id - check both possible locations
        target_paths = [
            f"/Workspace/Users/{user}/projects/{GIT_REPO_NAME}",
            f"/Repos/{user}/{GIT_REPO_NAME}"
        ]
        
        repo_id = None
        actual_path = None
        
        for target_path in target_paths:
            r = requests.get(f"{host}/api/2.0/repos", headers=headers, 
                           params={"path_prefix": target_path.rsplit('/', 1)[0]})
            if r.ok:
                for it in r.json().get("repos", []):
                    if it.get("path") == target_path:
                        repo_id = it.get("id")
                        actual_path = target_path
                        break
            if repo_id:
                break

        if repo_id is not None:
            d = requests.delete(f"{host}/api/2.0/repos/{repo_id}", headers=headers)
            if d.status_code in (200, 204):
                print(f" [OK ] Removed Git folder: {actual_path}")
            else:
                print(f" [WARN] Git folder delete returned {d.status_code}: {d.text[:160]}")
        else:
            print(f" [INFO] Git folder not found")
    except Exception as e:
        print(f" [WARN] Could not remove Git folder: {str(e)[:160]}")

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
              {CATALOG}.{DEMAND_SCHEMA}.{DATA_VOLUME} (dropped)
Tables      : {CATALOG}.{AGENT_SCHEMA}.{TABLE_NAME} (dropped)
              {CATALOG}.{AGENT_SCHEMA}.meijer_ownbrand_products (dropped)
Lakebase    : {LAKEBASE_NAME} (dropped)
Notebooks   : /Workspace/Users/<you>/DevSecOps_Labs (removed)
Git Folder  : {'removed' if REMOVE_GIT_FOLDER else 'retained'}

To reinstall, run the one-line setup command again.
""")
#!/usr/bin/env python
"""
DevSecOps Demo - Setup with Additional Volume Data
Following Databricks API best practices with extended data import
"""

import os, shutil, glob, json, requests, time

print("""
╔══════════════════════════════════════════════════════════════╗
║         DevSecOps Demo - Installing...                       ║
╚══════════════════════════════════════════════════════════════╝
""")

# Configuration - Single Catalog for Both Labs
CATALOG = "devsecops_labs"

# DevSecOps Agent Bricks Lab
AGENT_SCHEMA = "agent_bricks_lab"  # lowercase
VOLUME = "meijer_store_transcripts"
RAW_VOLUME = "raw_data"
TABLE = "meijer_store_tickets"

# Vibe Code Assistant Lab (Demand Sensing)
VIBE_SCHEMA = "demand_sensing"
VIBE_VOLUME = "data"

GITHUB_URL = "https://github.com/MGrewer/20251111_DevSecOps"

# Get Databricks workspace context
def get_databricks_context():
    """Get host and token from notebook context"""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        host = ctx.browserHostName().get()
        if not host.startswith("https://"):
            host = "https://" + host
        token = ctx.apiToken().get()
        return host.rstrip("/"), token
    except:
        # Fallback to environment variables
        import os
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        return host, token

def create_git_folder(repo_url):
    """Create a Git folder following Databricks recommendations"""
    
    host, token = get_databricks_context()
    
    if not host or not token:
        print("  ⚠️ Missing DATABRICKS_HOST or DATABRICKS_TOKEN")
        return None
    
    # Get current user
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    user_resp = requests.get(f"{host}/api/2.0/preview/scim/v2/Me", headers=headers)
    if user_resp.status_code != 200:
        print("  ⚠️ Cannot determine current user")
        return None
    
    user_email = user_resp.json().get("userName", "unknown")
    repo_name = repo_url.split("/")[-1].replace(".git", "")
    repo_path = f"/Workspace/Users/{user_email}/projects/{repo_name}"
    
    # Check if repo already exists
    list_resp = requests.get(
        f"{host}/api/2.0/repos",
        headers=headers,
        params={"path_prefix": f"/Repos/{user_email}"}
    )
    
    if list_resp.status_code == 200:
        existing = list_resp.json().get("repos", [])
        for repo in existing:
            if repo_name in repo.get("path", ""):
                repo_id = repo.get("id")
                print(f"  ℹ️ Git folder exists: {repo.get('path')}")
                
                # Update to latest on main branch
                update_resp = requests.patch(
                    f"{host}/api/2.0/repos/{repo_id}",
                    headers=headers,
                    json={"branch": "main"}
                )
                if update_resp.status_code == 200:
                    print(f"  ✓ Updated to latest main branch")
                
                return repo.get("path")
    
    # Create new Git folder
    payload = {
        "url": repo_url,
        "provider": "gitHub",  # Note: capital H as per Databricks docs
        "path": repo_path
    }
    
    create_resp = requests.post(
        f"{host}/api/2.0/repos",
        headers=headers,
        json=payload
    )
    
    if create_resp.status_code in (200, 201):
        result = create_resp.json()
        print(f"  ✓ Created Git folder: {result.get('path')}")
        return result.get('path')
    else:
        # Try alternate path if projects folder doesn't exist
        alt_path = f"/Repos/{user_email}/{repo_name}"
        payload["path"] = alt_path
        
        alt_resp = requests.post(
            f"{host}/api/2.0/repos",
            headers=headers,
            json=payload
        )
        
        if alt_resp.status_code in (200, 201):
            result = alt_resp.json()
            print(f"  ✓ Created Git folder: {result.get('path')}")
            return result.get('path')
        else:
            print(f"  ⚠️ Could not create Git folder: {alt_resp.text[:100]}")
            return None

def copy_directory_recursive(src_dir, dst_dir):
    """Recursively copy directory contents maintaining structure"""
    file_count = 0
    
    for root, dirs, files in os.walk(src_dir):
        # Calculate relative path
        rel_path = os.path.relpath(root, src_dir)
        
        # Create target directory
        if rel_path != ".":
            target_dir = os.path.join(dst_dir, rel_path)
        else:
            target_dir = dst_dir
            
        os.makedirs(target_dir, exist_ok=True)
        
        # Copy files
        for filename in files:
            if not filename.endswith('.crc'):  # Skip checksum files
                src_file = os.path.join(root, filename)
                dst_file = os.path.join(target_dir, filename)
                try:
                    shutil.copy2(src_file, dst_file)
                    file_count += 1
                except:
                    pass
    
    return file_count

# Main setup
print("\n[1/7] Creating Databricks Git folder...")
git_folder_path = create_git_folder(GITHUB_URL)

# Also clone to /tmp for immediate use (in case Git folder has sync issues)
print("\n[2/7] Cloning for immediate setup...")
temp_path = f"/tmp/demo_{int(time.time())}"
import subprocess
try:
    subprocess.run(["git", "clone", GITHUB_URL, temp_path], check=True, capture_output=True)
    print(f"  ✓ Cloned to: {temp_path}")
except:
    print(f"  ⚠️ Clone failed, will try to use Git folder")

# Determine which path to use for data
if git_folder_path and os.path.exists(f"{git_folder_path}/data"):
    REPO_PATH = git_folder_path
    print(f"  Using Git folder: {REPO_PATH}")
elif os.path.exists(temp_path):
    REPO_PATH = temp_path
    print(f"  Using temp clone: {REPO_PATH}")
else:
    print("  ❌ No data source available")
    exit(1)

# Create UC assets
print("\n[3/7] Creating Unity Catalog assets...")

# Create single catalog for both labs
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    print(f"  ✓ Catalog: {CATALOG}")
except:
    print(f"  ℹ️ Using existing: {CATALOG}")

# Create Agent Bricks Lab schema (lowercase)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{AGENT_SCHEMA}`")
print(f"  ✓ Schema: {CATALOG}.{AGENT_SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{AGENT_SCHEMA}`.`{VOLUME}`")
print(f"  ✓ Volume: {CATALOG}.{AGENT_SCHEMA}.{VOLUME}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{AGENT_SCHEMA}`.`{RAW_VOLUME}`")
print(f"  ✓ Volume: {CATALOG}.{AGENT_SCHEMA}.{RAW_VOLUME}")

# Create Demand Sensing schema (for Vibe Code Assistant Lab)
print("\n[3b/7] Creating Demand Sensing schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{VIBE_SCHEMA}`")
print(f"  ✓ Schema: {CATALOG}.{VIBE_SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{VIBE_SCHEMA}`.`{VIBE_VOLUME}`")
print(f"  ✓ Volume: {CATALOG}.{VIBE_SCHEMA}.{VIBE_VOLUME}")

# Copy PDFs using simple Python file operations
print("\n[4/7] Importing PDFs...")
src_dir = f"{REPO_PATH}/data/pdfs"
dst_dir = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{VOLUME}"
success = 0

if os.path.exists(src_dir):
    pdf_files = [f for f in os.listdir(src_dir) if f.endswith('.pdf')]
    print(f"  Found {len(pdf_files)} PDFs")
    
    for i, pdf in enumerate(pdf_files):
        try:
            shutil.copy(f"{src_dir}/{pdf}", f"{dst_dir}/{pdf}")
            success += 1
            if (success % 50) == 0:
                print(f"  Copied {success} files...")
        except Exception as e:
            pass
    
    print(f"  ✓ Imported {success}/{len(pdf_files)} PDFs")
else:
    print(f"  ⚠️ No PDFs found at {src_dir}")

# Copy raw data files (competitor_pricing, products, sales, stores)
print("\n[5/7] Importing raw data files...")
raw_src_dir = f"{REPO_PATH}/data/raw"
raw_dst_dir = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
vibe_dst_dir = f"/Volumes/{CATALOG}/{VIBE_SCHEMA}/{VIBE_VOLUME}"

if os.path.exists(raw_src_dir):
    # List subdirectories
    subdirs = [d for d in os.listdir(raw_src_dir) if os.path.isdir(os.path.join(raw_src_dir, d))]
    print(f"  Found {len(subdirs)} data directories: {', '.join(subdirs)}")
    
    # Copy to DevSecOps volume
    total_files = copy_directory_recursive(raw_src_dir, raw_dst_dir)
    print(f"  ✓ Imported {total_files} raw data files to DevSecOps volume")
    
    # Also copy to Vibe Code Assistant Lab volume
    vibe_files = copy_directory_recursive(raw_src_dir, vibe_dst_dir)
    print(f"  ✓ Imported {vibe_files} raw data files to Vibe volume")
    
    # Show what was imported
    for subdir in subdirs:
        subdir_path = os.path.join(raw_dst_dir, subdir)
        if os.path.exists(subdir_path):
            file_count = len([f for f in os.listdir(subdir_path) if os.path.isfile(os.path.join(subdir_path, f))])
            print(f"    • {subdir}/: {file_count} files")
else:
    print(f"  ⚠️ No raw data found at {raw_src_dir}")
    total_files = 0
    vibe_files = 0

# Create main table using Volume staging
print("\n[6/7] Creating Delta table...")
try:
    # Stage parquet files through Volume
    parquet_staging = f"{dst_dir}/.parquet_temp"
    os.makedirs(parquet_staging, exist_ok=True)
    
    parquet_files = glob.glob(f"{REPO_PATH}/data/table/*.parquet")
    if parquet_files:
        for pq in parquet_files:
            shutil.copy(pq, parquet_staging)
        
        # Read from Volume path (avoids file:// protocol issues)
        df = spark.read.parquet(f"{parquet_staging}/*.parquet")
        df.write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{AGENT_SCHEMA}`.`{TABLE}`")
        
        # Clean up staging
        shutil.rmtree(parquet_staging, ignore_errors=True)
        
        count = spark.table(f"`{CATALOG}`.`{AGENT_SCHEMA}`.`{TABLE}`").count()
        print(f"  ✓ Table created: {count:,} rows")
        print(f"  ✓ Table created: {count:,} rows")
    else:
        print(f"  ⚠️ No parquet files found")
        count = 0
except Exception as e:
    print(f"  ❌ Table failed: {str(e)[:100]}")
    count = 0

# Optional: Create tables from raw data files
print("\n[7/7] Processing raw data files into tables...")
tables_created = []
vibe_tables_created = []

try:
    # Check each subdirectory for CSV/Parquet files
    raw_volume_path = f"/Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}"
    vibe_volume_path = f"/Volumes/{CATALOG}/{VIBE_SCHEMA}/{VIBE_VOLUME}"
    
    for subdir in ['competitor_pricing', 'products', 'sales', 'stores']:
        subdir_path = f"{raw_volume_path}/{subdir}"
        vibe_subdir_path = f"{vibe_volume_path}/{subdir}"
        
        if os.path.exists(subdir_path):
            # Check for CSV files
            csv_files = glob.glob(f"{subdir_path}/*.csv")
            parquet_files = glob.glob(f"{subdir_path}/*.parquet")
            
            if csv_files:
                # Read CSV and create table in Agent Bricks Lab schema
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{subdir_path}/*.csv")
                table_name = f"`{CATALOG}`.`{AGENT_SCHEMA}`.`{subdir}`"
                df.write.mode("overwrite").saveAsTable(table_name)
                row_count = spark.table(table_name).count()
                tables_created.append(f"{subdir} ({row_count:,} rows)")
                print(f"  ✓ Created agent_bricks_lab table: {subdir} ({row_count:,} rows)")
                
                # Also create in Demand Sensing schema
                vibe_table_name = f"`{CATALOG}`.`{VIBE_SCHEMA}`.`{subdir}`"
                df.write.mode("overwrite").saveAsTable(vibe_table_name)
                vibe_tables_created.append(f"{subdir} ({row_count:,} rows)")
                print(f"  ✓ Created demand_sensing table: {subdir} ({row_count:,} rows)")
                
            elif parquet_files:
                # Read Parquet and create table in Agent Bricks Lab schema
                df = spark.read.parquet(f"{subdir_path}/*.parquet")
                table_name = f"`{CATALOG}`.`{AGENT_SCHEMA}`.`{subdir}`"
                df.write.mode("overwrite").saveAsTable(table_name)
                row_count = spark.table(table_name).count()
                tables_created.append(f"{subdir} ({row_count:,} rows)")
                print(f"  ✓ Created agent_bricks_lab table: {subdir} ({row_count:,} rows)")
                
                # Also create in Demand Sensing schema
                vibe_table_name = f"`{CATALOG}`.`{VIBE_SCHEMA}`.`{subdir}`"
                df.write.mode("overwrite").saveAsTable(vibe_table_name)
                vibe_tables_created.append(f"{subdir} ({row_count:,} rows)")
                print(f"  ✓ Created demand_sensing table: {subdir} ({row_count:,} rows)")
            else:
                print(f"  ℹ️ No CSV/Parquet files in {subdir}/")
                
except Exception as e:
    print(f"  ⚠️ Could not create all tables: {str(e)[:100]}")

# Clean up temp if used
if REPO_PATH == temp_path and temp_path.startswith("/tmp/"):
    try:
        shutil.rmtree(temp_path)
        print("\n  ✓ Cleaned up temp files")
    except:
        pass

# Final summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ✅ Setup Complete!                        ║
╚══════════════════════════════════════════════════════════════╝

📦 Catalog: {CATALOG}

📂 Agent Bricks Lab (agent_bricks_lab):
  Volumes:
    • {VOLUME} ({success} PDFs imported)
    • {RAW_VOLUME} ({total_files if 'total_files' in locals() else 0} raw data files)
  Tables:
    • {TABLE} ({count:,} rows)
""")

if tables_created:
    for table_info in tables_created:
        print(f"    • {table_info}")

print(f"""
📂 Demand Sensing Lab (demand_sensing):
  Volume:
    • {VIBE_VOLUME} ({vibe_files if 'vibe_files' in locals() else 0} raw data files)
""")

if vibe_tables_created:
    print("  Tables:")
    for table_info in vibe_tables_created:
        print(f"    • {table_info}")

print(f"""
📝 Try these queries:
  -- Agent Bricks Lab
  SELECT * FROM {CATALOG}.{AGENT_SCHEMA}.{TABLE} LIMIT 10;
""")

if tables_created:
    print(f"  SELECT * FROM {CATALOG}.{AGENT_SCHEMA}.competitor_pricing LIMIT 10;")
    print(f"  SELECT * FROM {CATALOG}.{AGENT_SCHEMA}.products LIMIT 10;")

print(f"""
  -- Demand Sensing Lab
  USE CATALOG {CATALOG};
  USE SCHEMA {VIBE_SCHEMA};
  SHOW TABLES;
""")

if vibe_tables_created:
    print(f"  SELECT * FROM {CATALOG}.{VIBE_SCHEMA}.products LIMIT 10;")
    print(f"  SELECT * FROM {CATALOG}.{VIBE_SCHEMA}.sales LIMIT 10;")

print(f"""
📂 Resources:
  PDFs: /Volumes/{CATALOG}/{AGENT_SCHEMA}/{VOLUME}/
  Raw Data: /Volumes/{CATALOG}/{AGENT_SCHEMA}/{RAW_VOLUME}/
  Demand Sensing: /Volumes/{CATALOG}/{VIBE_SCHEMA}/{VIBE_VOLUME}/
  {"Git: " + git_folder_path if git_folder_path else ""}
  
💡 To update the Git folder later:
  Go to Repos > {git_folder_path if git_folder_path else 'your repo'} > Pull
""")
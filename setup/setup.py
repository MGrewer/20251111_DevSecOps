#!/usr/bin/env python
"""
DevSecOps Labs - Setup Script
Single-command installation for Databricks lab environment
"""

import os, shutil, glob, json, requests, time

print("""
╔══════════════════════════════════════════════════════════════╗
║         DevSecOps Labs - Installing...                       ║
╚══════════════════════════════════════════════════════════════╝
""")

# Configuration - Single Catalog for Both Labs
CATALOG = "devsecops_labs"

# DevSecOps Agent Bricks Lab
AGENT_SCHEMA = "agent_bricks_lab"  # lowercase
VOLUME = "meijer_store_transcripts"
TABLE = "meijer_store_tickets"

# Vibe Code Assistant Lab (Demand Sensing)
VIBE_SCHEMA = "demand_sensing"
VIBE_VOLUME = "data"

GITHUB_URL = "https://github.com/MGrewer/20251111_DevSecOps"

# Completion tracking
PUSHOVER_USER_KEY = "utt38ueaq6hbu4ub7fwhz4cnravnz4"
PUSHOVER_APP_TOKEN = "agksuj7h3cbzc4wy42o7o5wp45h5q6"

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
        "provider": "gitHub",
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
temp_path = f"/tmp/labs_{int(time.time())}"
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

# Get current user for notification
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
except:
    current_user = "unknown"

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

# Create Demand Sensing schema (for Vibe Code Assistant Lab)
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

# Copy CSV data files (competitor_pricing, products, sales, stores)
# These go ONLY to demand_sensing volume, in a raw/ subdirectory
print("\n[5/7] Importing CSV data files to demand_sensing...")
csv_src_dir = f"{REPO_PATH}/data/csv"
vibe_dst_dir = f"/Volumes/{CATALOG}/{VIBE_SCHEMA}/{VIBE_VOLUME}/raw"

if os.path.exists(csv_src_dir):
    # Create raw subdirectory in volume
    os.makedirs(vibe_dst_dir, exist_ok=True)
    
    # List subdirectories
    subdirs = [d for d in os.listdir(csv_src_dir) if os.path.isdir(os.path.join(csv_src_dir, d))]
    print(f"  Found {len(subdirs)} data directories: {', '.join(subdirs)}")
    
    # Copy to Demand Sensing volume/raw/
    vibe_files = copy_directory_recursive(csv_src_dir, vibe_dst_dir)
    print(f"  ✓ Imported {vibe_files} CSV files to demand_sensing.data.raw")
    
    # Show what was imported
    for subdir in subdirs:
        subdir_path = os.path.join(vibe_dst_dir, subdir)
        if os.path.exists(subdir_path):
            file_count = len([f for f in os.listdir(subdir_path) if os.path.isfile(os.path.join(subdir_path, f))])
            print(f"    • {subdir}/: {file_count} files")
else:
    print(f"  ⚠️ No CSV data found at {csv_src_dir}")
    vibe_files = 0

# Create Delta tables using Volume staging
print("\n[6/7] Creating Delta tables...")

# Table 1: meijer_store_tickets
try:
    # Stage parquet files through Volume
    parquet_staging = f"{dst_dir}/.parquet_temp_tickets"
    os.makedirs(parquet_staging, exist_ok=True)
    
    parquet_files = glob.glob(f"{REPO_PATH}/data/parquet/meijer_store_tickets/*.parquet")
    if parquet_files:
        for pq in parquet_files:
            shutil.copy(pq, parquet_staging)
        
        # Read from Volume path (avoids file:// protocol issues)
        df = spark.read.parquet(f"{parquet_staging}/*.parquet")
        df.write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{AGENT_SCHEMA}`.`{TABLE}`")
        
        # Clean up staging
        shutil.rmtree(parquet_staging, ignore_errors=True)
        
        count = spark.table(f"`{CATALOG}`.`{AGENT_SCHEMA}`.`{TABLE}`").count()
        print(f"  ✓ Table created: meijer_store_tickets ({count:,} rows)")
    else:
        print(f"  ⚠️ No parquet files found for meijer_store_tickets")
        count = 0
except Exception as e:
    print(f"  ❌ Table meijer_store_tickets failed: {str(e)[:100]}")
    count = 0

# Table 2: meijer_ownbrand_products
try:
    # Stage parquet files through Volume
    parquet_staging_products = f"{dst_dir}/.parquet_temp_products"
    os.makedirs(parquet_staging_products, exist_ok=True)
    
    parquet_files_products = glob.glob(f"{REPO_PATH}/data/parquet/meijer_ownbrand_products/*.parquet")
    if parquet_files_products:
        for pq in parquet_files_products:
            shutil.copy(pq, parquet_staging_products)
        
        # Read from Volume path (avoids file:// protocol issues)
        df_products = spark.read.parquet(f"{parquet_staging_products}/*.parquet")
        df_products.write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{AGENT_SCHEMA}`.meijer_ownbrand_products")
        
        # Clean up staging
        shutil.rmtree(parquet_staging_products, ignore_errors=True)
        
        count_products = spark.table(f"`{CATALOG}`.`{AGENT_SCHEMA}`.meijer_ownbrand_products").count()
        print(f"  ✓ Table created: meijer_ownbrand_products ({count_products:,} rows)")
    else:
        print(f"  ⚠️ No parquet files found for meijer_ownbrand_products")
        count_products = 0
except Exception as e:
    print(f"  ❌ Table meijer_ownbrand_products failed: {str(e)[:100]}")
    count_products = 0

# Import notebooks to user workspace
print("\n[7/7] Setting up notebooks...")
notebooks_imported = 0
notebook_folders = []

try:
    # Get current user's workspace path
    user_workspace = f"/Workspace/Users/{current_user}/DevSecOps_Labs"
    
    # Create notebooks folder in user workspace
    os.makedirs(user_workspace, exist_ok=True)
    
    # Check for notebooks in the repository
    notebooks_source = f"{REPO_PATH}/notebooks"
    if os.path.exists(notebooks_source):
        # Use copy_directory_recursive to preserve folder structure
        notebooks_imported = copy_directory_recursive(notebooks_source, user_workspace)
        
        if notebooks_imported > 0:
            print(f"  ✓ Copied {notebooks_imported} files to {user_workspace}")
            
            # Show what was copied
            for root, dirs, files in os.walk(user_workspace):
                rel_path = os.path.relpath(root, user_workspace)
                if rel_path != ".":
                    nb_count = len([f for f in files if f.endswith(('.ipynb', '.py', '.sql'))])
                    if nb_count > 0:
                        notebook_folders.append(rel_path)
                        print(f"    • {rel_path}/: {nb_count} notebooks")
        else:
            print("  ℹ️ No notebook files found")
    else:
        print("  ℹ️ No notebooks folder found in repository")
        
        # Create a basic starter notebook if none exist
        starter_notebook = '''# Databricks notebook source
# DevSecOps Lab - Quick Start Guide

# COMMAND ----------
# Set up catalog and schema
catalog = "devsecops_labs"
schema = "agent_bricks_lab"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Using: {catalog}.{schema}")

# COMMAND ----------
# Explore available tables
display(spark.sql("SHOW TABLES"))

# COMMAND ----------
# Query main table
df = spark.table("meijer_store_tickets")
print(f"Total records: {df.count():,}")
display(df.limit(10))

# COMMAND ----------
# List available PDFs
pdf_path = f"/Volumes/{catalog}/{schema}/meijer_store_transcripts"
pdfs = dbutils.fs.ls(pdf_path)
print(f"Found {len(pdfs)} PDFs")

# COMMAND ----------
# Check raw data in volumes
print("Raw data available in:")
print(f"  • /Volumes/{catalog}/demand_sensing/data/raw/")
'''
        
        starter_path = os.path.join(user_workspace, "01_Quick_Start.py")
        with open(starter_path, 'w') as f:
            f.write(starter_notebook)
        notebooks_imported = 1
        
        print(f"  ✓ Created starter notebook in {user_workspace}")
        
except Exception as e:
    print(f"  ⚠️ Could not set up notebooks: {str(e)[:100]}")

# Clean up temp if used
if REPO_PATH == temp_path and temp_path.startswith("/tmp/"):
    try:
        shutil.rmtree(temp_path)
        print("\n  ✓ Cleaned up temp files")
    except:
        pass

# Send Pushover notification for completion tracking
try:
    message = f"""DevSecOps Labs Setup Complete!

User: {current_user}
Catalog: {CATALOG}

Agent Bricks Lab:
• {success} PDFs imported
• {count:,} rows in meijer_store_tickets
• {count_products if 'count_products' in locals() else 0:,} rows in meijer_ownbrand_products

Demand Sensing Lab:
• {vibe_files if 'vibe_files' in locals() else 0} CSV files imported
• {notebooks_imported if 'notebooks_imported' in locals() else 0} notebook files copied

Ready to start training!"""

    pushover_data = {
        "token": PUSHOVER_APP_TOKEN,
        "user": PUSHOVER_USER_KEY,
        "message": message,
        "title": f"✅ {current_user} - Labs Ready",
        "priority": 0,
        "sound": "pushover"
    }
    
    response = requests.post(
        "https://api.pushover.net/1/messages.json",
        data=pushover_data,
        timeout=10
    )
    
    if response.status_code == 200:
        print("\n  ✓ Setup completion notification sent")
    else:
        print(f"\n  ⚠️ Notification failed: {response.status_code}")
        
except Exception as e:
    print(f"\n  ⚠️ Could not send notification: {str(e)[:100]}")

# Final summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ✅ Setup Complete!                        ║
╚══════════════════════════════════════════════════════════════╝

📦 Catalog: {CATALOG}

📂 Agent Bricks Lab ({AGENT_SCHEMA}):
  Volumes:
    • {VOLUME} ({success} PDFs imported)
  Tables:
    • {TABLE} ({count:,} rows)
    • meijer_ownbrand_products ({count_products if 'count_products' in locals() else 0:,} rows)

📂 Demand Sensing Lab ({VIBE_SCHEMA}):
  Volume:
    • {VIBE_VOLUME} ({vibe_files if 'vibe_files' in locals() else 0} raw data files)
  Tables:
    • Tables will be created during lab exercises
""")

if notebooks_imported > 0:
    print(f"""
📓 Notebooks:
  Location: /Workspace/Users/{current_user if 'current_user' in locals() else 'your_user'}/DevSecOps_Labs/
  Imported: {notebooks_imported} files""")
    if notebook_folders:
        print(f"  Folders:")
        for folder in notebook_folders:
            print(f"    • {folder}/")

print(f"""
📂 Raw Data Location:
  Demand Sensing: /Volumes/{CATALOG}/{VIBE_SCHEMA}/{VIBE_VOLUME}/raw/
  
  Available datasets:
    • competitor_pricing
    • products  
    • sales
    • stores

💡 Access your notebooks:
  Navigate to: Workspace > Users > Your Name > DevSecOps_Labs
  {"Git Folder: " + git_folder_path if git_folder_path else ""}
""")
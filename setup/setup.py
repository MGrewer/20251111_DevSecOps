#!/usr/bin/env python
"""
DevSecOps Demo - Setup with Databricks Git Folder Creation
Following Databricks API best practices
"""

import os, shutil, glob, json, requests, time

print("""
╔══════════════════════════════════════════════════════════════╗
║         DevSecOps Demo - Installing...                       ║
╚══════════════════════════════════════════════════════════════╝
""")

# Configuration
CATALOG = "DevSecOps_Labs"
SCHEMA = "Agent_Bricks_Lab"
VOLUME = "meijer_store_transcripts"
TABLE = "meijer_store_tickets"
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

# Main setup
print("\n[1/5] Creating Databricks Git folder...")
git_folder_path = create_git_folder(GITHUB_URL)

# Also clone to /tmp for immediate use (in case Git folder has sync issues)
print("\n[2/5] Cloning for immediate setup...")
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
print("\n[3/5] Creating Unity Catalog assets...")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    print(f"  ✓ Catalog: {CATALOG}")
except:
    print(f"  ℹ️ Using existing: {CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
print(f"  ✓ Schema: {SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
print(f"  ✓ Volume: {VOLUME}")

# Copy PDFs using simple Python file operations
print("\n[4/5] Importing PDFs...")
src_dir = f"{REPO_PATH}/data/pdfs"
dst_dir = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
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

# Create table using Volume staging
print("\n[5/5] Creating Delta table...")
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
        df.write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`")
        
        # Clean up staging
        shutil.rmtree(parquet_staging, ignore_errors=True)
        
        count = spark.table(f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`").count()
        print(f"  ✓ Table created: {count:,} rows")
    else:
        print(f"  ⚠️ No parquet files found")
        count = 0
except Exception as e:
    print(f"  ❌ Table failed: {str(e)[:100]}")
    count = 0

# Clean up temp if used
if REPO_PATH == temp_path and temp_path.startswith("/tmp/"):
    try:
        shutil.rmtree(temp_path)
        print("\n  ✓ Cleaned up temp files")
    except:
        pass

print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ✅ Setup Complete!                        ║
╚══════════════════════════════════════════════════════════════╝

📦 Created:
  • {CATALOG}.{SCHEMA}.{VOLUME} ({success} PDFs)
  • {CATALOG}.{SCHEMA}.{TABLE} ({count:,} rows)
  {"• " + git_folder_path if git_folder_path else ""}

📝 Try:
  SELECT * FROM {CATALOG}.{SCHEMA}.{TABLE} LIMIT 10;

📂 Resources:
  PDFs: /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/
  {"Git: " + git_folder_path if git_folder_path else ""}
  
💡 To update the Git folder later:
  Go to Repos > {git_folder_path if git_folder_path else 'your repo'} > Pull
""")
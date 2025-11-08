# DevSecOps Labs - Databricks Training Materials

## One-Line Installation

Copy and paste this into any Databricks notebook and click run:

```python
import subprocess, time; t=str(int(time.time())); subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", f"/tmp/demo_{t}"], check=True); exec(open(f"/tmp/demo_{t}/setup/setup.py").read())
```

The script will:
1. Download everything from GitHub
2. Create Unity Catalog assets for both labs
3. Import PDF transcripts
4. Create Delta tables with sample data
5. Import lab notebooks with folder structure
6. Clean up temp files

Total time: ~2-3 minutes

## What Gets Created

### Agent Bricks Lab
- `devsecops_labs.agent_bricks_lab` - Schema
- `meijer_store_transcripts` - Volume with PDF transcripts
- `meijer_store_tickets` - Delta table
- `meijer_ownbrand_products` - Delta table

### Demand Sensing Lab (Vibe Code Assistant)
- `devsecops_labs.demand_sensing` - Schema
- `data` - Volume with raw CSV data (competitor_pricing, products, sales, stores)
- Lab notebooks in your workspace

## After Installation

### Access Your Data
```sql
-- View your data
USE CATALOG devsecops_labs;
USE SCHEMA agent_bricks_lab;

SELECT * FROM meijer_store_tickets LIMIT 10;

-- Count records
SELECT COUNT(*) FROM meijer_store_tickets;
```

### Access PDFs and Files
```python
# List PDFs in Agent Bricks Lab
pdfs = dbutils.fs.ls("/Volumes/devsecops_labs/agent_bricks_lab/meijer_store_transcripts")
print(f"Found {len(pdfs)} PDFs")

# List raw data directories in Demand Sensing Lab
raw_data = dbutils.fs.ls("/Volumes/devsecops_labs/demand_sensing/data/raw")
for folder in raw_data:
    print(f"  • {folder.name}")
```

### Access Lab Notebooks
Navigate to: **Workspace > Users > Your Name > DevSecOps_Labs/**

You'll find:
- `vibe-code-assistant-lab/` - Complete lab notebooks with solutions
- `includes/` - Helper files and utilities

## Available Datasets

### Raw Data (in demand_sensing schema only)
- **competitor_pricing** - Competitor pricing data
- **products** - Product catalog
- **sales** - Historical sales transactions
- **stores** - Store locations and details

### Use in Your Labs
```python
# Example: Create table from raw CSV
spark.sql("""
  CREATE TABLE products 
  USING CSV 
  OPTIONS (header 'true', inferSchema 'true')
  LOCATION '/Volumes/devsecops_labs/demand_sensing/data/raw/products'
""")
```

## Requirements

- Databricks workspace with Unity Catalog enabled
- CREATE CATALOG permissions (or have `devsecops_labs` catalog pre-created)
- Network access to GitHub (public repo)
- Python 3.8+ (standard in Databricks)

## Uninstall

### Quick Uninstall (One-Liner)

Remove all lab assets with a single command:

```python
import subprocess, time; t=str(int(time.time())); subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", f"/tmp/uninstall_{t}"], check=True); exec(open(f"/tmp/uninstall_{t}/setup/uninstall.py").read())
```

This removes:
- Delta tables
- All volumes (PDFs and raw data)
- Both schemas
- Notebooks from your workspace
- Keeps the catalog and Git folder

### Full Uninstall (Remove Everything)

To also remove the catalog and Git folder:

```python
import subprocess, time; t=str(int(time.time())); subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", f"/tmp/uninstall_{t}"], check=True); FULL_WIPE=True; REMOVE_GIT_FOLDER=True; exec(open(f"/tmp/uninstall_{t}/setup/uninstall.py").read())
```

### Manual Cleanup (SQL)

If you prefer manual cleanup, use these commands:
```sql
-- Remove Agent Bricks Lab assets
DROP TABLE IF EXISTS devsecops_labs.agent_bricks_lab.meijer_store_tickets;
DROP TABLE IF EXISTS devsecops_labs.agent_bricks_lab.meijer_ownbrand_products;
DROP VOLUME IF EXISTS devsecops_labs.agent_bricks_lab.meijer_store_transcripts;
DROP SCHEMA IF EXISTS devsecops_labs.agent_bricks_lab CASCADE;

-- Remove Demand Sensing Lab assets
DROP VOLUME IF EXISTS devsecops_labs.demand_sensing.data;
DROP SCHEMA IF EXISTS devsecops_labs.demand_sensing CASCADE;

-- Remove catalog (only if empty and you created it)
DROP CATALOG IF EXISTS devsecops_labs;
```

To remove notebooks from your workspace:
- Navigate to `/Workspace/Users/{your_name}/DevSecOps_Labs`
- Right-click the folder and select "Delete"

## Repository Structure

```
20251111_DevSecOps/
├── data/
│   ├── pdfs/              # PDF call transcripts
│   ├── parquet/           # Delta table parquet files
│   │   ├── meijer_store_tickets/
│   │   └── meijer_ownbrand_products/
│   └── csv/               # CSV datasets
│       ├── competitor_pricing/
│       ├── products/
│       ├── sales/
│       └── stores/
├── notebooks/
│   └── vibe-code-assistant-lab/  # Lab notebooks + solutions
│       ├── includes/
│       └── *.ipynb        # Lab exercises
├── setup/
│   ├── setup.py           # One-click installer
│   └── uninstall.py       # One-click uninstaller
├── config.json            # Configuration
└── README.md              # This file
```

## Lab Structure

#### Agent Bricks Lab
Build a rag knowledge assistant, natural language query agent, unity catalog function, and a multi-agent supervisor in the context of retail store operations.

#### Vibe Coding Assistant (Demand Sensing Lab)
Focus on code assistance using retail sales and inventory data.

## Support

Issues? Open a GitHub issue at: https://github.com/MGrewer/20251111_DevSecOps/issues

---
*Inspired by dbdemos https://github.com/databricks-demos/dbdemos?tab=readme-ov-file#dbdemos*
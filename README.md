# DevSecOps Databricks Lab Materials

## 🚀 One-Line Installation

Just copy and paste this into any Databricks notebook:

```python
import subprocess; subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", "/tmp/demo"], check=True); exec(open("/tmp/demo/setup.py").read())
```

**That's it!** No Repos setup needed. The script will:
1. Download everything from GitHub
2. Create Unity Catalog assets
3. Import 400+ PDFs
4. Create Delta table with 24,000+ rows
5. Clean up temp files

Total time: ~2-3 minutes

## 📦 What Gets Created

- `DevSecOps_Labs` - Catalog
- `DevSecOps_Demo` - Schema  
- `demo_pdfs` - Volume with PDFs
- `meijer_store_tickets` - Delta table

## 📝 After Installation

```sql
-- View your data
SELECT * FROM DevSecOps_Labs.DevSecOps_Demo.meijer_store_tickets LIMIT 10;

-- Count records
SELECT COUNT(*) FROM DevSecOps_Labs.DevSecOps_Demo.meijer_store_tickets;
```

```python
# List PDFs
dbutils.fs.ls("/Volumes/DevSecOps_Labs/DevSecOps_Demo/demo_pdfs")

# Count PDFs
pdf_count = len(dbutils.fs.ls("/Volumes/DevSecOps_Labs/DevSecOps_Demo/demo_pdfs"))
print(f"Found {pdf_count} PDFs")
```

## 🔄 Alternative Installation Methods

If git isn't available, use wget/curl:
```python
import requests
exec(requests.get("https://raw.githubusercontent.com/MGrewer/20251111_DevSecOps/main/setup.py").text)
```

Or with more error handling:
```python
import subprocess
try:
    subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", "/tmp/demo"], check=True)
    exec(open("/tmp/demo/setup.py").read())
except:
    print("Failed to clone. Check network connectivity.")
```

## 🔧 Requirements

- Databricks workspace with Unity Catalog enabled
- CREATE SCHEMA permissions (or have `DevSecOps_Labs` catalog pre-created)
- Network access to GitHub (public repo)

## 🧹 Uninstall

Remove everything with these commands:
```sql
-- Remove table
DROP TABLE IF EXISTS DevSecOps_Labs.DevSecOps_Demo.meijer_store_tickets;

-- Remove volume (this deletes all PDFs)
DROP VOLUME IF EXISTS DevSecOps_Labs.DevSecOps_Demo.demo_pdfs;

-- Remove schema
DROP SCHEMA IF EXISTS DevSecOps_Labs.DevSecOps_Demo CASCADE;

-- Remove catalog (only if empty and you created it)
DROP CATALOG IF EXISTS DevSecOps_Labs;
```

## 📂 Repository Structure

```
20251111_DevSecOps/
├── data/
│   ├── pdfs/         # 400+ PDF documents
│   └── table/        # Parquet files
├── setup.py          # One-click installer
├── config.json       # Configuration
└── README.md         # This file
```

## 🤝 Support

Issues? Open a GitHub issue at: https://github.com/MGrewer/20251111_DevSecOps/issues

---
*One line. No repos. No hassle. Inspired by dbdemos simplicity.*
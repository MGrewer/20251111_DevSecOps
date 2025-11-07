# DevSecOps Databricks Lab Materials

## Quick Start

Execute this command in any Databricks notebook:

```python
import subprocess; subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", "/tmp/demo"], check=True); exec(open("/tmp/demo/setup.py").read())
```

Installation completes in approximately 3-4 minutes with no additional configuration required.

## Overview

This repository provides a comprehensive dataset and lab environment for DevSecOps training and development in Databricks. The installation creates a complete Unity Catalog structure with sample data, enabling immediate hands-on practice with realistic retail datasets.

## Architecture

### Catalog Structure

The installation creates a single Unity Catalog with two schemas:

```
devsecops_labs/
├── agent_bricks_lab/
│   ├── Volumes:
│   │   ├── meijer_store_transcripts/
│   │   └── raw_data/
│   └── Tables:
│       ├── meijer_store_tickets
│       ├── competitor_pricing
│       ├── products
│       ├── sales
│       └── stores
│
└── demand_sensing/
    ├── Volume:
    │   └── data/
    └── Tables:
        ├── competitor_pricing
        ├── products
        ├── sales
        └── stores
```

### Data Contents

#### Agent Bricks Lab (`agent_bricks_lab`)
- **meijer_store_tickets**: Support ticket data with AI-generated transcripts
- **PDF transcripts**: Customer service call documentation
- **Raw data tables**: Products, sales, stores, and competitor pricing

#### Demand Sensing Lab (`demand_sensing`)
- **Analytical tables**: Products, sales, stores, and competitor pricing
- **Raw data files**: Source CSV/Parquet files in volume

## Installation Details

The setup script performs the following operations:

1. Creates Unity Catalog: `devsecops_labs`
2. Creates schemas: `agent_bricks_lab` and `demand_sensing`
3. Creates volumes for data storage
4. Imports PDF transcripts
5. Loads raw data files
6. Creates Delta tables from source data
7. Validates installation

## Usage Examples

### Basic Queries

```sql
-- Set catalog and schema
USE CATALOG devsecops_labs;
USE SCHEMA agent_bricks_lab;

-- Query main table
SELECT * FROM meijer_store_tickets LIMIT 10;

-- Analyze product data
SELECT category, COUNT(*) as product_count
FROM products
GROUP BY category;
```

### Using SQL Variables

```sql
DECLARE VARIABLE catalog_name STRING DEFAULT 'devsecops_labs';
DECLARE VARIABLE schema_name STRING DEFAULT 'demand_sensing';

USE CATALOG IDENTIFIER(catalog_name);
USE SCHEMA IDENTIFIER(schema_name);

SELECT COUNT(*) FROM products;
```

### Cross-Schema Analysis

```sql
SELECT 
    a.product_id,
    a.product_name,
    b.total_sales
FROM devsecops_labs.agent_bricks_lab.products a
JOIN devsecops_labs.demand_sensing.sales b
    ON a.product_id = b.product_id;
```

### Accessing Volume Data

```python
# List PDF files
dbutils.fs.ls("/Volumes/devsecops_labs/agent_bricks_lab/meijer_store_transcripts")

# Access raw data
dbutils.fs.ls("/Volumes/devsecops_labs/demand_sensing/data")

# Count files
pdf_count = len(dbutils.fs.ls("/Volumes/devsecops_labs/agent_bricks_lab/meijer_store_transcripts"))
print(f"Total PDFs: {pdf_count}")
```

## Requirements

- Databricks workspace with Unity Catalog enabled
- CREATE CATALOG permissions (or pre-existing `devsecops_labs` catalog)
- Network access to GitHub
- Approximately 500MB available storage

## Alternative Installation Methods

### Using requests library

```python
import requests
exec(requests.get("https://raw.githubusercontent.com/MGrewer/20251111_DevSecOps/main/setup.py").text)
```

### With error handling

```python
import subprocess
try:
    subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", "/tmp/demo"], check=True)
    exec(open("/tmp/demo/setup.py").read())
except Exception as e:
    print(f"Installation failed: {e}")
```

## Uninstallation

### Remove Individual Schema

```sql
-- Remove agent_bricks_lab
DROP SCHEMA IF EXISTS devsecops_labs.agent_bricks_lab CASCADE;

-- Remove demand_sensing
DROP SCHEMA IF EXISTS devsecops_labs.demand_sensing CASCADE;
```

### Complete Removal

```sql
-- Remove entire catalog and all contents
DROP CATALOG IF EXISTS devsecops_labs CASCADE;
```

## Repository Structure

```
20251111_DevSecOps/
├── setup.py           # Installation script
├── README.md          # This file
├── data/
│   ├── pdfs/         # PDF transcripts
│   ├── table/        # Parquet files
│   └── raw/          # Raw data files
└── notebooks/        # Sample analysis notebooks
```

## Verification

After installation, verify the setup:

```python
# Check schemas
spark.sql("SHOW SCHEMAS IN devsecops_labs").show()

# Verify tables in agent_bricks_lab
spark.sql("SHOW TABLES IN devsecops_labs.agent_bricks_lab").show()

# Verify tables in demand_sensing
spark.sql("SHOW TABLES IN devsecops_labs.demand_sensing").show()

# Check row counts
spark.sql("SELECT COUNT(*) FROM devsecops_labs.agent_bricks_lab.meijer_store_tickets").show()
```

## Support

For issues or questions, please open an issue at: https://github.com/MGrewer/20251111_DevSecOps/issues

## License

This project is provided for educational and training purposes.

---

Developed for DevSecOps training - Single command installation inspired by Databricks demo best practices.
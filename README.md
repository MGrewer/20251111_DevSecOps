# DevSecOps Databricks Lab Materials

## Installation

Execute this command in any Databricks notebook:

```python
import subprocess; subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", "/tmp/demo"], check=True); exec(open("/tmp/demo/setup.py").read())
```

Installation takes approximately 3-4 minutes and requires no additional configuration.

## Overview

This repository provides a complete lab environment for DevSecOps training in Databricks, featuring realistic retail datasets and comprehensive Unity Catalog structures.

## Created Assets

### Catalog: devsecops_labs

#### Schema: agent_bricks_lab
- **Tables**: meijer_store_tickets
- **Volumes**: meijer_store_transcripts (PDFs), raw_data (source files)

#### Schema: demand_sensing  
- **Volume**: data (raw source files)
- **Tables**: Created during lab exercises

## Usage Examples

### Basic Queries

```sql
-- Use agent_bricks_lab schema
USE CATALOG devsecops_labs;
USE SCHEMA agent_bricks_lab;
SELECT * FROM meijer_store_tickets LIMIT 10;

-- Use demand_sensing schema  
USE SCHEMA demand_sensing;
-- Tables created during lab from raw data
```

### SQL Variables Pattern

```sql
DECLARE VARIABLE catalog_name STRING DEFAULT 'devsecops_labs';
DECLARE VARIABLE schema_name STRING DEFAULT 'demand_sensing';

USE CATALOG IDENTIFIER(catalog_name);
USE SCHEMA IDENTIFIER(schema_name);
```

### File Access

```python
# List PDF transcripts
dbutils.fs.ls("/Volumes/devsecops_labs/agent_bricks_lab/meijer_store_transcripts")

# Access raw data
dbutils.fs.ls("/Volumes/devsecops_labs/demand_sensing/data")
```

### Creating Tables from Raw Data

During the lab, create tables as needed:

```sql
-- Example: Create products table from CSV
CREATE TABLE devsecops_labs.demand_sensing.products
USING CSV
OPTIONS (header = "true", inferSchema = "true")
LOCATION '/Volumes/devsecops_labs/demand_sensing/data/products';

-- Example: Create sales table
CREATE TABLE devsecops_labs.demand_sensing.sales
USING PARQUET
LOCATION '/Volumes/devsecops_labs/demand_sensing/data/sales';
```

## Repository Structure

```
20251111_DevSecOps/
├── setup.py          # Installation script
├── data/
│   ├── pdfs/        # PDF transcripts
│   ├── table/       # Parquet files for main table
│   └── raw/         # Raw data files
│       ├── competitor_pricing/
│       ├── products/
│       ├── sales/
│       └── stores/
└── notebooks/       # Sample notebooks
```

## Requirements

- Databricks workspace with Unity Catalog
- CREATE CATALOG permissions
- Network access to GitHub
- 500MB available storage

## Notebooks

After installation, notebooks are available in:
- `/Workspace/Users/{your_username}/DevSecOps_Labs/`

These notebooks provide:
- Quick start guide
- Data analysis examples
- Demand sensing patterns
- SQL variable usage examples

## Raw Data Available

The following datasets are available in volumes for table creation:
- **competitor_pricing**: Competitive price tracking
- **products**: Product catalog  
- **sales**: Transaction data
- **stores**: Store locations and details

## Uninstallation

Remove specific schema:
```sql
DROP SCHEMA IF EXISTS devsecops_labs.agent_bricks_lab CASCADE;
DROP SCHEMA IF EXISTS devsecops_labs.demand_sensing CASCADE;
```

Remove entire catalog:
```sql
DROP CATALOG IF EXISTS devsecops_labs CASCADE;
```

## Support

For issues or questions: https://github.com/MGrewer/20251111_DevSecOps/issues
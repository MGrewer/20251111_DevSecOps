Here’s the updated **README** rewritten to match your new one-click architecture, correct UC layout, and built-in cleanup option.
It replaces the old “clone + exec” multi-line install with the new resilient one-liner and adds a cleanup section that mirrors it.

---

# DevSecOps Databricks Lab Materials

## Installation (One-Click)

Execute this command in any Databricks notebook cell:

```python
import requests; exec(requests.get("https://raw.githubusercontent.com/MGrewer/20251111_DevSecOps/main/setup/setup.py", timeout=60).text, {"spark": spark, "dbutils": dbutils})
```

This automatically downloads and executes the latest **setup script**, which:

1. Creates the catalog **`devsecops_labs`**
2. Builds the schemas **`agent_bricks_lab`** and **`demand_sensing`**
3. Creates three volumes:

   * `agent_bricks_lab.meijer_store_transcripts` – PDF transcripts
   * `agent_bricks_lab.raw_data` – raw source files
   * `demand_sensing.data` – raw folders used in lab exercises
4. Imports all PDF and raw data files
5. Creates the table **`devsecops_labs.agent_bricks_lab.meijer_store_tickets`**

Typical runtime: **3–4 minutes**, no manual setup required.

---

## Cleanup (One-Click Uninstall)

To remove all lab assets and reset the workspace:

```python
import requests; exec(requests.get("https://raw.githubusercontent.com/MGrewer/20251111_DevSecOps/main/setup/setup.py", timeout=60).text, {"spark": spark, "dbutils": dbutils, "MODE": "cleanup"})
```

This safely drops:

* The `meijer_store_tickets` table
* All three volumes
* Both schemas (`agent_bricks_lab`, `demand_sensing`)
* Optionally removes the `/Users/.../DevSecOps Demo` notebook folder

---

## Created Assets

### Catalog: `devsecops_labs`

#### Schema: `agent_bricks_lab`

* **Tables**

  * `meijer_store_tickets`
* **Volumes**

  * `meijer_store_transcripts` – PDF transcripts
  * `raw_data` – source files and tickets table data

#### Schema: `demand_sensing`

* **Volume**

  * `data/raw/...` – four raw folders (`products`, `sales`, `stores`, `competitor_pricing`)
* **Tables**

  * Created during lab exercises

---

## Example Usage

### Explore the environment

```sql
USE CATALOG devsecops_labs;
USE SCHEMA agent_bricks_lab;
SELECT * FROM meijer_store_tickets LIMIT 10;
```

### Browse files

```python
# PDF transcripts
dbutils.fs.ls("/Volumes/devsecops_labs/agent_bricks_lab/meijer_store_transcripts")

# Raw data (demand sensing)
dbutils.fs.ls("/Volumes/devsecops_labs/demand_sensing/data/raw")
```

### Create lab tables manually

```sql
CREATE TABLE devsecops_labs.demand_sensing.products
USING CSV
OPTIONS (header = "true", inferSchema = "true")
LOCATION '/Volumes/devsecops_labs/demand_sensing/data/raw/products';

CREATE TABLE devsecops_labs.demand_sensing.sales
USING PARQUET
LOCATION '/Volumes/devsecops_labs/demand_sensing/data/raw/sales';
```

---

## Repository Structure

```
20251111_DevSecOps/
├── setup/
│   ├── setup.py      # One-click setup & cleanup dispatcher
│   └── cleanup.py    # Cleanup logic
├── data/
│   ├── pdfs/         # PDF transcripts
│   ├── table/        # Parquet source for tickets table
│   └── raw/
│       ├── competitor_pricing/
│       ├── products/
│       ├── sales/
│       └── stores/
└── notebooks/        # Optional example notebooks
```

---

## Requirements

* Databricks workspace with Unity Catalog enabled
* CREATE CATALOG privileges
* Network access to GitHub
* ~500 MB available storage

---

## Notes

* Re-running the setup script automatically updates or re-creates all assets.
* Cleanup can be invoked at any time with the same script.
* If your cluster has no outbound internet, clone this repo into a Workspace Repo and run:

```python
exec(open("/Workspace/Repos/<user>/DevSecOps/setup/setup.py").read(), {"spark": spark, "dbutils": dbutils})
```

---

## Support

Report issues or request improvements at
**[https://github.com/MGrewer/20251111_DevSecOps/issues](https://github.com/MGrewer/20251111_DevSecOps/issues)**

---

Would you like me to include the visual summary box from the setup script (“Catalog / Schemas / Volumes / Table” layout) as a formatted code block at the end of the README to match the runtime summary?

# DevSecOps Labs - Databricks Training Materials

## Prerequisites: Free Trial Setup

#### 1. Sign Up for Databricks Free Trial

Visit the signup page with your work email:

[Sign up for a Databricks free trial](http://signup.databricks.com/?provider=DB&region=us-west-2&utm_source=direct&utm_medium=hackathon)

- Use your work email address
- Patiently wait for the verification code email
- Enter the 6-digit code when prompted

Note: Agent Bricks is only available in free trials, not the educational Free Edition. No commercial/confidential data should be uploaded to either Free Trials or Free Edition.

#### 2. Create New Account (if needed)

Even if it shows you already have an account associated with your email, please create a fresh trial by selecting **"Create new account"**.

#### 3. Choose Express Setup

Select **"Express Setup"** to create a pre-configured serverless workspace. This automatically provisions your account and cloud infrastructure.

#### 4. Name Your Account

You can name it anything, but if you plan to revisit this lab, use a recognizable name: **`YourName_2025 DevSecOps`**

**Optional:** Check "Allow anyone from @yourdomain.com to join this account" - This enables an SME from your company to join your workspace for troubleshooting support if needed.

#### 5. Enable Required Preview Features

In the top right corner, navigate to: **User Menu > Previews**

Enable these two preview features:
1. **Agent Framework: On-Behalf-Of-User Authorization** - Required for AI agents to access Databricks resources using your identity
2. **Databricks Apps - On-Behalf-Of-User Authorization** - Required for the chatbot application to act on your behalf

Your workspace is now ready for the one-line installation below.

### One-Line Installation

Copy and paste this into any Databricks notebook and click run:

```python
import subprocess, time; t=str(int(time.time())); subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", f"/tmp/labs_{t}"], check=True); exec(open(f"/tmp/labs_{t}/setup/setup.py").read())
```

The script will:
1. Download everything from GitHub
2. Create Unity Catalog assets for both labs
3. Import PDF transcripts
4. Create Delta tables with sample data
5. Import lab notebooks with folder structure
6. Clean up temp files

Total time: ~2-3 minutes

## After Installation

### Access Lab Notebooks
Navigate to: **Workspace > Users > Your Name > DevSecOps_Labs/**

You'll find:

**Agent Bricks Lab**

Build a rag knowledge assistant, natural language query agent, unity catalog function, and a multi-agent supervisor in the context of retail store operations.
- `-02_agent-bricks-lab/` - Lab notebook and instructions
  - 01_instructions_agent-bricks-lab
  - `includes/` - Helper files and utilities

- `chatbot-app` - app template used as an optional excercise in the Agent Bricks lab

**Vibe Coding Assistant (Demand Sensing Lab)**

Focus on code assistance using retail sales and inventory data.
- `01_vibe-code-assistant-lab/` - Complete lab notebooks with solutions
  - Lab notebook 1 - Chat Assistant
  - Lab notebook 2 - Optimize with Edit Assistant
  - Lab notebook 2 [Solution] - Optimize with Edit Assistant
  - Lab notebook 3 Demand Sensing ML with Agent
  - Lab notebook 3 [Solution] - Demand Sensing ML with Agent
  - `includes/` - Helper files and utilities



## What Gets Created

### Agent Bricks Lab
- `devsecops_labs.agent_bricks_lab` - Schema
- `meijer_store_transcripts` - Volume with PDF transcripts
- `meijer_store_tickets` - Delta table
- `meijer_ownbrand_products` - Delta table
- Lab notebook in your workspoace

### Demand Sensing Lab (Vibe Code Assistant)
- `devsecops_labs.demand_sensing` - Schema
- `data` - Volume with raw CSV data (competitor_pricing, products, sales, stores)
- Lab notebooks in your workspace

## Requirements

- Databricks workspace with Unity Catalog enabled
- CREATE CATALOG permissions (or have `devsecops_labs` catalog pre-created)
- Network access to GitHub (public repo)
- Python 3.8+ (standard in Databricks)

## Full Uninstall (Remove Everything)

This removes:
- Delta tables
- All volumes (PDFs and raw data)
- Both schemas
- Notebooks from your workspace
- Catalog
- Git folder

```python
import subprocess, time; t=str(int(time.time())); subprocess.run(["git", "clone", "https://github.com/MGrewer/20251111_DevSecOps", f"/tmp/uninstall_{t}"], check=True); FULL_WIPE=True; REMOVE_GIT_FOLDER=True; exec(open(f"/tmp/uninstall_{t}/setup/uninstall.py").read())
```

## Support

Issues? Open a GitHub issue at: https://github.com/MGrewer/20251111_DevSecOps/issues

---
*Inspired by dbdemos https://github.com/databricks-demos/dbdemos?tab=readme-ov-file#dbdemos*
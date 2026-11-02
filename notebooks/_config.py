# Databricks notebook source
# MAGIC %md
# MAGIC # Project config
# MAGIC
# MAGIC Shared settings for every notebook in this project. Each notebook loads it with `%run ./_config`.
# MAGIC
# MAGIC In **Databricks Free Edition** the default catalog is `workspace`. This notebook creates a schema
# MAGIC (`je_testing`) and a Unity Catalog volume (`raw`) that acts as the landing zone for the ERP extract.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "je_testing"
VOLUME = "raw"

# Landing zone for raw files, like an SFTP drop from a client's ERP system
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Fiscal year under audit (Indian FY: April to March)
FY_START = "2025-04-01"
FY_END = "2026-03-31"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using {CATALOG}.{SCHEMA}  |  raw files in {RAW_PATH}")

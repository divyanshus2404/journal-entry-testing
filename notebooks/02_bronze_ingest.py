# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: ingest the raw ERP extract
# MAGIC
# MAGIC The **bronze** layer keeps data exactly as it arrived: every column as a string, nothing cleaned or
# MAGIC removed. We only add lineage columns (`_source_file`, `_ingested_at`) so any number in a later report
# MAGIC can be traced back to the file it came from. That traceability matters in audit work.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def string_schema(cols):
    return StructType([StructField(c, StringType(), True) for c in cols])

JE_COLUMNS = ["company_code", "fiscal_year", "period", "je_id", "line_no", "doc_type", "source",
              "posting_date", "entry_date", "entry_time", "account_number", "debit_amount",
              "credit_amount", "currency", "description", "reference", "created_by", "approved_by"]

def ingest(name, columns, table):
    df = (spark.read
               .option("header", True)
               .schema(string_schema(columns))
               .csv(f"{RAW_PATH}/{name}")
               .withColumn("_source_file", F.col("_metadata.file_path"))
               .withColumn("_ingested_at", F.current_timestamp()))
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table))
    print(f"{table:<28} {spark.table(table).count():>7,} rows")

# COMMAND ----------

ingest("journal_entries", JE_COLUMNS, "bronze_journal_entries")
ingest("chart_of_accounts", ["account_number", "account_name", "account_type", "process"], "bronze_chart_of_accounts")
ingest("users", ["user_id", "department", "role", "termination_date"], "bronze_users")

# COMMAND ----------

display(spark.table("bronze_journal_entries").limit(10))

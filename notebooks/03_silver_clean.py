# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: clean, validate and model the ledger
# MAGIC
# MAGIC The **silver** layer turns the raw extract into data auditors can trust:
# MAGIC
# MAGIC 1. **Type casting** – SAP-style `dd.MM.yyyy` dates become real dates, amounts become decimals.
# MAGIC 2. **De-duplication** – rows the extract job sent twice are removed.
# MAGIC 3. **Validation** – lines posted to unknown accounts, lines with both or neither a debit and a credit,
# MAGIC    and entries whose debits don't equal credits are **quarantined** with a reason, not silently dropped.
# MAGIC 4. **Modelling** – a line-level table enriched with the chart of accounts, and a header-level table with
# MAGIC    one row per journal entry.
# MAGIC
# MAGIC A short data quality report at the end shows what happened to every row.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

raw = spark.table("bronze_journal_entries")
coa = (spark.table("bronze_chart_of_accounts")
            .select(F.trim("account_number").alias("account_number"),
                    F.trim("account_name").alias("account_name"),
                    F.trim("account_type").alias("account_type"),
                    F.trim("process").alias("process")))
users = (spark.table("bronze_users")
              .select(F.trim("user_id").alias("user_id"), "department", "role",
                      F.to_date("termination_date").alias("termination_date")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cast types and standardise values

# COMMAND ----------

typed = (raw.select(
    F.trim("company_code").alias("company_code"),
    F.col("fiscal_year").cast("int").alias("fiscal_year"),
    F.col("period").cast("int").alias("period"),
    F.trim("je_id").alias("je_id"),
    F.col("line_no").cast("int").alias("line_no"),
    F.upper(F.trim("doc_type")).alias("doc_type"),
    F.upper(F.trim("source")).alias("source"),
    F.to_date("posting_date", "dd.MM.yyyy").alias("posting_date"),
    F.to_timestamp(F.concat_ws(" ", "entry_date", "entry_time"), "yyyy-MM-dd HH:mm:ss").alias("entry_ts"),
    F.trim("account_number").alias("account_number"),
    F.coalesce(F.col("debit_amount").cast("decimal(18,2)"), F.lit(0).cast("decimal(18,2)")).alias("debit"),
    F.coalesce(F.col("credit_amount").cast("decimal(18,2)"), F.lit(0).cast("decimal(18,2)")).alias("credit"),
    F.upper(F.trim("currency")).alias("currency"),
    F.trim("description").alias("description"),
    F.trim("reference").alias("reference"),
    F.upper(F.trim("created_by")).alias("created_by"),
    F.upper(F.trim("approved_by")).alias("approved_by"),
    "_source_file",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Remove rows the extract sent twice

# COMMAND ----------

business_cols = [c for c in typed.columns if c != "_source_file"]
deduped = typed.dropDuplicates(business_cols)

rows_raw = typed.count()
rows_deduped = deduped.count()
print(f"Raw lines: {rows_raw:,}  |  exact duplicates removed: {rows_raw - rows_deduped:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate lines and entries

# COMMAND ----------

valid_accounts = coa.select("account_number").withColumn("_known_account", F.lit(True))

line_checks = (deduped
    .join(valid_accounts, "account_number", "left")
    .withColumn("line_issue",
        F.when(F.col("_known_account").isNull(), F.lit("Account not in chart of accounts"))
         .when((F.col("debit") > 0) & (F.col("credit") > 0), F.lit("Line has both debit and credit"))
         .when((F.col("debit") == 0) & (F.col("credit") == 0), F.lit("Line has no amount"))
         .when((F.col("debit") < 0) | (F.col("credit") < 0), F.lit("Negative amount"))
         .when(F.col("posting_date").isNull() | F.col("entry_ts").isNull(), F.lit("Unparseable date"))
    )
    .drop("_known_account"))

# Entry-level checks: does each journal entry balance, and does any line have an issue?
je_checks = (line_checks.groupBy("je_id")
    .agg(F.sum("debit").alias("total_debit"),
         F.sum("credit").alias("total_credit"),
         F.max("line_issue").alias("first_line_issue"))
    .withColumn("je_issue",
        F.when(F.col("first_line_issue").isNotNull(), F.col("first_line_issue"))
         .when(F.col("total_debit") != F.col("total_credit"),
               F.concat(F.lit("Entry does not balance (difference "),
                        (F.col("total_debit") - F.col("total_credit")).cast("string"), F.lit(")")))
    )
    .select("je_id", "je_issue"))

checked = line_checks.join(je_checks, "je_id")

quarantine = checked.filter(F.col("je_issue").isNotNull())
clean = checked.filter(F.col("je_issue").isNull()).drop("line_issue", "je_issue")

# COMMAND ----------

(quarantine.write.mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable("silver_quarantine"))

display(quarantine.select("je_id", "line_no", "account_number", "debit", "credit", "line_issue", "je_issue")
                  .orderBy("je_id", "line_no"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model the clean data

# COMMAND ----------

silver_lines = (clean
    .join(coa, "account_number", "left")
    .withColumn("amount", F.greatest("debit", "credit"))
    .withColumn("side", F.when(F.col("debit") > 0, "D").otherwise("C")))

(silver_lines.write.mode("overwrite").option("overwriteSchema", "true")
             .saveAsTable("silver_journal_lines"))

# One row per journal entry
silver_entries = (silver_lines.groupBy("je_id")
    .agg(F.first("company_code").alias("company_code"),
         F.first("fiscal_year").alias("fiscal_year"),
         F.first("period").alias("period"),
         F.first("posting_date").alias("posting_date"),
         F.first("entry_ts").alias("entry_ts"),
         F.first("doc_type").alias("doc_type"),
         F.first("source").alias("source"),
         F.first("description").alias("description"),
         F.first("reference").alias("reference"),
         F.first("created_by").alias("created_by"),
         F.first("approved_by").alias("approved_by"),
         F.sum("debit").alias("total_amount"),
         F.count("*").alias("line_count"),
         F.array_sort(F.collect_set(F.concat("side", F.lit(":"), "account_number"))).alias("account_signature"),
         F.array_sort(F.collect_set("process")).alias("processes"),
         F.max(F.when((F.col("account_type") == "Revenue") & (F.col("side") == "C"), True)
                .otherwise(False)).alias("credits_revenue")))

(silver_entries.write.mode("overwrite").option("overwriteSchema", "true")
               .saveAsTable("silver_journal_entries"))

(users.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_users"))
(coa.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_chart_of_accounts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality report

# COMMAND ----------

dq_report = spark.createDataFrame([
    ("Lines in raw extract", rows_raw),
    ("Exact duplicate lines removed", rows_raw - rows_deduped),
    ("Lines quarantined", quarantine.count()),
    ("Journal entries quarantined", quarantine.select("je_id").distinct().count()),
    ("Clean lines (silver_journal_lines)", spark.table("silver_journal_lines").count()),
    ("Clean journal entries (silver_journal_entries)", spark.table("silver_journal_entries").count()),
], ["check", "count"])

dq_report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_dq_report")
display(dq_report)

# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Validate the pipeline against the answer key
# MAGIC
# MAGIC Because the data is synthetic, we know exactly which entries were planted with each red flag or data
# MAGIC quality problem. This notebook checks that every planted issue was caught (**recall**) and reports how
# MAGIC many normal entries were flagged by mistake (**false positives**).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

answer_key = (spark.read.option("header", True).csv(f"{RAW_PATH}/_answer_key").distinct())
results = spark.table("gold_je_test_results")
quarantined = spark.table("silver_quarantine").select("je_id").distinct()

SCENARIO_TO_TEST = {
    "weekend_posting": "t_weekend",
    "after_hours_entry": "t_after_hours",
    "round_amount": "t_round_amount",
    "period_end_manual_revenue": "t_period_end_revenue",
    "self_approved": "t_self_approved",
    "suspense_or_rare_account": "t_suspense_or_rare_account",
    "duplicate_entry": "t_duplicate_entry",
    "suspicious_description": "t_suspicious_description",
    "terminated_user": "t_terminated_user",
}

# COMMAND ----------

rows = []
for scenario, test_col in SCENARIO_TO_TEST.items():
    planted = answer_key.filter(F.col("scenario") == scenario).select("je_id")
    caught = planted.join(results.filter(F.col(test_col)), "je_id").count()
    total_flagged = results.filter(F.col(test_col)).count()
    n = planted.count()
    rows.append((scenario, n, caught, round(caught / n * 100, 1) if n else None, total_flagged - caught))

for scenario in ["dq_unbalanced", "dq_invalid_account"]:
    planted = answer_key.filter(F.col("scenario") == scenario).select("je_id")
    caught = planted.join(quarantined, "je_id").count()
    n = planted.count()
    rows.append((scenario, n, caught, round(caught / n * 100, 1) if n else None, None))

validation = spark.createDataFrame(rows, "scenario string, planted int, caught int, recall_pct double, flagged_but_not_planted int")
validation.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_validation")
display(validation)

# COMMAND ----------

# MAGIC %md
# MAGIC `flagged_but_not_planted` is not automatically a mistake: a normal entry can genuinely match a test
# MAGIC (for example an ordinary account that is rarely used). In a real audit, an auditor would review these
# MAGIC and document why each one is or isn't a concern.

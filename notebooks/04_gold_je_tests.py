# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: journal entry testing
# MAGIC
# MAGIC Auditors test journal entries because management can override controls by posting manual entries
# MAGIC (SA 240 / ISA 240, *the auditor's responsibilities relating to fraud*). This notebook runs nine
# MAGIC standard tests on every clean journal entry and gives each entry a **risk score**.
# MAGIC
# MAGIC | # | Test | Why it matters | Weight |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Weekend posting | Entries outside normal working days avoid review | 1 |
# MAGIC | 2 | After-hours entry | Created before 7am or after 9pm | 1 |
# MAGIC | 3 | Round amount | Estimates and made-up numbers tend to be round | 1 |
# MAGIC | 4 | Period-end manual revenue | Revenue pushed into a quarter at the last minute | 3 |
# MAGIC | 5 | Self-approved | Creator and approver are the same person (segregation of duties) | 3 |
# MAGIC | 6 | Suspense or rarely used account | Hides items that should be classified properly | 2 |
# MAGIC | 7 | Duplicate entry | Same accounts and amount within 7 days (double payment risk) | 2 |
# MAGIC | 8 | Suspicious description | Words like *plug*, *override*, *per CFO* | 2 |
# MAGIC | 9 | Terminated user | Posted by someone who had already left | 3 |
# MAGIC
# MAGIC Outputs: `gold_je_test_results` (every entry with its flags), `gold_je_exceptions` (flagged entries,
# MAGIC highest risk first), `gold_test_summary`, `gold_process_summary` and `gold_benford`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

entries = spark.table("silver_journal_entries")
lines = spark.table("silver_journal_lines")
users = spark.table("silver_users")

ROUND_MIN = 50_000          # only look at round amounts above this
ROUND_STEP = 10_000         # "round" = exact multiple of this
RARE_ACCOUNT_MAX_USES = 10  # an account used in 10 or fewer entries is "rarely used"
DUPLICATE_WINDOW_DAYS = 7
KEYWORDS = r"(?i)\b(plug|override|per cfo|adjust(ment)?\s+per|write[- ]?off|misc(ellaneous)? adj)\b"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests that look at a single entry

# COMMAND ----------

quarter_end_months = [6, 9, 12, 3]

tests = (entries
    .join(users.select(F.col("user_id").alias("created_by"), "termination_date"), "created_by", "left")
    # Timing tests skip scheduled system jobs (payroll, depreciation), which run at fixed early hours
    .withColumn("is_person", F.col("created_by") != "SYS_BATCH")
    .withColumn("t_weekend", F.col("is_person") & F.dayofweek("posting_date").isin(1, 7))   # 1 = Sun, 7 = Sat
    .withColumn("t_after_hours", F.col("is_person") & ((F.hour("entry_ts") < 7) | (F.hour("entry_ts") >= 21)))
    .withColumn("t_round_amount", (F.col("total_amount") >= ROUND_MIN) &
                                  (F.col("total_amount") % ROUND_STEP == 0))
    .withColumn("t_period_end_revenue",
                (F.col("source") == "MANUAL") & F.col("credits_revenue") &
                F.month("posting_date").isin(quarter_end_months) &
                (F.datediff(F.last_day("posting_date"), F.col("posting_date")) <= 3))
    .withColumn("t_self_approved", (F.col("source") == "MANUAL") & (F.col("created_by") == F.col("approved_by")))
    .withColumn("t_suspicious_description", F.col("description").rlike(KEYWORDS))
    .withColumn("t_terminated_user", F.col("termination_date").isNotNull() &
                                     (F.col("posting_date") > F.col("termination_date"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests that need other entries for context

# COMMAND ----------

# 6. Suspense or rarely used accounts
account_usage = lines.groupBy("account_number").agg(F.countDistinct("je_id").alias("uses"))
odd_accounts = (account_usage.join(spark.table("silver_chart_of_accounts"), "account_number")
    .filter((F.col("uses") <= RARE_ACCOUNT_MAX_USES) | F.lower("account_name").contains("suspense"))
    .select("account_number"))
je_odd_account = (lines.join(odd_accounts, "account_number")
    .select("je_id").distinct().withColumn("t_suspense_or_rare_account", F.lit(True)))

# 7. Duplicates: same accounts, same amount, posted within 7 days of each other
a = entries.select("je_id", "posting_date", "total_amount", "account_signature")
b = a.select([F.col(c).alias(f"b_{c}") for c in a.columns])
dup_pairs = (a.join(b,
        (F.col("account_signature") == F.col("b_account_signature")) &
        (F.col("total_amount") == F.col("b_total_amount")) &
        (F.col("je_id") < F.col("b_je_id")) &
        (F.abs(F.datediff("posting_date", "b_posting_date")) <= DUPLICATE_WINDOW_DAYS)))
je_duplicates = (dup_pairs.select(F.col("je_id")).union(dup_pairs.select(F.col("b_je_id").alias("je_id")))
    .distinct().withColumn("t_duplicate_entry", F.lit(True)))

tests = (tests
    .join(je_odd_account, "je_id", "left")
    .join(je_duplicates, "je_id", "left")
    .fillna(False, subset=["t_suspense_or_rare_account", "t_duplicate_entry"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk score

# COMMAND ----------

WEIGHTS = {
    "t_weekend": 1,
    "t_after_hours": 1,
    "t_round_amount": 1,
    "t_period_end_revenue": 3,
    "t_self_approved": 3,
    "t_suspense_or_rare_account": 2,
    "t_duplicate_entry": 2,
    "t_suspicious_description": 2,
    "t_terminated_user": 3,
}
LABELS = {
    "t_weekend": "Weekend posting",
    "t_after_hours": "After-hours entry",
    "t_round_amount": "Round amount",
    "t_period_end_revenue": "Period-end manual revenue",
    "t_self_approved": "Self-approved",
    "t_suspense_or_rare_account": "Suspense or rare account",
    "t_duplicate_entry": "Possible duplicate",
    "t_suspicious_description": "Suspicious description",
    "t_terminated_user": "Terminated user",
}

risk_score = sum(F.when(F.col(c), F.lit(w)).otherwise(F.lit(0)) for c, w in WEIGHTS.items())
reasons = F.filter(F.array(*[F.when(F.col(c), F.lit(LABELS[c])) for c in WEIGHTS]), lambda x: x.isNotNull())

results = (tests
    .withColumn("risk_score", risk_score)
    .withColumn("flags", reasons)
    .withColumn("flag_count", F.size("flags"))
    .withColumn("risk_level", F.when(F.col("risk_score") >= 4, "High")
                               .when(F.col("risk_score") >= 2, "Medium")
                               .when(F.col("risk_score") >= 1, "Low")
                               .otherwise("None"))
    .drop("termination_date", "is_person"))

(results.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_je_test_results"))

exceptions = (spark.table("gold_je_test_results").filter("risk_score > 0")
    .select("je_id", "posting_date", "entry_ts", "source", "created_by", "approved_by", "description",
            "total_amount", "processes", "risk_score", "risk_level", "flags")
    .orderBy(F.desc("risk_score"), F.desc("total_amount")))
(exceptions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_je_exceptions"))

display(spark.table("gold_je_exceptions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summaries

# COMMAND ----------

r = spark.table("gold_je_test_results")
total_entries = r.count()

summary = spark.createDataFrame(
    [(LABELS[c], WEIGHTS[c], *r.filter(F.col(c)).agg(F.count("*"), F.coalesce(F.sum("total_amount"), F.lit(0))).first())
     for c in WEIGHTS],
    "test string, weight int, flagged_entries long, flagged_amount decimal(38,2)")
summary = (summary
    .withColumn("pct_of_entries", F.round(F.col("flagged_entries") / total_entries * 100, 2))
    .orderBy(F.desc("flagged_entries")))
summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_test_summary")
display(summary)

# COMMAND ----------

process_summary = (r.withColumn("process", F.explode("processes"))
    .groupBy("process")
    .agg(F.count("*").alias("entries"),
         F.sum(F.when(F.col("risk_score") > 0, 1).otherwise(0)).alias("flagged_entries"),
         F.sum(F.when(F.col("risk_level") == "High", 1).otherwise(0)).alias("high_risk_entries"),
         F.sum("total_amount").alias("total_amount"))
    .orderBy("process"))
process_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_process_summary")
display(process_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benford's law (first-digit test)
# MAGIC
# MAGIC In natural financial data the first digit is **1** about 30% of the time and **9** under 5% of the time.
# MAGIC Made-up numbers usually break this pattern. The test runs on every line amount of ₹10 or more.

# COMMAND ----------

first_digit = (lines.filter(F.col("amount") >= 10)
    .withColumn("digit", F.substring(F.regexp_replace(F.col("amount").cast("string"), r"^0+|\.", ""), 1, 1).cast("int")))
n_lines = first_digit.count()

benford = (first_digit.groupBy("digit").count()
    .withColumn("observed_pct", F.round(F.col("count") / n_lines * 100, 2))
    .withColumn("expected_pct", F.round(F.log10(1 + 1 / F.col("digit")) * 100, 2))
    .withColumn("difference_pct", F.round(F.col("observed_pct") - F.col("expected_pct"), 2))
    .orderBy("digit"))
benford.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_benford")
display(benford)

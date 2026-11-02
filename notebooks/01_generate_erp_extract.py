# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate a synthetic ERP extract
# MAGIC
# MAGIC Real audit data is confidential, so this notebook creates a **realistic general ledger extract** for a
# MAGIC fictional retail company ("Acme Retail Pvt Ltd") for **FY 2025-26 (Apr 2025 – Mar 2026)**.
# MAGIC
# MAGIC It writes three CSV files to the raw volume, in the shape a client's ERP (SAP-style) would export:
# MAGIC
# MAGIC | File | What it holds |
# MAGIC |---|---|
# MAGIC | `journal_entries` | One row per journal entry **line** (debit or credit) |
# MAGIC | `chart_of_accounts` | Account number, name, type, and business process (O2C / P2P / R2R) |
# MAGIC | `users` | ERP users, their department, and termination date if they left |
# MAGIC
# MAGIC Normal activity covers **Order-to-Cash** (sales invoices, customer receipts), **Procure-to-Pay**
# MAGIC (vendor invoices, vendor payments) and **Record-to-Report** (payroll, depreciation, accruals).
# MAGIC
# MAGIC On top of that, it plants known **red flags** (weekend postings, round amounts, duplicate invoices,
# MAGIC segregation-of-duties breaks…) and **data quality problems** (unbalanced entries, invalid accounts,
# MAGIC re-sent rows). The list of planted issues is saved as an answer key so notebook 04 can check that the
# MAGIC pipeline actually catches them.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import math
import random
from datetime import date, datetime, timedelta

import pandas as pd

random.seed(42)

fy_start = date.fromisoformat(FY_START)
fy_end = date.fromisoformat(FY_END)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data: chart of accounts and users

# COMMAND ----------

chart_of_accounts = [
    # account_number, account_name, account_type, process
    ("1000", "Cash at Bank", "Asset", "Treasury"),
    ("1100", "Accounts Receivable", "Asset", "O2C"),
    ("1200", "Inventory", "Asset", "P2P"),
    ("1300", "GST Input Credit", "Asset", "P2P"),
    ("1500", "Fixed Assets", "Asset", "R2R"),
    ("1550", "Accumulated Depreciation", "Asset", "R2R"),
    ("1900", "Suspense Account", "Asset", "R2R"),
    ("2000", "Accounts Payable", "Liability", "P2P"),
    ("2100", "GST Output Payable", "Liability", "O2C"),
    ("2200", "Accrued Expenses", "Liability", "R2R"),
    ("2300", "Salaries Payable", "Liability", "R2R"),
    ("3000", "Share Capital", "Equity", "R2R"),
    ("4000", "Sales Revenue", "Revenue", "O2C"),
    ("4100", "Service Revenue", "Revenue", "O2C"),
    ("5000", "Cost of Goods Sold", "Expense", "P2P"),
    ("6000", "Salaries Expense", "Expense", "R2R"),
    ("6100", "Rent Expense", "Expense", "P2P"),
    ("6200", "Utilities Expense", "Expense", "P2P"),
    ("6300", "Marketing Expense", "Expense", "P2P"),
    ("6400", "Depreciation Expense", "Expense", "R2R"),
    ("6900", "Miscellaneous Expense", "Expense", "R2R"),
]

users = [
    # user_id, department, role, termination_date
    ("SYS_BATCH", "IT", "System job", ""),
    ("AR_CLERK1", "Finance", "AR Clerk", ""),
    ("AR_CLERK2", "Finance", "AR Clerk", ""),
    ("AP_CLERK1", "Finance", "AP Clerk", ""),
    ("AP_CLERK2", "Finance", "AP Clerk", ""),
    ("GL_ACCT1", "Finance", "GL Accountant", ""),
    ("GL_ACCT2", "Finance", "GL Accountant", ""),
    ("FIN_MGR1", "Finance", "Finance Manager", ""),
    ("FIN_CTRL", "Finance", "Financial Controller", ""),
    ("EX_ACCT", "Finance", "GL Accountant", "2025-10-31"),  # left the company
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

lines = []          # every journal entry line
answer_key = []     # planted issues: (je_id, scenario)
je_counter = [100000]


def next_je_id():
    je_counter[0] += 1
    return f"JE{je_counter[0]}"


def money(low, high):
    """A realistic, non-round amount. Drawn on a log scale, like real transaction values,
    so the first digits follow Benford's law."""
    return round(math.exp(random.uniform(math.log(low), math.log(high))), 2)


def business_time():
    return f"{random.randint(9, 18):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"


def fiscal_period(d):
    # April = period 1 ... March = period 12
    return (d.month - 4) % 12 + 1


def add_je(posting_date, doc_type, source, created_by, approved_by, description, reference,
           debits, credits, entry_time=None, entry_date=None):
    """debits / credits: list of (account_number, amount)."""
    je_id = next_je_id()
    entry_date = entry_date or posting_date
    entry_time = entry_time or business_time()
    line_no = 0
    for side, items in (("D", debits), ("C", credits)):
        for account, amount in items:
            line_no += 1
            lines.append({
                "company_code": "IN01",
                "fiscal_year": "2025",
                "period": str(fiscal_period(posting_date)),
                "je_id": je_id,
                "line_no": str(line_no),
                "doc_type": doc_type,
                "source": source,
                "posting_date": posting_date.strftime("%d.%m.%Y"),   # SAP-style date
                "entry_date": entry_date.isoformat(),
                "entry_time": entry_time,
                "account_number": account,
                "debit_amount": f"{amount:.2f}" if side == "D" else "0.00",
                "credit_amount": f"{amount:.2f}" if side == "C" else "0.00",
                "currency": "INR",
                "description": description,
                "reference": reference,
                "created_by": created_by,
                "approved_by": approved_by,
            })
    return je_id


def last_business_day(year, month):
    d = (date(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1))
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normal business activity

# COMMAND ----------

vendor_invoices = []  # kept so we can duplicate a few later

d = fy_start
while d <= fy_end:
    if d.weekday() < 5:  # Monday to Friday
        # --- Order to Cash ---
        for _ in range(random.randint(4, 8)):
            net = money(5_000, 500_000)
            gst = round(net * 0.18, 2)
            clerk = random.choice(["AR_CLERK1", "AR_CLERK2"])
            add_je(d, "DR", "AR_SUBLEDGER", clerk, "FIN_MGR1",
                   "Sales invoice", f"INV-{random.randint(10000, 99999)}",
                   [("1100", round(net + gst, 2))], [("4000", net), ("2100", gst)])
        for _ in range(random.randint(3, 6)):
            amt = money(5_000, 500_000)
            add_je(d, "DZ", "AR_SUBLEDGER", random.choice(["AR_CLERK1", "AR_CLERK2"]), "FIN_MGR1",
                   "Customer receipt", f"RCPT-{random.randint(10000, 99999)}",
                   [("1000", amt)], [("1100", amt)])
        if random.random() < 0.3:
            net = money(10_000, 1_000_000)
            gst = round(net * 0.18, 2)
            add_je(d, "DR", "AR_SUBLEDGER", random.choice(["AR_CLERK1", "AR_CLERK2"]), "FIN_MGR1",
                   "Service invoice", f"INV-{random.randint(10000, 99999)}",
                   [("1100", round(net + gst, 2))], [("4100", net), ("2100", gst)])

        # --- Procure to Pay ---
        for _ in range(random.randint(3, 6)):
            expense = random.choice(["5000", "5000", "1200", "6200", "6300"])
            net = money(5_000, 500_000)
            gst = round(net * 0.18, 2)
            vendor = f"VND-{random.randint(100, 180)}"
            clerk = random.choice(["AP_CLERK1", "AP_CLERK2"])
            je = add_je(d, "KR", "AP_SUBLEDGER", clerk, "FIN_MGR1",
                        f"Vendor invoice {vendor}", f"{vendor}/BILL-{random.randint(1000, 9999)}",
                        [(expense, net), ("1300", gst)], [("2000", round(net + gst, 2))])
            vendor_invoices.append((je, d, expense, net, gst, vendor, clerk))
        for _ in range(random.randint(2, 5)):
            amt = money(5_000, 500_000)
            add_je(d, "KZ", "AP_SUBLEDGER", random.choice(["AP_CLERK1", "AP_CLERK2"]), "FIN_MGR1",
                   "Vendor payment", f"PAY-{random.randint(10000, 99999)}",
                   [("2000", amt)], [("1000", amt)])

        # --- Small manual reclasses by GL team (normal, properly approved) ---
        if random.random() < 0.15:
            amt = money(1_000, 100_000)
            add_je(d, "SA", "MANUAL", random.choice(["GL_ACCT1", "GL_ACCT2"]), "FIN_MGR1",
                   "Reclass marketing spend", f"GL-{random.randint(1000, 9999)}",
                   [("6300", amt)], [("6900", amt)])
    d += timedelta(days=1)

# --- Month-end Record to Report ---
month = date(fy_start.year, fy_start.month, 1)
while month <= fy_end:
    me = last_business_day(month.year, month.month)
    payroll = money(1_850_000, 1_950_000)
    add_je(me, "PR", "PAYROLL", "SYS_BATCH", "FIN_CTRL", "Monthly payroll", f"PAYRUN-{me:%Y%m}",
           [("6000", payroll)], [("2300", payroll)], entry_time="06:30:00")
    pay_day = me + timedelta(days=1)
    while pay_day.weekday() >= 5:
        pay_day += timedelta(days=1)
    if pay_day <= fy_end:
        add_je(pay_day, "KZ", "PAYROLL", "SYS_BATCH", "FIN_CTRL", "Salary disbursement",
               f"PAYRUN-{me:%Y%m}", [("2300", payroll)], [("1000", payroll)], entry_time="10:15:00")
    add_je(me, "AF", "FIXED_ASSETS", "SYS_BATCH", "FIN_CTRL", "Monthly depreciation", f"DEP-{me:%Y%m}",
           [("6400", 212_437.18)], [("1550", 212_437.18)], entry_time="07:05:00")
    add_je(me, "SA", "MANUAL", "GL_ACCT1", "FIN_MGR1", "Rent accrual", f"ACR-{me:%Y%m}",
           [("6100", 147_250.00)], [("2200", 147_250.00)])
    month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)

normal_count = je_counter[0] - 100000
print(f"Normal journal entries: {normal_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plant red flags
# MAGIC
# MAGIC These are the patterns auditors look for in **journal entry testing** (the fraud-risk procedure under
# MAGIC auditing standards SA 240 / ISA 240). Each planted entry is logged in the answer key.

# COMMAND ----------

def random_day(weekend=False):
    while True:
        d = fy_start + timedelta(days=random.randint(0, (fy_end - fy_start).days))
        if (d.weekday() >= 5) == weekend:
            return d

def manual(posting_date, created_by, approved_by, description, debits, credits, **kw):
    return add_je(posting_date, "SA", "MANUAL", created_by, approved_by, description,
                  f"GL-{random.randint(1000, 9999)}", debits, credits, **kw)

# A. Weekend postings
for _ in range(8):
    amt = money(20_000, 90_000)
    je = manual(random_day(weekend=True), "GL_ACCT2", "FIN_MGR1", "Expense correction",
                [("6200", amt)], [("2200", amt)])
    answer_key.append((je, "weekend_posting"))

# B. After-hours postings
for _ in range(8):
    amt = money(15_000, 80_000)
    t = f"{random.choice([22, 23, 1, 2, 3]):02d}:{random.randint(0, 59):02d}:00"
    je = manual(random_day(), "GL_ACCT1", "FIN_MGR1", "Accrual true-up",
                [("6300", amt)], [("2200", amt)], entry_time=t)
    answer_key.append((je, "after_hours_entry"))

# C. Round-amount entries
for amt in [500_000, 250_000, 1_000_000, 300_000, 750_000, 200_000, 400_000, 150_000]:
    je = manual(random_day(), "GL_ACCT2", "FIN_MGR1", "Provision for expenses",
                [("6900", float(amt))], [("2200", float(amt))])
    answer_key.append((je, "round_amount"))

# D. Manual revenue booked in the last days of a quarter (possible revenue inflation)
for (y, m) in [(2025, 6), (2025, 9), (2025, 12), (2026, 3), (2026, 3), (2025, 9)]:
    me = last_business_day(y, m) - timedelta(days=random.randint(0, 1))
    amt = money(300_000, 900_000)
    je = manual(me, "GL_ACCT1", "FIN_MGR1", "Revenue recognition - customer contract",
                [("1100", amt)], [("4000", amt)])
    answer_key.append((je, "period_end_manual_revenue"))

# E. Segregation of duties: same person creates and approves
for _ in range(6):
    amt = money(40_000, 150_000)
    je = manual(random_day(), "FIN_MGR1", "FIN_MGR1", "Expense reclass",
                [("6300", amt)], [("6200", amt)])
    answer_key.append((je, "self_approved"))

# F. Postings to the suspense account
for _ in range(5):
    amt = money(10_000, 120_000)
    je = manual(random_day(), "GL_ACCT2", "FIN_MGR1", "Unidentified receipt",
                [("1000", amt)], [("1900", amt)])
    answer_key.append((je, "suspense_or_rare_account"))

# G. Duplicate vendor invoices (same vendor, accounts and amount, a few days apart)
for (orig_je, d0, expense, net, gst, vendor, clerk) in random.sample(vendor_invoices, 6):
    dup_day = d0 + timedelta(days=random.randint(0, 3))
    je = add_je(dup_day, "KR", "AP_SUBLEDGER", clerk, "FIN_MGR1", f"Vendor invoice {vendor}",
                f"{vendor}/BILL-{random.randint(1000, 9999)}",
                [(expense, net), ("1300", gst)], [("2000", round(net + gst, 2))])
    answer_key.append((je, "duplicate_entry"))
    answer_key.append((orig_je, "duplicate_entry"))

# H. Suspicious wording in the description
for text in ["Adjustment per CFO instruction", "Plug to balance TB", "Manual override - urgent",
             "Adjust per CFO", "Plug entry", "Override approval limit"]:
    amt = money(25_000, 200_000)
    je = manual(random_day(), "GL_ACCT1", "FIN_MGR1", text, [("6900", amt)], [("2200", amt)])
    answer_key.append((je, "suspicious_description"))

# I. Entries created by a user who had already left the company
for _ in range(4):
    d = date(2025, 11, 1) + timedelta(days=random.randint(5, 120))
    while d.weekday() >= 5:
        d += timedelta(days=1)
    amt = money(30_000, 110_000)
    je = manual(d, "EX_ACCT", "FIN_MGR1", "Expense accrual", [("6200", amt)], [("2200", amt)])
    answer_key.append((je, "terminated_user"))

# J. A few entries that tick many boxes at once (these should rank highest)
for _ in range(3):
    t = f"{random.choice([23, 0, 1]):02d}:{random.randint(0, 59):02d}:00"
    je = manual(random_day(weekend=True), "FIN_MGR1", "FIN_MGR1", "Adjustment per CFO - year end",
                [("1100", 1_200_000.0)], [("4000", 1_200_000.0)], entry_time=t)
    for s in ["weekend_posting", "after_hours_entry", "round_amount", "self_approved",
              "suspicious_description"]:
        answer_key.append((je, s))

print(f"Total journal entries: {je_counter[0] - 100000:,}  |  planted red flags: {len(set(j for j, _ in answer_key))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plant data quality problems
# MAGIC
# MAGIC Real ERP extracts are messy. The silver layer has to catch these before any testing happens.

# COMMAND ----------

# K. Unbalanced entries (debits do not equal credits)
for _ in range(3):
    amt = money(10_000, 60_000)
    je = manual(random_day(), "GL_ACCT2", "FIN_MGR1", "Accrual", [("6200", amt)], [("2200", round(amt - 500, 2))])
    answer_key.append((je, "dq_unbalanced"))

# L. A line posted to an account that does not exist in the chart of accounts
for _ in range(2):
    amt = money(10_000, 60_000)
    je = manual(random_day(), "GL_ACCT2", "FIN_MGR1", "Misc posting", [("9999", amt)], [("2200", amt)])
    answer_key.append((je, "dq_invalid_account"))

# M. Rows sent twice by the extract job (exact duplicates)
resent = random.sample(lines, 40)
lines.extend(dict(r) for r in resent)

random.shuffle(lines)
print(f"Journal entry lines in extract: {len(lines):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the extract to the raw volume

# COMMAND ----------

def write_csv(rows, columns, name):
    pdf = pd.DataFrame(rows, columns=columns)
    (spark.createDataFrame(pdf)
          .coalesce(1)
          .write.mode("overwrite")
          .option("header", True)
          .csv(f"{RAW_PATH}/{name}"))
    print(f"{name:<20} {len(pdf):>7,} rows  ->  {RAW_PATH}/{name}")

write_csv(lines, list(lines[0].keys()), "journal_entries")
write_csv(chart_of_accounts, ["account_number", "account_name", "account_type", "process"], "chart_of_accounts")
write_csv(users, ["user_id", "department", "role", "termination_date"], "users")
write_csv(answer_key, ["je_id", "scenario"], "_answer_key")

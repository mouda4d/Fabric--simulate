# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c54f02f-db6d-408b-af73-0d7359844470",
# META       "default_lakehouse_name": "lh_contoso",
# META       "default_lakehouse_workspace_id": "200ec9d1-af61-4293-b45d-b22223a97add",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c54f02f-db6d-408b-af73-0d7359844470"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "aa8ac93e-c2fd-43d6-845c-b50feb8b98f3",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Left its seeds while i was sleeping**

# CELL ********************

from pyspark.sql import functions as F

failures = []
def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(name)

silver = spark.table("silver_orders")
# display(silver.groupBy("order_id").agg(F.count("*").alias("order_dupes")).filter(F.col("order_dupes") > 1))
dupes = silver.groupBy("order_id").agg(F.count("*").alias("order_dupes")).filter(F.col("order_dupes") > 1).count()

check("silver has one row per order_id", dupes == 0, f"{dupes} duplicated ids")
check("no PII column in silver", "customer_email" not in silver.columns)

if failures:
    raise AssertionError(f"{len(failures)} check(s) failed: {failures}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

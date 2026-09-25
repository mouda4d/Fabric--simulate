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

# # **I've come to talk with you again**

# PARAMETERS CELL ********************

run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import functions as F, Window

latest_first = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(),
                                                      F.col("_ingested_at").desc())
silver = (spark.table("bronze_orders")
          .drop("customer_email")                                  # PII stops at bronze
          .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(10,2))"))
          .withColumn("order_ts", F.try_to_timestamp("order_ts"))
          .withColumn("updated_at", F.try_to_timestamp("updated_at"))
          .withColumn("_rn", F.row_number().over(latest_first))
          .filter("_rn = 1")
          .drop("_rn"))
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")

rows = spark.table("silver_orders").count()
(spark.createDataFrame([(datetime.now(timezone.utc), run_id, "silver", rows, "full rebuild, latest per order")],
                       "logged_at timestamp, run_id string, step string, rows bigint, note string")
      .write.mode("append").saveAsTable("ops_run_log"))
print(f"silver_orders: {rows} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# # **And the vision, that was planted in my brain**

# PARAMETERS CELL ********************

run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SCHEMA = StructType([StructField(c, DoubleType() if c == "amount" else StringType())
                     for c in ["order_id", "customer_id", "customer_email", "card_id", "zone",
                               "amount", "status", "order_ts", "updated_at"]])

bronze = (spark.read.schema(SCHEMA).json("Files/landing/orders/")
          .select("*", F.col("_metadata.file_path").alias("_source_file"))
          .withColumn("_ingested_at", F.current_timestamp()))
bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_orders")

rows = spark.table("bronze_orders").count()
(spark.createDataFrame([(datetime.now(timezone.utc), run_id, "bronze", rows, "full reload")],
                       "logged_at timestamp, run_id string, step string, rows bigint, note string")
      .write.mode("append").saveAsTable("ops_run_log"))
print(f"bronze_orders: {rows} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

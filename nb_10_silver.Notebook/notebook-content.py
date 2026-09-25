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

from pyspark.sql import functions as F

landing = spark.read.json("Files/landing/orders/")   # schema inferred: fine for a skeleton, fixed in Sprint 2

silver = (landing
    .drop("customer_email")                                        # privacy minimum: PII stops here
    .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(10,2))"))
    .withColumn("order_ts", F.try_to_timestamp("order_ts"))
    .withColumn("updated_at", F.try_to_timestamp("updated_at")))

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
print(spark.table("silver_orders").count(), "rows in silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

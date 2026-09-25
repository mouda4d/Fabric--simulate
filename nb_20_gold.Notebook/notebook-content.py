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

# # **Because a vision softly cre~~~~~~~~~eping**

# CELL ********************

spark.sql("""
    CREATE OR REPLACE TABLE gold_daily_revenue AS
    SELECT CAST(order_ts AS DATE) AS order_date,
           zone,
           COUNT(*)               AS orders,
           SUM(amount)            AS revenue
    FROM silver_orders
    WHERE status = 'delivered'
    GROUP BY CAST(order_ts AS DATE), zone
""")
display(spark.table("gold_daily_revenue").orderBy("order_date", "zone"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

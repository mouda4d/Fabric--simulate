# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Contoso Eats — orders platform
# 
# **Owner:** ____ · **Failure emails go to:** ____ · **Capacity region:** ____
# 
# ## What it does
# Lands order batches from the app, builds cleaned and business-ready tables, and serves the
# revenue dashboard. Runs daily in Prod.
# 
# ## Pipeline
# pl_orders: simulate -> silver -> gold. Two retries per step (3 minutes apart); 1-hour timeout per step.
# 
# ## Data dictionary
# | Table            | Column         | Meaning                                        | Classification            |
# |------------------|----------------|------------------------------------------------|---------------------------|
# | landing (files)  | customer_email | Customer's email address                       | Restricted: PII. Never beyond bronze |
# | silver_orders    | order_id       | One row per order (from Sprint 2)              | Internal                  |
# | silver_orders    | customer_id    | Pseudonymous customer key                      | Confidential              |
# | silver_orders    | card_id        | Payment token; we never receive card numbers   | Confidential              |
# | silver_orders    | amount         | Order value, 2 decimal places                  | Internal                  |
# | gold_daily_revenue | revenue      | Delivered revenue per day and zone             | Internal                  |
# 
# ## Known limitations (the backlog, honestly)
# - Re-sent orders are double-counted (Sprint 2)
# - Every run reprocesses every file (Sprint 3)
# - Settings are the same in every stage (Sprint 4)
# - Bad data is not stopped (Sprint 5)


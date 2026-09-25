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

# # **Hello Darkness My Ol' Friend**

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

orders_per_run = 500    # 0 = skip this run (used later to prove re-runs are harmless)
dup_rate       = 0.08   # the app re-sends about 8% of orders
update_rate    = 0.10   # delivered orders that get refunded later
late_rate      = 0.05   # orders that turn up 1-3 days late
bad_row_pct    = 0      # % of rows to break on purpose. Use it in Test, never in Prod
fraud_rings    = 1      # one card used by many customers inside ten minutes
refund_abusers = 1      # customers who refund almost everything
seed           = -1     # -1 = new data every run; any other number = repeatable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if orders_per_run <= 0:
    notebookutils.notebook.exit("skipped: orders_per_run = 0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random, uuid
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.errors import AnalysisException

rng  = random.Random(None if seed < 0 else seed)
now  = datetime.now(timezone.utc).replace(microsecond=0)
BASE = "Files/landing/orders/"
SCHEMA = StructType([StructField(c, DoubleType() if c == "amount" else StringType())
                     for c in ["order_id", "customer_id", "customer_email", "card_id", "zone",
                               "amount", "status", "order_ts", "updated_at"]])
ZONES     = ["Riverside", "Hillcrest", "Harbor", "Old Town", "Airport"]
FINAL     = ["delivered"] * 8 + ["cancelled", "refunded"]
CUSTOMERS = [f"C{n:05d}" for n in range(1, 1201)]
CARDS     = [f"K{n:05d}" for n in range(1, 1001)]

def order(ts, **overrides):
    done = ts + timedelta(minutes=rng.randint(20, 75))
    finished = done <= now
    o = {"order_id":    str(uuid.UUID(int=rng.getrandbits(128), version=4)),
         "customer_id": rng.choice(CUSTOMERS),
         "card_id":     rng.choice(CARDS),
         "zone":        rng.choice(ZONES),
         "amount":      round(rng.lognormvariate(3.1, 0.55), 2),
         "status":      rng.choice(FINAL) if finished else "placed",
         "order_ts":    ts.isoformat(),
         "updated_at":  (done if finished else ts).isoformat()}
    o.update(overrides)
    o["customer_email"] = f"{o['customer_id'].lower()}@example.com"   # PII: must never reach silver
    return o

# 1. normal orders over the last 24 hours, a few of them arriving days late
rows = []
for _ in range(orders_per_run):
    ts = now - timedelta(minutes=rng.randint(0, 24 * 60))
    if rng.random() < late_rate:
        ts -= timedelta(days=rng.randint(1, 3))
    rows.append(order(ts))
originals = list(rows)

# 2. the app re-sends some orders unchanged
rows += [dict(r) for r in rng.sample(originals, int(len(originals) * dup_rate))]

# 3. some delivered orders are refunded hours later
older = [r for r in originals if r["status"] == "delivered"
         and datetime.fromisoformat(r["updated_at"]) < now - timedelta(hours=2)]
for r in rng.sample(older, min(len(older), int(len(originals) * update_rate))):
    prev = datetime.fromisoformat(r["updated_at"])
    later = prev + timedelta(seconds=rng.randint(3600, int((now - prev).total_seconds())))
    rows.append(dict(r, status="refunded", updated_at=later.isoformat()))

# 4. orders still 'placed' in the previous batch get completed in this one
completions = 0
try:
    landed = spark.read.schema(SCHEMA).json(BASE)
    last = landed.agg(F.max("batch")).first()[0]
    if last:
        for r in landed.filter((F.col("batch") == last) & (F.col("status") == "placed")).collect():
            if rng.random() < 0.9:
                d = r.asDict()
                d.pop("batch", None)
                d.update(status=rng.choice(["delivered"] * 9 + ["cancelled"]), updated_at=now.isoformat())
                rows.append(d)
                completions += 1
except AnalysisException:
    pass  # first run: nothing has landed yet

# 5. fraud rings: one card, many different customers, all inside ONE ten-minute window
for _ in range(fraud_rings):
    card  = rng.choice(CARDS)
    start = now - timedelta(minutes=rng.randint(90, 600))
    start = start.replace(minute=start.minute - start.minute % 10, second=0)
    for cust in rng.sample(CUSTOMERS, rng.randint(6, 9)):
        ts = start + timedelta(seconds=rng.randint(0, 590))
        rows.append(order(ts, customer_id=cust, card_id=card, status="delivered",
                          amount=round(rng.uniform(80, 240), 2)))

# 6. refund abusers: one customer, lots of refunds
for _ in range(refund_abusers):
    cust = rng.choice(CUSTOMERS)
    for _ in range(rng.randint(4, 6)):
        rows.append(order(now - timedelta(minutes=rng.randint(120, 24 * 60)),
                          customer_id=cust, status="refunded"))

# 7. broken rows, only when asked for (Test stage)
for r in rng.sample(rows, int(len(rows) * bad_row_pct / 100)):
    fault = rng.choice(["amount_null", "amount_negative", "status_unknown", "customer_missing"])
    if fault == "amount_null":       r["amount"] = None
    elif fault == "amount_negative": r["amount"] = -abs(r["amount"] or 1.0)
    elif fault == "status_unknown":  r["status"] = "unknown"
    else:                            r["customer_id"] = None

rng.shuffle(rows)
batch = now.strftime("%Y%m%dT%H%M%SZ")
spark.createDataFrame(rows, SCHEMA).coalesce(1).write.mode("overwrite").json(f"{BASE}batch={batch}")
print(f"batch {batch}: {len(rows)} rows, {completions} completions of earlier orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

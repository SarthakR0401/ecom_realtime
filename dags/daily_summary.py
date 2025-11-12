# dags/daily_summary.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import csv, os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def compute_daily_summary(execution_date=None, **context):
    client = MongoClient("mongodb://localhost:27017")
    db = client['ecom_analytics']
    raw = db.raw_events
    # For simplicity we'll use today's date (execution_date is string)
    start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    pipeline = [
        {"$match": {"timestamp": {"$gte": start.isoformat(), "$lt": end.isoformat()}}},
        {"$group": {"_id": "$product_id",
                    "views": {"$sum": {"$cond":[{"$eq":["$type","page_view"]},1,0]}},
                    "purchases": {"$sum": {"$cond":[{"$eq":["$type","purchase"]},1,0]}},
                    "revenue": {"$sum": {"$cond":[{"$eq":["$type","purchase"]},"$price",0]}}}},
        {"$sort": {"views": -1}}
    ]
    results = list(raw.aggregate(pipeline))
    outdir = "/tmp/ecom_reports"
    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, f"daily-{start.date()}.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id","views","purchases","revenue"])
        for r in results:
            writer.writerow([r["_id"], r["views"], r["purchases"], r["revenue"]])
    db.daily_summary.insert_one({"date": str(start.date()), "count": len(results)})

with DAG('daily_ecom_summary', start_date=datetime(2025,1,1), schedule_interval='@daily', default_args=default_args, catchup=False, max_active_runs=1) as dag:
    t1 = PythonOperator(task_id='compute_daily_summary', python_callable=compute_daily_summary)
    t1

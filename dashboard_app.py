# -------------------------------
# Streamlit Real-Time Dashboard
# -------------------------------

import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timezone
import time

# -------------------------------
# CONFIGURATION
# -------------------------------
st.set_page_config(page_title="E-Commerce Real-Time Analytics", layout="wide")

# MongoDB connection
# Use your MongoDB Atlas connection string here
# Example: st.secrets["MONGO_URI"] if using Streamlit Secrets
try:
    client = MongoClient("mongodb://localhost:27017")  # or st.secrets["MONGO_URI"]
    db = client["ecom_analytics"]
except Exception as e:
    st.error("❌ Failed to connect to MongoDB")
    st.stop()

# -------------------------------
# FETCHING FUNCTIONS
# -------------------------------

def fetch_top_products():
    doc = db.top_products.find().sort("ts", -1).limit(1)
    doc = list(doc)
    if not doc:
        return pd.DataFrame(columns=["product_id", "count"])
    return pd.DataFrame(doc[0]["top"])

def fetch_category_distribution():
    pipeline = [
        {"$match": {"window_start": {"$exists": True}}},
        {"$group": {"_id": "$category", "count": {"$sum": "$count"}}},
        {"$project": {"category": "$_id", "count": 1, "_id": 0}}
    ]
    data = list(db.product_views.aggregate(pipeline))
    return pd.DataFrame(data)

def fetch_trend():
    pipeline = [
        {"$group": {"_id": "$window_start", "total_views": {"$sum": "$count"}}},
        {"$sort": {"_id": -1}},
        {"$limit": 10},
        {"$project": {"window_start": "$_id", "total_views": 1, "_id": 0}}
    ]
    data = list(db.product_views.aggregate(pipeline))
    for d in data:
        try:
            dt = datetime.fromisoformat(d["window_start"].replace("Z", "+00:00"))
            d["window_start"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
    return pd.DataFrame(list(reversed(data)))

def fetch_alerts():
    data = list(db.alerts.find().sort("ts", -1).limit(10))
    alerts = []
    for d in data:
        ts = d.get("ts")
        if isinstance(ts, datetime):
            ts = ts.replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        alerts.append({
            "Timestamp": ts,
            "Product ID": d.get("product_id"),
            "Count": d.get("count")
        })
    return pd.DataFrame(alerts)

# -------------------------------
# STREAMLIT UI
# -------------------------------
st.title("📊 E-Commerce Real-Time Analytics Dashboard")
st.caption("Auto-refreshing every 5 seconds for real-time insights.")

placeholder = st.empty()

# Live refresh every 5s
while True:
    with placeholder.container():
        col1, col2 = st.columns(2)

        # --- Top Products ---
        top_df = fetch_top_products()
        if not top_df.empty:
            col1.subheader("🏆 Top Products")
            col1.bar_chart(top_df.set_index("product_id")["count"])
        else:
            col1.info("No top product data yet.")

        # --- Category Distribution ---
        cat_df = fetch_category_distribution()
        if not cat_df.empty:
            col2.subheader("📦 Category Distribution")
            fig = px.pie(cat_df, values="count", names="category", hole=0.4)
            col2.plotly_chart(fig, use_container_width=True)
        else:
            col2.info("No category data yet.")

        # --- Views Trend ---
        st.subheader("📈 Views Trend (Last 10 Windows)")
        trend_df = fetch_trend()
        if not trend_df.empty:
            fig2 = px.line(trend_df, x="window_start", y="total_views", markers=True)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No trend data yet.")

        # --- Recent Alerts ---
        st.subheader("⚠️ Recent Alerts")
        alert_df = fetch_alerts()
        if not alert_df.empty:
            st.dataframe(alert_df, use_container_width=True)
        else:
            st.info("No alerts to display.")

        st.markdown(f"**⏱ Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(5)

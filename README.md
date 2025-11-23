
# 🛒 Real-Time E-Commerce Analytics System

A real-time analytics system built with **Python, Apache Kafka, MongoDB, Flask, Chart.js, and Docker**. It simulates e-commerce user events, processes streaming data in real time, and visualizes insights on a live dashboard.

---

## 🚀 Key Features

* **Real-Time Streaming** using Kafka (page views, add-to-cart, purchases)
* **Live ETL Processing** with a Python consumer (1-minute aggregations)
* **Product & Category Insights** updated every 5 seconds
* **Spike Detection** for sudden traffic surges
* **Interactive Dashboard** built with Flask + Chart.js
* **Dockerized Environment** for Kafka, Zookeeper & MongoDB

---

## 🧱 Architecture Overview

```
Producer.py  
   → Kafka (events.raw)  
      → Stream Aggregator (Python)  
         → MongoDB (raw_events, product_views, top_products, alerts)  
            → Flask API  
               → Chart.js Dashboard
```

---

## ⚙️ Getting Started

```bash
docker-compose up -d          # Start Kafka, Zookeeper, MongoDB
pip install -r requirements.txt
python producer.py            # Start event generator
python stream_aggregator.py   # Start real-time processor
python dashboard_app.py       # Launch dashboard
```


## 📚 What I Learned

Developed hands-on experience with:

* Real-time ETL pipelines
* Event streaming systems
* Window-based aggregations
* NoSQL schema design
* Building real-time dashboards
* Containerized microservices


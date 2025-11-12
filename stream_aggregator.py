import json
import time
from confluent_kafka import Consumer
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from collections import defaultdict
import pytz

# -----------------------
# CONFIGURATION
# -----------------------
KAFKA_GROUP = "stream-agg-group"
TOP_N = 10
WINDOW_SECONDS = 60
SPIKE_THRESHOLD = 5  # Adjust for your load

# -----------------------
# KAFKA CONSUMER
# -----------------------
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': KAFKA_GROUP,
    'auto.offset.reset': 'earliest'
})
c.subscribe(['events.raw'])

# -----------------------
# MONGODB CONNECTION
# -----------------------
mongo = MongoClient("mongodb://localhost:27017")
db = mongo['ecom_analytics']
raw = db.raw_events
product_views = db.product_views
top_products = db.top_products
alerts = db.alerts

# -----------------------
# IN-MEMORY WINDOW STORE
# -----------------------
windows = defaultdict(lambda: defaultdict(int))
product_category = {}  # product_id → category
last_flush = time.time()

# -----------------------
# UTILITY FUNCTIONS
# -----------------------
def window_start(ts):
    """Return the start of the aggregation window for the event timestamp."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    epoch = int(dt.timestamp())
    start = epoch - (epoch % WINDOW_SECONDS)
    return datetime.utcfromtimestamp(start).replace(tzinfo=pytz.UTC).isoformat()


def flush_windows():
    """Flush the in-memory windows to MongoDB."""
    global windows
    ops = []
    top_docs = []

    for wstart, counts in list(windows.items()):
        for prod, cnt in counts.items():
            # Get category from memory or fallback to Mongo lookup
            category = product_category.get(prod)
            if not category:
                sample = raw.find_one(
                    {"product_id": prod, "category": {"$ne": None}},
                    {"category": 1}
                )
                if sample and sample.get("category"):
                    category = sample["category"]
                    product_category[prod] = category
                else:
                    category = "uncategorized"

            ops.append(UpdateOne(
                {"product_id": prod, "window_start": wstart},
                {"$inc": {"count": cnt}, "$set": {"category": category}},
                upsert=True
            ))

        # Compute top N products for this window
        sorted_items = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
        top_docs.append({
            "window_start": wstart,
            "top": [{"product_id": p, "count": c} for p, c in sorted_items],
            "ts": datetime.utcnow()
        })

    if ops:
        product_views.bulk_write(ops)
    if top_docs:
        top_products.insert_many(top_docs)

    # Reset after flushing
    windows = defaultdict(lambda: defaultdict(int))


# -----------------------
# MAIN LOOP
# -----------------------
try:
    print("Starting stream aggregator... Press Ctrl+C to stop.")
    while True:
        msg = c.poll(timeout=1.0)
        now = time.time()

        # Periodically flush windowed aggregates
        if now - last_flush >= WINDOW_SECONDS:
            flush_windows()
            last_flush = now

        if msg is None:
            continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue

        # Decode Kafka event
        ev = json.loads(msg.value().decode('utf-8'))

        # Store raw event
        raw.insert_one(ev)

        # Only aggregate page view events
        if ev.get('type') == 'page_view':
            w = window_start(ev['timestamp'])
            prod_id = ev['product_id']
            windows[w][prod_id] += 1

            # Store category in memory mapping
            if ev.get('category'):
                product_category[prod_id] = ev['category']

            # Spike alert
            if windows[w][prod_id] > SPIKE_THRESHOLD:
                alerts.insert_one({
                    "alert_type": "view_spike",
                    "product_id": prod_id,
                    "window_start": w,
                    "count": windows[w][prod_id],
                    "ts": datetime.utcnow()
                })

except KeyboardInterrupt:
    print("Shutting down stream aggregator...")
    flush_windows()
    c.close()

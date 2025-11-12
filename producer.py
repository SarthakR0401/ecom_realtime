import json
import time
import random
import uuid
from confluent_kafka import Producer
from faker import Faker
from datetime import datetime, timezone

fake = Faker()

# Kafka connection
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Product and category definitions
PRODUCTS = [f"prod-{i}" for i in range(1, 101)]
POPULAR_PRODUCTS = ["prod-1", "prod-2", "prod-3", "prod-5", "prod-7"]

CATEGORIES = ['electronics', 'books', 'fashion', 'home', 'toys']
CATEGORY_WEIGHTS = [0.35, 0.2, 0.2, 0.15, 0.1]


def random_category():
    return random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS)[0]


def weighted_product():
    if random.random() < 0.5:  # 50% chance for a popular one
        return random.choice(POPULAR_PRODUCTS)
    return random.choice(PRODUCTS)


def make_event():
    event_type = random.choices(
        ['page_view', 'add_to_cart', 'purchase'],
        weights=[0.8, 0.15, 0.05]
    )[0]
    product = weighted_product()
    user = f"user-{random.randint(1, 200)}"
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "user_id": user,
        "session_id": f"sess-{random.randint(1, 500)}",
        "product_id": product,
        "category": random_category(),
        "price": round(random.uniform(5.0, 500.0), 2)
                 if event_type == 'purchase' else None,
        "timestamp": ts,
        "metadata": {
            "device": random.choice(['mobile', 'desktop']),
            "referrer": fake.domain_name()
        }
    }


def delivery(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def run(events_per_cycle=5, interval_sec=5):
    """Produce several events every few seconds."""
    print(f"Starting producer... sending {events_per_cycle} events every {interval_sec} seconds. Press Ctrl+C to stop.")
    try:
        while True:
            for _ in range(events_per_cycle):
                ev = make_event()
                producer.produce('events.raw',
                                 key=ev['user_id'],
                                 value=json.dumps(ev),
                                 callback=delivery)
            producer.flush()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {events_per_cycle} events")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.flush()


if __name__ == "__main__":
    run(events_per_cycle=5, interval_sec=5)

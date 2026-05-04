# import packages

import json
import random
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer


# ----------------------------
# Kafka Configuration
# ----------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ----------------------------
# Static Pools (Simulated Data Sources)
# ----------------------------
USERS = [f"user_{i}" for i in range(100, 121)]
MERCHANTS = ["amazon", "walmart", "target", "bestbuy", "flipkart"]
LOCATIONS = ["India", "USA", "Canada", "UK", "Germany"]


# ----------------------------
# Transaction Generator
# ----------------------------
def generate_transaction():
    transaction = {
        "transaction_id": str(uuid.uuid4()),

        "user_id": random.choice(USERS),

        "merchant_id": random.choice(MERCHANTS),

        "transaction_amount": round(random.uniform(10.0, 20000.0), 2),

        "transaction_time": datetime.utcnow().isoformat(),

        "device_id": f"device_{random.randint(1, 50)}",

        "location": random.choice(LOCATIONS),

        "is_international": random.choice([True, False])
    }

    return transaction


# ----------------------------
# Continuous Streaming Loop
# ----------------------------
if __name__ == "__main__":
    print("Starting Normal Transaction Producer...\n")

    try:
        while True:
            event = generate_transaction()

            print("Sending Event:", event)

            producer.send("transactions", event)
            producer.flush()

            time.sleep(random.randint(5, 10))

    except KeyboardInterrupt:
        print("\nStopping Producer...")
        producer.close()

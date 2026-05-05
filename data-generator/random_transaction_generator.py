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
# Normal Transaction Generator
# ----------------------------
def generate_normal_transaction():
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
# Fraud Transaction Generator - 1
# ----------------------------
def generate_high_amount_transaction():
    transaction = generate_normal_transaction()
    transaction["transaction_amount"] = round(random.uniform(30000.0, 60000.0),2)
    return transaction

# ----------------------------
# Fraud Transaction Generator - 2
# ----------------------------

def generate_high_velocity_transaction(user_id):
    transaction = []

    for _ in range(6): # since the no of transactions should be 5 atleast
        transactions = {
        "transaction_id": str(uuid.uuid4()),

        "user_id": user_id,

        "merchant_id": random.choice(MERCHANTS),

        "transaction_amount": round(random.uniform(10.0, 20000.0), 2),

        "transaction_time": datetime.utcnow().isoformat(),

        "device_id": f"device_{random.randint(1, 50)}",

        "location": random.choice(LOCATIONS),

        "is_international": False
    }
    
    transaction.append(transactions)

    return transaction
 
        


# ----------------------------
# Continuous Streaming Loop
# ----------------------------
if __name__ == "__main__":
    print("Starting Normal Transaction Producer...\n")

    try:
        while True:

            rand = random.random()

            # 80 % normal transactions 
            if rand < 0.8:
                event = generate_normal_transaction()

                print("Sending Normal Event:", event)

                producer.send("transactions", event)
            
            # 10% fraud transactions
            elif rand < 0.9:
                event = generate_high_amount_transaction()
                
                print("HIGH AMOUNT", event)
                producer.send("transactions", event)
            
            # 10% velocity fraud transactions
            else:
                user_id = random.choice(USERS)
                event = generate_high_velocity_transaction(user_id)

                # sending high velocity_transaction
                print("HIGH VELOCITY fraud for user:",user_id)
                for e in event:
                    producer.send("transactions",e)

            producer.flush()

            time.sleep(random.randint(2,5))

    except KeyboardInterrupt:
        print("\nStopping Producer...")
        producer.close()

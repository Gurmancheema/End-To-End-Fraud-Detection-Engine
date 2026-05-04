# import packages
# Kafka sends bytes, not Python objects.
import json
from kafka import KafkaProducer
from datetime import datetime
import time

producer = KafkaProducer( bootstrap_servers="localhost:9092",
                            value_serializer=lambda v: json.dumps(v).encode('utf-8')
                            )


# dummy json

dummy_test_json  = {"testing_key":"testing_pair"}

# creating couple of fraudulent transactions to test the embedded fraud rules in the pipeline
# import some packages
import uuid
import random

USERS = [f"user_id_{i}" for i in range (1,2)]
MERCHANTS = ["walmart","target","fortinos","metro"]
LOCATIONS = ["India","Canada","China","USA","pakistan"]

def dummy_fraud_transactions():
    transactions = {
            "transaction_id" : str(uuid.uuid4()),
            "user_id" : random.choice(USERS),
            "merchant_id" : random.choice(MERCHANTS),
            "transaction_amount" : round(random.uniform(20000.0,30000.0),2),
            "transaction_time" : datetime.utcnow().isoformat(),
            "device_id" : [f"device_{i}" for i in range (10,20)],
            "location" : random.choice(LOCATIONS),
            "is_international": random.choice([True,False])
            }
    return transactions
# creating entry point of the application
if __name__ =="__main__":
    print("Starting producer to send fraudulent transactions")

    try:
        while True:
            event = dummy_fraud_transactions()
            print("Sending transactions",producer.send("transactions",event))
            producer.flush()

            time.sleep(random.randint(1,2))

    except KeyboardInterrupt:
        print("Stopping transactions")
        producer.close()
    

#publishing it to kafka
# it needs an already created topic if auto_create_topic is not enabled

#producer.send("transactions",dummy_test_json)
#producer.flush()

#print("Everything ran successfully")

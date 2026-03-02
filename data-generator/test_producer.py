# import packages
# Kafka sends bytes, not Python objects.
import json
from kafka import KafkaProducer

producer = KafkaProducer( bootstrap_servers="localhost:9092",
                            value_serializer=lambda v: json.dumps(v).encode('utf-8')
                            )


# dummy json

dummy_test_json  = {"testing_key":"testing_pair"}

#publishing it to kafka
# it needs an already created topic if auto_create_topic is not enabled

producer.send("transactions",dummy_test_json)
producer.flush()

print("Everything ran successfully")

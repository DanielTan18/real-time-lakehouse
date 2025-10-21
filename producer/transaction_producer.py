from kafka import KafkaProducer
from faker import Faker
import json, time, random


### generates fake transaction data and sends it to a kafka topic
### this file will be ran locally after starting docker containers.

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9093',  # Changed to port 9093
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    txn = {
        "txn_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(1.0, 200.0), 2),
        "currency": random.choice(["USD", "MYR", "SGD"]),
        "merchant": random.choice(["Greb", "Zhopee", "DaoBao", "Chaji", "Zussy Kopi"]),
        "txn_type": random.choice(["DEBIT", "CREDIT"]),
        "timestamp": fake.iso8601()
    }
    producer.send('transactions', txn)
    print(f"Sent: {txn}")
    time.sleep(1)
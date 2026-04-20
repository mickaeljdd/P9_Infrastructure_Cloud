from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='redpanda:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

types_demande = ["incident", "question", "réclamation", "demande_info"]
priorites = ["faible", "moyenne", "haute", "critique"]

def generate_ticket():
    return {
        "ticket_id": random.randint(1000, 9999),
        "client_id": random.randint(1, 100),
        "timestamp": datetime.now().isoformat(),
        "demande": "Problème technique simulé",
        "type_demande": random.choice(types_demande),
        "priorite": random.choice(priorites)
    }

while True:
    ticket = generate_ticket()
    producer.send("client_tickets", ticket)
    print(f"Sent: {ticket}")
    time.sleep(1)
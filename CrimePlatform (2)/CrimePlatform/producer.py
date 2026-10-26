
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

crime_types = ["Theft", "Burglary", "Robbery", "Assault"]
locations = ["Area A", "Area B", "Area C", "Area D"]

print("Crime event stream started. Press Ctrl+C to stop.")

try:
    while True:
        event = {
            "event_id": f"EVT-{int(time.time() * 1000)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "crime_type": random.choice(crime_types),
            "location": random.choice(locations),
            "source": "SIMULATED_DEMO",
        }

        producer.send("crime-events", value=event)
        producer.flush()
        print("Sent:", event)
        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()

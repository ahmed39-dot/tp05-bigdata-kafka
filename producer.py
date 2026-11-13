
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "traffic-data"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

print("Starting Traffic Producer...")

try:
    for event_id in range(1, 101):

        # Generate simulated traffic data
        vehicle_count = random.randint(5, 100)
        average_speed = round(random.uniform(10, 120), 2)

        # Determine congestion level
        if vehicle_count >= 70 or average_speed < 30:
            congestion_level = "HIGH"
        elif vehicle_count >= 40 or average_speed < 60:
            congestion_level = "MEDIUM"
        else:
            congestion_level = "LOW"

        event = {
            "event_id": event_id,
            "sensor_id": f"traffic-{random.randint(1, 4):02d}",
            "vehicle_count": vehicle_count,
            "average_speed": average_speed,
            "congestion_level": congestion_level,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Send event to Kafka
        future = producer.send(TOPIC, value=event)
        future.get(timeout=10)

        print("Sent:", event)

        time.sleep(1)

    producer.flush()

except KeyboardInterrupt:
    print("Producer interrupted.")

finally:
    producer.close()
    print("Producer closed.")

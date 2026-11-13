
import json
from kafka import KafkaConsumer

TOPIC = "traffic-data"

# Connect to Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="traffic-monitor-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Traffic Monitoring Consumer started...")
print("Waiting for traffic data...\n")

try:
    for message in consumer:

        event = message.value

        event_id = event["event_id"]
        sensor_id = event["sensor_id"]
        vehicle_count = event["vehicle_count"]
        average_speed = event["average_speed"]
        congestion = event["congestion_level"]
        timestamp = event["timestamp"]

        print("-----------------------------------")
        print(f"Event ID: {event_id}")
        print(f"Sensor: {sensor_id}")
        print(f"Vehicle Count: {vehicle_count}")
        print(f"Average Speed: {average_speed} km/h")
        print(f"Congestion Level: {congestion}")
        print(f"Timestamp: {timestamp}")

        if congestion == "HIGH":
            print("ALERT: HIGH TRAFFIC CONGESTION!")

        elif congestion == "MEDIUM":
            print("WARNING: Moderate traffic congestion.")

        else:
            print("Traffic is normal.")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()

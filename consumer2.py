
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "traffic-data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="traffic-monitor-group-2",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Traffic Consumer Group 2 is listening...")

try:
    for message in consumer:
        event = message.value

        print(
            f'Group 2 | Sensor: {event["sensor_id"]} | '
            f'Vehicles: {event["vehicle_count"]} | '
            f'Speed: {event["average_speed"]} km/h | '
            f'Congestion: {event["congestion_level"]}'
        )

        if event["congestion_level"] == "HIGH":
            print("ALERT: High traffic congestion detected!")

except KeyboardInterrupt:
    print("Consumer 2 stopped.")

finally:
    consumer.close()

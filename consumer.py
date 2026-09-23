from kafka import KafkaConsumer
import json

def json_deserializer(m):
    return json.loads(m.decode("utf-8"))

consumer = KafkaConsumer(
    "user-activity",
    bootstrap_servers=['localhost:9092'],
    group_id="activity-processor",
    auto_offset_reset="earliest",  # ← This means: read from beginning if new
    enable_auto_commit=True,
    value_deserializer=json_deserializer
)

print("👂 Listening... (Ctrl+C to stop)\n")
try:
    for msg in consumer:
        e = msg.value
        print(f"📥 Partition {msg.partition} | Offset {msg.offset} | {e['user']} → {e['action']}")
except KeyboardInterrupt:
    print("\n⏹️ Stopped")
finally:
    consumer.close()
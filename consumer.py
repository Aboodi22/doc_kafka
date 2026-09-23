from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "user-activity",
    bootstrap_servers=['localhost:9092'],
    group_id="activity-processor",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
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
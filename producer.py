from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

users = ["alice", "bob", "charlie", "diana"]
actions = ["login", "view", "purchase", "logout"]

print("🚀 Sending events... (Ctrl+C to stop)")
try:
    for _ in range(20):
        event = {
            "user": random.choice(users),
            "action": random.choice(actions),
            "timestamp": time.time()
        }
        producer.send("user-activity", key=event["user"], value=event)
        print(f"Sent: {event}")
        time.sleep(0.5)
    producer.flush()
    print("✅ All messages sent!")
except KeyboardInterrupt:
    print("\n⏹️ Stopped")
finally:
    producer.close()
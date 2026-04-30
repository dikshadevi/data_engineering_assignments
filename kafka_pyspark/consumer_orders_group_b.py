import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    group_id="analytics-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None
)

print("Consumer 3 started in different group: analytics-group")

for message in consumer:
    order = message.value

    print("\n--- Analytics Consumer Received Order ---")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Order ID: {order['order_id']}")
    print(f"Product: {order['product']}")
    print(f"City: {order['city']}")
    print(f"Payment Mode: {order['payment_mode']}")
    print(f"Total Amount: {order['total_amount']}")
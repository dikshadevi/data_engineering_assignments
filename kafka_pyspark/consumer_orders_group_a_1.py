import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    group_id="order-processing-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None
)

print("Consumer 1 started in group: order-processing-group")

for message in consumer:
    order = message.value

    print("\n--- Consumer 1 Received Order ---")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")
    print(f"Order ID: {order['order_id']}")
    print(f"Customer: {order['customer_name']}")
    print(f"Product: {order['product']}")
    print(f"Amount: {order['total_amount']}")
    print(f"Status: {order['status']}")
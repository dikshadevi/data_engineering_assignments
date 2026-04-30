import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "order",
    bootstrap_servers="localhost:29092",
    group_id="high-value-orders-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("High Value Order Consumer started")

for message in consumer:
    order = message.value

    if order["total_amount"] >= 100000:
        print("\n--- High Value Order Found ---")
        print(f"Order ID: {order['order_id']}")
        print(f"Customer: {order['customer_name']}")
        print(f"Product: {order['product']}")
        print(f"Total Amount: {order['total_amount']}")
        print(f"City: {order['city']}")
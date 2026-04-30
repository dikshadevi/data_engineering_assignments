import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

products = [
    "Laptop",
    "Mobile",
    "Keyboard",
    "Mouse",
    "Headphones",
    "Monitor",
    "Tablet",
    "Charger"
]

statuses = [
    "PLACED",
    "CONFIRMED",
    "PACKED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED"
]

customers = [
    "Rahul",
    "Amit",
    "Priya",
    "Neha",
    "Sanjay",
    "Karan",
    "Rohit",
    "Anjali"
]


def generate_order(order_id):
    product = random.choice(products)
    quantity = random.randint(1, 5)
    price = random.randint(500, 80000)

    return {
        "order_id": order_id,
        "customer_name": random.choice(customers),
        "product": product,
        "quantity": quantity,
        "price": price,
        "total_amount": quantity * price,
        "status": random.choice(statuses),
        "payment_mode": random.choice(["UPI", "CARD", "COD", "NET_BANKING"]),
        "city": random.choice(["Delhi", "Mumbai", "Bangalore", "Pune", "Chandigarh"]),
        "created_at": datetime.now().isoformat()
    }


if __name__ == "__main__":
    total_orders = 5

    for order_id in range(1, total_orders + 1):
        order = generate_order(order_id)

        key = str(order["order_id"])

        producer.send(
            topic="orders",
            key=key,
            value=order
        )

        print(f"Produced Order: {order}")

        time.sleep(2)

    producer.flush()
    producer.close()
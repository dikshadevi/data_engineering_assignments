# Kafka + PySpark Docker Setup

This project contains a local **Kafka + PySpark** setup using Docker Compose. It is designed for learning and practicing data engineering concepts such as Kafka topics, producers, consumers, consumer groups, partitions, and Spark streaming.

## Repository

```bash
git clone https://github.com/dikshadevi/data_engineering_assignments.git
cd data_engineering_assignments/kafka_pyspark
```

## Project Structure

```text
kafka_pyspark/
│
├── docker-compose.yml
├── producer_orders.py
├── consumer_orders_group_a_1.py
├── consumer_orders_group_a_2.py
├── consumer_orders_group_b.py
├── consumer_high_value_orders.py
├── requirements.txt
├── commands
├── coreConcepts
└── notebooks/
```

## Services

The Docker Compose setup includes the following services:

| Service | Description | Port |
|---|---|---|
| Kafka | Kafka broker running in KRaft mode | `9092`, `29092` |
| Spark Master | Apache Spark master node | `7077`, `8080`, `4040` |
| Spark Worker | Apache Spark worker node | Internal |
| Kafka UI | Web UI to view Kafka topics, messages, brokers, and consumers | `8081` |

## Prerequisites

Before running the project, make sure you have installed:

- Docker
- Docker Compose
- Git
- Python 3.10 or above

## Start Docker Services

Run this command inside the `kafka_pyspark` folder:

```bash
docker-compose up -d
```

Check running containers:

```bash
docker ps
```

## Open Kafka UI

After starting the containers, open Kafka UI in your browser:

```text
http://localhost:8081
```

Kafka UI can be used to view:

- Kafka cluster
- Topics
- Partitions
- Messages
- Consumer groups

## Kafka Topic

This setup uses the topic:

```text
orders
```

## Create Kafka Topic

Enter the Kafka container:

```bash
docker exec -it kafka bash
```

Create the topic:

```bash
kafka-topics \
  --create \
  --topic orders \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

## List Kafka Topics

```bash
kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

## Describe Kafka Topic

```bash
kafka-topics \
  --describe \
  --topic orders \
  --bootstrap-server kafka:9092
```

## Run Python Producer and Consumers

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run the producer:

```bash
python producer_orders.py
```

Run consumers in separate terminals:

```bash
python consumer_orders_group_a_1.py
```

```bash
python consumer_orders_group_a_2.py
```

```bash
python consumer_orders_group_b.py
```

```bash
python consumer_high_value_orders.py
```

## Consumer Groups Used

This project demonstrates multiple Kafka consumer groups:

| Consumer Group | Purpose |
|---|---|
| `order-processing-group` | Processes order messages |
| `analytics-group` | Reads same order data for analytics |
| `high-value-orders-group` | Handles high-value order filtering |

Different consumer groups receive their own copy of the data from the Kafka topic.

## Check Consumer Group Details

Inside the Kafka container, run:

```bash
kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group order-processing-group
```

```bash
kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group analytics-group
```

```bash
kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group high-value-orders-group
```

## Run PySpark Streaming Job

Enter the Spark container:

```bash
docker exec -it spark bash
```

Run the Spark job:

```bash
/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/notebooks/spark_kafka_orders.py
```

The Spark job continuously reads new messages from the Kafka `orders` topic.

## Useful Kafka Console Commands

Create a console producer:

```bash
kafka-console-producer \
  --topic orders \
  --bootstrap-server localhost:9092
```

Create a console consumer:

```bash
kafka-console-consumer \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --group group-1 \
  --from-beginning
```

## Stop Services

To stop all running containers:

```bash
docker-compose down
```

To stop and remove volumes:

```bash
docker-compose down -v
```

## Notes

- Kafka UI runs on `http://localhost:8081`.
- Kafka has 3 partitions for the `orders` topic.
- Replication factor is `1` because this is a local single-broker setup.
- Spark reads Kafka messages using the Spark Kafka connector.
- This project is intended for local development and learning purposes.

## Author

Created for Data Engineering practice using Kafka, PySpark, and Docker.

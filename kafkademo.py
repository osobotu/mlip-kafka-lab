#!/usr/bin/env python3
"""
Kafka Demo
Producer: sends random city temperature data to Kafka
Consumer: reads data back from Kafka
"""

import os
from datetime import datetime
from json import dumps, loads
from time import sleep
from random import randint
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any

# Fill in your unique identifier so your topic doesn’t collide with others
andrew_id = "soduh"
topic = f"lab02-{andrew_id}"

# Fill in the address of your Kafka bootstrap server
bootstrap_servers = ["localhost:9092"]

print(f"Using topic: {topic}")


# Generate data
def make_city_data(city: str, temperature_f: int) -> Dict[str, Any]:
    """Create a JSON-compatible dictionary for city temperature data."""
    return {
        "city": city,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_f": temperature_f,
    }

# Add a few examples of city data
cities = [
    make_city_data("Pittsburgh", 64),
    make_city_data("Kigali", 85),
    make_city_data("New York", 70),
    make_city_data("San Francisco", 58),
    make_city_data("Lagos", 100),
]

# PRODUCER
def run_producer():
    """Send random city data to Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: dumps(v).encode("utf-8"),  # dict → JSON string → bytes
    )

    print("Writing to Kafka Broker...")
    for i in range(10):
        data = cities[randint(0, len(cities) - 1)]  # random selection
        producer.send(topic=topic, value=data)
        print(f"Sent: {data}")
        sleep(1)

    producer.flush()
    print(f"Data written to topic: {topic}")

# CONSUMER
def run_consumer():
    """Read messages from Kafka topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",  # Experiment with different values (earliest, latest, none)
        # Commit that an offset has been read
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        # How often to tell Kafka, an offset has been read
        value_deserializer=lambda v: loads(v.decode("utf-8")),
    )

    print("Reading from Kafka Broker...")
    for message in consumer:
        print(message.value)
        with open("kafka_log.csv", "a") as f:
            f.write(f"{message.value}\n")


if __name__ == "__main__":
    # Select mode: producer or consumer
    import argparse

    parser = argparse.ArgumentParser(description="Kafka Demo Script")
    parser.add_argument("mode", choices=["producer", "consumer"], help="Run as producer or consumer")
    args = parser.parse_args()

    if args.mode == "producer":
        run_producer()
    elif args.mode == "consumer":
        run_consumer()

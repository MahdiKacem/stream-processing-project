import argparse
import json
import random
from datetime import datetime
from uuid import uuid4
import time
import psycopg2
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

def connect_postgres(max_retries=10, wait_sec=3):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="postgres",
                database="postgres",
                user="postgres",
                password="postgres"
            )
            print("Connected to Postgres!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Postgres not ready (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(wait_sec)
    raise Exception("Could not connect to Postgres")

def generate_user_data(number_user_records: int) -> None:
    for id in range(number_user_records): 
        connection = connect_postgres()
        curr = connection.cursor()
        curr.execute(
            """INSERT INTO commerce.users (username, password) VALUES (%s, %s)""", (fake.user_name(), fake.password())
        )
        curr.execute(
                """INSERT INTO commerce.products (name, description, price) VALUES (%s, %s, %s)""",
                (fake.name(), fake.text(), fake.random_int(min=1, max=100))
        )
        connection.commit()
            
        #update 10 % of the time

        if random.randint(1, 100) >= 90:
            curr.execute(
                "UPDATE commerce.users SET username = %s WHERE id = %s",
                (fake.user_name(), id),
            )
            curr.execute(
                "UPDATE commerce.products SET name = %s WHERE id = %s",
                (fake.name(), id),
            )
        connection.commit()
        curr.close()
    return

def generate_click_event(user_id, product_id=None):
    click_id = str(uuid4())
    product_id = product_id or str(uuid4())
    product = fake.word()
    price = fake.pyfloat(left_digits=2, right_digits=2, positive=True)
    url = fake.url()
    user_agent = fake.user_agent()
    ip_address = fake.ipv4()
    datetime_occured = datetime.now()
    click_event = {
        "click_id": click_id,
        "user_id": user_id,
        "product_id": product_id,
        "product": product,
        "price": price,
        "url": url,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }

    return click_event

def generate_checkout_event(user_id, product_id):
    payment_method = fake.credit_card_provider()
    total_amount = fake.pyfloat(left_digits=3, right_digits=2, positive=True)
    shipping_address = fake.address()
    billing_address = fake.address()
    user_agent = fake.user_agent()
    ip_address = fake.ipv4()
    datetime_occured = datetime.now()

    checkout_event = {
        "checkout_id": str(uuid4()),
        "user_id": user_id,
        "product_id": product_id,
        "payment_method": payment_method,
        "total_amount": total_amount,
        "shipping_address": shipping_address,
        "billing_address": billing_address,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }

    return checkout_event

def connect_kafka(max_retries=10, wait_sec=3):
    for attempt in range(max_retries):
        try:
            producer = Producer({'bootstrap.servers': 'kafka:9092'})
            return producer
        except Exception as e:
            print(f"Kafka not ready (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(wait_sec)
    raise Exception("Could not connect to Kafka")

def push_to_kafka(event, topic):
    producer = connect_kafka()
    producer.produce(topic, json.dumps(event).encode('utf-8'))
    print("Event sent to Kafka")
    producer.flush()

def generate_clickstream_data(number_click_records: int) -> None:
    for _ in range(number_click_records):
        user_id = random.randint(1, 100)
        click_event = generate_click_event(user_id)
        push_to_kafka(click_event, 'clicks')

        # simulate multiple click and checkouts 50 % of the time

        while random.randint(1, 100) >= 50:
            click_event = generate_click_event(user_id, click_event['product_id'])
            checkout_event = generate_checkout_event(click_event['user_id'], click_event['product_id'])
            push_to_kafka(click_event, 'clicks')
            push_to_kafka(checkout_event, 'checkouts')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '-nu',
            '--num_user_records',
            type=int,
            help='Number of user records to generate',
            default=100
    )
    parser.add_argument(
        "-nc",
        "--num_click_records",
        type=int,
        help="Number of click records to generate",
        default=100000000,
    )
    args = parser.parse_args()
    generate_user_data(args.num_user_records)
    generate_clickstream_data(args.num_click_records)

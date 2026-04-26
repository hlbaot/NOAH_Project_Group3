import json
import logging
import os
import time
from decimal import Decimal

import mysql.connector
import pika
import psycopg2


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("order-worker")


def env_int(name, default):
    return int(os.getenv(name, default))


def env_float(name, default):
    return float(os.getenv(name, default))


RETRY_ATTEMPTS = env_int("CONNECT_RETRY_ATTEMPTS", "10")
RETRY_DELAY_SECONDS = env_float("CONNECT_RETRY_DELAY_SECONDS", "3")


def with_retry(name, factory):
    last_error = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return factory()
        except Exception as exc:
            last_error = exc
            logger.warning(
                "%s connection attempt %s/%s failed: %s",
                name,
                attempt,
                RETRY_ATTEMPTS,
                exc,
            )
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)

    raise RuntimeError(f"{name} is unavailable after retrying") from last_error


def mysql_connection():
    def connect():
        return mysql.connector.connect(
            host=os.getenv("DB_HOST", "mysql"),
            port=env_int("DB_PORT", "3306"),
            database=os.getenv("DB_NAME", "webstore"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", "rootpassword"),
            connection_timeout=5,
            autocommit=False,
        )

    return with_retry("MySQL", connect)


def postgres_connection():
    def connect():
        return psycopg2.connect(
            host=os.getenv("PG_HOST", "postgres"),
            port=env_int("PG_PORT", "5432"),
            dbname=os.getenv("PG_DB", "finance"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", "rootpassword"),
        )

    return with_retry("PostgreSQL", connect)


def order_queue_name():
    return os.getenv("RABBITMQ_QUEUE", "order_queue")


def rabbitmq_connection():
    def connect():
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "noah"),
            os.getenv("RABBITMQ_PASSWORD", "noahpassword"),
        )
        parameters = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=env_int("RABBITMQ_PORT", "5672"),
            credentials=credentials,
            heartbeat=30,
            blocked_connection_timeout=30,
        )
        return pika.BlockingConnection(parameters)

    return with_retry("RabbitMQ", connect)


def ensure_finance_table():
    conn = None
    cur = None
    try:
        conn = postgres_connection()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                order_id INT NOT NULL UNIQUE,
                customer_id INT NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'PAID',
                paid_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        logger.info("customers and payments tables are ready")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def upsert_customer(cur, customer_id):
    cur.execute(
        """
        INSERT INTO customers (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (int(customer_id), f"Customer_{int(customer_id)}"),
    )


def insert_finance_transaction(message):
    conn = None
    cur = None
    try:
        conn = postgres_connection()
        cur = conn.cursor()
        upsert_customer(cur, message["user_id"])
        cur.execute(
            """
            INSERT INTO payments
            (order_id, customer_id, amount, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE
            SET customer_id = EXCLUDED.customer_id,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status
            """,
            (
                int(message["order_id"]),
                int(message["user_id"]),
                Decimal(message["total_price"]),
                "PAID",
            ),
        )
        conn.commit()
        logger.info("Inserted order_id=%s into PostgreSQL payments", message["order_id"])
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def update_mysql_order_status(order_id, new_status="SYNCED"):
    conn = None
    cur = None
    try:
        conn = mysql_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE orders
            SET status = %s
            WHERE id = %s
            """,
            (new_status, int(order_id)),
        )
        conn.commit()
        logger.info("Updated MySQL order_id=%s to %s", order_id, new_status)
    finally:
        if cur:
            cur.close()
        if conn and conn.is_connected():
            conn.close()


def process_message(ch, method, properties, body):
    try:
        payload = json.loads(body.decode("utf-8"))
        logger.info("Received message: %s", payload)

        insert_finance_transaction(payload)
        update_mysql_order_status(payload["order_id"], "SYNCED")

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("ACK sent for order_id=%s", payload["order_id"])

    except Exception as exc:
        logger.exception("Failed to process message: %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    ensure_finance_table()

    connection = rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=order_queue_name(), durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=order_queue_name(), on_message_callback=process_message)

    logger.info("Worker is waiting for messages on queue=%s", order_queue_name())
    channel.start_consuming()


if __name__ == "__main__":
    main()

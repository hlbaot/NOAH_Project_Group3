import json
import logging
import os
import time
from decimal import Decimal

import mysql.connector
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("order-api")


def env_int(name, default):
    return int(os.getenv(name, default))


def env_float(name, default):
    return float(os.getenv(name, default))


RETRY_ATTEMPTS = env_int("CONNECT_RETRY_ATTEMPTS", "10")
RETRY_DELAY_SECONDS = env_float("CONNECT_RETRY_DELAY_SECONDS", "3")


class CreateOrderRequest(BaseModel):
    user_id: int = Field(..., ge=1)
    product_id: int = Field(..., ge=1)
    quantity: int = Field(..., ge=1)


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


def fetch_product_price(cursor, product_id):
    cursor.execute(
        """
        SELECT price
        FROM products
        WHERE id = %s
        """,
        (int(product_id),),
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Product not found")

    return Decimal(row[0])


def insert_order(cursor, payload, total_price):
    cursor.execute(
        """
        INSERT INTO orders (user_id, product_id, quantity, total_price, status)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            int(payload.user_id),
            int(payload.product_id),
            int(payload.quantity),
            total_price,
            "PENDING",
        ),
    )
    return int(cursor.lastrowid)


def publish_order_message(message):
    connection = None
    channel = None
    try:
        connection = rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=order_queue_name(), durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=order_queue_name(),
            body=json.dumps(message).encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Published order_id=%s to queue=%s", message["order_id"], order_queue_name())
    finally:
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()


app = FastAPI(title="NOAH Order API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/orders", status_code=201)
def create_order(payload: CreateOrderRequest):
    conn = None
    cursor = None
    try:
        conn = mysql_connection()
        cursor = conn.cursor()

        unit_price = fetch_product_price(cursor, payload.product_id)
        total_price = unit_price * payload.quantity
        order_id = insert_order(cursor, payload, total_price)
        conn.commit()

        message = {
            "order_id": order_id,
            "user_id": payload.user_id,
            "product_id": payload.product_id,
            "quantity": payload.quantity,
            "total_price": str(total_price),
        }
        publish_order_message(message)

        logger.info("Created order_id=%s with status=PENDING", order_id)
        return {
            "success": True,
            "message": "Order created and queued successfully",
            "order": {
                "order_id": order_id,
                "user_id": payload.user_id,
                "product_id": payload.product_id,
                "quantity": payload.quantity,
                "total_price": float(total_price),
                "status": "PENDING",
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to create order: %s", exc)
        raise HTTPException(status_code=503, detail="Unable to create order") from exc
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

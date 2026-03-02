import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

DB_CONF = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

db_conn = psycopg2.connect(**DB_CONF)
db_cursor = db_conn.cursor()

TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")


def ingest_quote_to_db(cursor, conn, quote):
    try:
        insert_query = """
            INSERT INTO stock_quotes (symbol, current_price, high_price, low_price, open_price, previous_close, market_time, fetch_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        m_time = quote["market_time"]
        if isinstance(m_time, int):
            m_time = datetime.fromtimestamp(m_time / 1000.0, tz=timezone.utc)

        f_time = quote["fetch_time"]
        if isinstance(f_time, int):
            f_time = datetime.fromtimestamp(f_time / 1000.0, tz=timezone.utc)

        cursor.execute(
            insert_query,
            (
                quote["symbol"],
                quote["current_price"],
                quote["high_price"],
                quote["low_price"],
                quote["open_price"],
                quote["previous_close"],
                m_time,
                f_time,
            ),
        )
        conn.commit()
        print(f"✅ Ingested quote for {quote['symbol']} into DB.")
    except Exception as e:
        conn.rollback()
        print("❌ Database error:", e)


def dict_to_quote(obj, ctx):
    return obj


with open("quote.avsc", "r") as f:
    schema_str = f.read()

schema_registry_conf = {"url": "http://schema-registry-stock:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(schema_registry_client, schema_str, dict_to_quote)

consumer_conf = {
    "bootstrap.servers": "kafka:29092",
    "group.id": "quotes-tracker",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(dict(consumer_conf))

consumer.subscribe(["stock-quotes"])

print("🟢 Consumer is running and subscribed to stock-quotes topic")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("❌ Error:", msg.error())
            continue

        quote = avro_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )
        ingest_quote_to_db(db_cursor, db_conn, quote)

except KeyboardInterrupt:
    print("\n🔴 Stopping Consumer.")

finally:
    db_cursor.close()
    db_conn.close()
    consumer.close()

import os, time
from dotenv import load_dotenv

load_dotenv()

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)


API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY:
    raise ValueError("API Key not found! Check your .env file.")

BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")


def fetch_stock_quote(symbol):
    import requests

    params = {"symbol": symbol, "token": API_KEY}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        quote = response.json()
        quote["symbol"] = symbol
        quote["fetch_time"] = int(time.time())
        return quote
    else:
        print(f"Error fetching quote for {symbol}: {response.status_code}")
        return None


producer_conf = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(dict(producer_conf))


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [Partition: {msg.partition()}]")


def quote_to_dict(quote, ctx):
    return {
        "symbol": quote["symbol"],
        "current_price": float(quote["c"]),
        "high_price": float(quote["h"]),
        "low_price": float(quote["l"]),
        "open_price": float(quote["o"]),
        "previous_close": float(quote["pc"]),
        "market_time": int(quote["t"] * 1000),
        "fetch_time": int(quote["fetch_time"] * 1000),
    }


with open("quote.avsc", "r") as f:
    schema_str = f.read()


schema_registry_conf = {"url": "http://localhost:8081"}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    quote_to_dict,
)

string_serializer = StringSerializer("utf-8")

try:
    while True:
        for symbol in SYMBOLS:
            quote = fetch_stock_quote(symbol)
            if quote:
                producer.produce(
                    topic=TOPIC,
                    key=string_serializer(
                        symbol, SerializationContext("quotes", MessageField.KEY)
                    ),
                    value=avro_serializer(
                        quote, SerializationContext("quotes", MessageField.VALUE)
                    ),
                    on_delivery=delivery_report,
                )
        producer.flush()
        time.sleep(10)
except KeyboardInterrupt:
    print("\n🔴 Stopping Producer.")
finally:
    producer.close()

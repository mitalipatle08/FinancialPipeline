import json
import time
import yfinance as yf
from kafka import KafkaProducer

# ---------------------------------------
# CONFIG
# ---------------------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC = "finance_stream"
TICKER = "AAPL"       #you can change to any stock
INTERVAL_SECONDS = 5  #fetch updates every 5 seconds

# ---------------------------------------
# HELPER: Serialize data to JSON
# ---------------------------------------
def serializer(data):
    return json.dumps(data).encode("utf-8")

# ---------------------------------------
# MAIN PRODUCER LOGIC
# ---------------------------------------
def stream_price():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=serializer
    )

    print(f"Streaming {TICKER} price updates to Kafka topic '{TOPIC}'...")

    while True:
        data = yf.Ticker(TICKER).history(period="1d").tail(1)

        if data.empty:
            print("No data received. Retrying...")
            time.sleep(INTERVAL_SECONDS)
            continue

        payload = {
            "ticker": TICKER,
            "timestamp": str(data.index[0]),
            "open": float(data["Open"][0]),
            "high": float(data["High"][0]),
            "low": float(data["Low"][0]),
            "close": float(data["Close"][0]),
            "volume": int(data["Volume"][0])
        }

        print("→ Sending:", payload)
        producer.send(TOPIC, payload)

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    stream_price()

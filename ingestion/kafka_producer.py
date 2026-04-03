from kafka import KafkaProducer
import json

from ingestion.binance_websocket import run_binance_socket

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def kafka_handler(data):
    # clean schema (important for downstream)
    event = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "timestamp": data["T"]
    }

    print("Sending:", event)

    future = producer.send("btc_trades", event)
    future.get(timeout=5)  # 👈 ensure delivery


if __name__ == "__main__":
    run_binance_socket(kafka_handler)
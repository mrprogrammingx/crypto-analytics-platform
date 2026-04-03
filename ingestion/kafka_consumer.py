from kafka import KafkaConsumer
from collections import defaultdict
import json

def main():

    consumer = KafkaConsumer(
        'btc_trades',
        bootstrap_servers='localhost:9092',
        group_id='btc_trades_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("📡 Listening for messages on 'btc_trades' topic...")
    bucket = defaultdict(list)
    for msg in consumer:
        trade = msg.value
        minute = trade['timestamp'] // 60000
        # bucket[minute].append(float(trade['price']))
        print("Bucket:", minute)
        print("Received:", trade)

if __name__ == "__main__":
    main()
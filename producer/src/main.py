import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce():
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'client.id': 'crypto-producer'
    }

    time.sleep(10)
    p = Producer(conf)

    symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD']
    
    try:
        while True:
            data = {
                "timestamp": datetime.utcnow().isoformat(),
                "symbol": random.choice(symbols),
                "price": round(random.uniform(20, 60000), 2),
                "volume": round(random.uniform(0.1, 5), 4)
            }
            
            p.produce('crypto_data', json.dumps(data).encode('utf-8'), callback=delivery_report)
            p.flush() 
            print(f"Produced: {data}")
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    produce()
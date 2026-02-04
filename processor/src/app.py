import asyncio
import json
import psycopg2
import os
import logging

from aiokafka import AIOKafkaConsumer 

logging.basicConfig(level=logging.INFO)

async def consume():
    await asyncio.sleep(5)
    
    conn = psycopg2.connect(
        host="timescaledb",
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()

    consumer = AIOKafkaConsumer(
        'crypto_data',
        bootstrap_servers='kafka:9092',
        group_id="processor-group",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    await consumer.start()
    logging.info("Processor started and listening...")

    try:
        async for msg in consumer:
            data = msg.value
            cur.execute(
                "INSERT INTO metrics (time, symbol, price, volume) VALUES (%s, %s, %s, %s)",
                (data['timestamp'], data['symbol'], data['price'], data['volume'])
            )
            conn.commit()
            logging.info(f"Stored: {data['symbol']}")
    finally:
        await consumer.stop()
        cur.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(consume())
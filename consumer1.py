import sys
import os
import json
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

KAFKA_TOPIC_NAME = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

if __name__ == "__main__":
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'stock_price_consumer',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC_NAME])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'End of partition reached {msg.topic()} [{msg.partition()}]')
                else:
                    print(f'Error while consuming message: {msg.error()}')
            else:
                value = msg.value().decode('utf-8')
                data = json.loads(value)  # Convert the string to a dictionary

                stock = data['stock']
                date = data['date']
                open_price = data['open']
                high = data['high']
                low = data['low']
                close = data['close']
                volume = data['volume']

                # Create or append to the CSV file
                csv_file_path = os.path.join('data', f'{stock}.csv')
                print(stock)
                os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

                with open(csv_file_path, 'a') as f:
                    if os.path.getsize(csv_file_path) == 0:
                        # Write header if the file is empty
                        f.write('Date,Open,High,Low,Close,Volume\n')

                    f.write(f'{date},{open_price},{high},{low},{close},{volume}\n')

                # print(f'Consumed message: {value}')

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
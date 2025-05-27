from kafka import KafkaProducer
import time
import os

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092']
    )
except Exception as e:
    print("Kafka not reachable:", e)
    exit(1)

# go from kafka/ to data/flights
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.abspath(os.path.join(script_dir, '..', 'data', 'flights'))

print(f"Using data directory: {data_dir}")

for file_name in os.listdir(data_dir):
    file_path = os.path.join(data_dir, file_name)
    print(f"Processing: {file_path}")
    try:
        with open(file_path, 'r') as f:
            for line in f:
                message = line.strip().encode('utf-8')
                try:
                    producer.send('flights', message)
                    print(f"Producer: {message}")
                except Exception as e:
                    print("Error sending event to Kafka:", e)
                time.sleep(0.2)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

producer.flush() 

import csv
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    kafka_connected = True
except Exception:
    producer = None
    kafka_connected = False

TOPIC_NAME = 'sensor_telemetry'

def run_simulation():
    csv_file_path = 'sensor_data.csv'
    
    print("\n" + "="*50)
    print("      ATMOSYNC PRODUCER: CONTAINER STREAM      ")
    print("="*50)
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            count = 0
            
            for row in reader:
                count += 1
                
                payload = {
                    "Container_ID": row.get("Container_ID", f"ATM{count:04d}"),
                    "Commodity": row.get("Commodity", "Avocado"),
                    "Temperature": round(float(row.get("AveragePrice", row.get("Temperature", random.uniform(2.0, 10.0)))), 2),
                    "Humidity": round(float(row.get("Humidity", random.uniform(50.0, 80.0))), 1),
                    "Vibration": int(row.get("Vibration", random.randint(1, 15))),
                    "Battery": int(row.get("Battery", random.randint(70, 100))),
                    "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Send to Kafka with explicit status tracking
                if producer and kafka_connected:
                    try:
                        producer.send(TOPIC_NAME, value=payload)
                        kafka_status = "SENT TO KAFKA"
                    except Exception:
                        kafka_status = "NOT SENT"
                else:
                    kafka_status = "NOT SENT"
                
                # Print individual card with Kafka Transmission Status
                print(f"+--------------------------------------------------+")
                print(f"| CONTAINER RECORD #{count:<30} |")
                print(f"+-------------------+------------------------------+")
                print(f"| Container ID      | {payload['Container_ID']:<28} |")
                print(f"| Temperature       | {payload['Temperature']:<25} Â°C |")
                print(f"| Humidity          | {payload['Humidity']:<25} %  |")
                print(f"| Vibration         | {payload['Vibration']:<28} |")
                print(f"| Battery           | {payload['Battery']:<25} %  |")
                print(f"+-------------------+------------------------------+")
                print(f"| Kafka Status      | {kafka_status:<28} |")
                print(f"+-------------------+------------------------------+\n")
                
                time.sleep(0.1)
                
            if producer:
                producer.flush()
            print(f"Completed streaming all {count} container records.")

    except FileNotFoundError:
        print(f"ERROR: Dataset file '{csv_file_path}' not found.")

if __name__ == "__main__":
    run_simulation()
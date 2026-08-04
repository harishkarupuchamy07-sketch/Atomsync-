import json
from kafka import KafkaConsumer

TEMP_HIGH_LIMIT = 5.0
BATTERY_LOW_LIMIT = 85
VIBRATION_HIGH_LIMIT = 10

# Flag to toggle real database connectivity later
IS_SNOWFLAKE_CONNECTED = False 

consumer = KafkaConsumer(
    'sensor_telemetry',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("\n" + "="*50)
print("      ATMOSYNC CONSUMER: SENSOR MONITOR        ")
print("="*50)

count = 0

for message in consumer:
    count += 1
    record = message.value
    
    container_id = record.get("Container_ID", "UNKNOWN")
    
    # Safe numerical conversions
    try:
        temp = float(record.get("Temperature", 0.0))
    except (ValueError, TypeError):
        temp = 0.5

    try:
        humidity = float(record.get("Humidity", 0.0))
    except (ValueError, TypeError):
        humidity = 0.0

    try:
        vibration = int(record.get("Vibration", 0))
    except (ValueError, TypeError):
        vibration = 0

    try:
        battery = int(record.get("Battery", 100))
    except (ValueError, TypeError):
        battery = 100

    # Collect active alerts
    alerts = []
    if temp > TEMP_HIGH_LIMIT:
        alerts.append(f"HIGH TEMP ({temp:.2f} °C > {TEMP_HIGH_LIMIT} °C)")
    if battery < BATTERY_LOW_LIMIT:
        alerts.append(f"LOW BATTERY ({battery}% < {BATTERY_LOW_LIMIT}%)")
    if vibration > VIBRATION_HIGH_LIMIT:
        alerts.append(f"HIGH VIBRATION ({vibration} > {VIBRATION_HIGH_LIMIT})")

    # Honest storage status based on actual connection flag
    storage_status = "STORED SUCCESSFUL" if IS_SNOWFLAKE_CONNECTED else "NOT CONNECTED"

    # Clean individual sensor card
    print(f"+--------------------------------------------------+")
    print(f"| RECEIVED SENSOR RECORD #{count:<24} |")
    print(f"+-------------------+------------------------------+")
    print(f"| Container ID      | {container_id:<28} |")
    print(f"| Temperature       | {temp:<25.2f} °C |")
    print(f"| Humidity          | {humidity:<25.1f} %  |")
    print(f"| Vibration         | {vibration:<28} |")
    print(f"| Battery           | {battery:<25} %  |")
    
    if alerts:
        print(f"+-------------------+------------------------------+")
        for alert in alerts:
            print(f"| ALERT             | {alert:<28} |")
            
    print(f"+-------------------+------------------------------+")
    print(f"| Snowflake Storage | {storage_status:<28} |")
    print(f"+-------------------+------------------------------+\n")

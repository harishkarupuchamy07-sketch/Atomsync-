import json
import os
from kafka import KafkaConsumer
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

TEMP_HIGH_LIMIT = 5.0
BATTERY_LOW_LIMIT = 85
VIBRATION_HIGH_LIMIT = 10

# ---------------------------------------------------------
# SNOWFLAKE CONFIGURATION & RSA AUTHENTICATION
# ---------------------------------------------------------
SNOWFLAKE_ACCOUNT = "BSCWHWM-NE88744"
SNOWFLAKE_USER = "KAFKA_USER"
SNOWFLAKE_ROLE = "KAFKA_CONNECTOR_ROLE"
SNOWFLAKE_DATABASE = "ATMOSYNC_DB"
SNOWFLAKE_SCHEMA = "TELEMETRY"
SNOWFLAKE_TABLE = "SENSOR_TELEMETRY"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"  # Added warehouse to resolve 000606 error
PRIVATE_KEY_PATH = "snowflake_key.p8"

IS_SNOWFLAKE_CONNECTED = False
sf_conn = None
sf_cursor = None

try:
    if os.path.exists(PRIVATE_KEY_PATH):
        with open(PRIVATE_KEY_PATH, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # Set passphrase if private key is password-protected
                backend=default_backend()
            )
        
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        sf_conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            account=SNOWFLAKE_ACCOUNT,
            private_key=pkb,
            role=SNOWFLAKE_ROLE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE
        )
        sf_cursor = sf_conn.cursor()
        IS_SNOWFLAKE_CONNECTED = True
        print("\n[SUCCESS] Connected to Snowflake via RSA Key Pair!", flush=True)
    else:
        print(f"\n[WARNING] Private key file '{PRIVATE_KEY_PATH}' not found. Snowflake insertion disabled.", flush=True)

except Exception as e:
    print(f"\n[ERROR] Failed to connect to Snowflake: {e}", flush=True)
    IS_SNOWFLAKE_CONNECTED = False


# ---------------------------------------------------------
# KAFKA CONSUMER SETUP
# ---------------------------------------------------------
consumer = KafkaConsumer(
    'sensor_telemetry',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("\n" + "="*50)
print("      ATMOSYNC CONSUMER: SENSOR MONITOR        ")
print("="*50, flush=True)

count = 0

# ---------------------------------------------------------
# STREAM PROCESSING LOOP
# ---------------------------------------------------------
for message in consumer:
    count += 1
    record = message.value
    
    container_id = record.get("Container_ID", "UNKNOWN")
    
    # Safe numerical conversions
    try:
        temp = float(record.get("Temperature", 0.0))
    except (ValueError, TypeError):
        temp = 0.0

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

    # Insert record into Snowflake
    if IS_SNOWFLAKE_CONNECTED and sf_cursor:
        try:
            json_str = json.dumps(record)
            metadata = json.dumps({"source": "kafka_consumer", "topic": "sensor_telemetry"})
            
            insert_query = f"""
                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
                (RECORD_CONTENT, RECORD_METADATA) 
                SELECT PARSE_JSON(%s), PARSE_JSON(%s);
            """
            
            sf_cursor.execute(insert_query, (json_str, metadata))
            storage_status = "STORED SUCCESSFUL"
        except Exception as err:
            storage_status = f"INSERT ERROR: {err}"
    else:
        storage_status = "NOT CONNECTED"

    # Print dashboard card output
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
    print(f"+-------------------+------------------------------+\n", flush=True)
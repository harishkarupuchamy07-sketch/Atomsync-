# AtmoSync – Micro Climate Arbitrage Analytics

## Project Overview

AtmoSync is a real-time data engineering project that monitors the environmental conditions of agricultural commodities during transportation using IoT sensor data. The project collects sensor readings, streams them through Apache Kafka, processes the data, stores it for further analysis, and visualizes insights using business intelligence dashboards.

---

## Project Pipeline

IoT Sensor Simulator (Python)
        ↓
Kafka Producer
        ↓
Apache Kafka Topic
        ↓
Kafka Consumer
        ↓
Snowflake Storage
        ↓
dbt Transformation
        ↓
Apache Superset Dashboard

---

## My Contribution

I worked on the first stage of the AtmoSync pipeline, which focuses on real-time IoT data ingestion and Kafka streaming.

### Work Done

- Developed the Python IoT Sensor Simulator (`sensor_simulator.py`).
- Read sensor data from the CSV dataset using `csv.DictReader`.
- Generated telemetry payloads containing:
  - Container ID
  - Commodity
  - Temperature
  - Humidity
  - Vibration
  - Battery
  - Timestamp
- Integrated Apache Kafka Producer to publish real-time sensor data to the `sensor_telemetry` topic.
- Developed a Kafka Consumer (`consumer.py`) to receive and validate sensor data.
- Implemented alert generation for High Temperature, Low Battery, and High Vibration.
- Configured Kafka and Zookeeper using Docker Compose.
- Tested the complete Producer–Consumer pipeline to verify real-time data streaming and validation.

---

## Output

- Successfully simulated real-time IoT sensor data.
- Streamed telemetry records to Apache Kafka.
- Validated incoming sensor data using the Kafka Consumer.
- Generated alerts based on predefined threshold values.
- Prepared the data ingestion module for the next stage of the project.

---

## Technologies Used

- Python
- Apache Kafka
- Docker
- Docker Compose
- CSV
- JSON
- Git
- GitHub

---

## Note

Snowflake, dbt, and Apache Superset are part of the complete AtmoSync project pipeline. My contribution was focused on the Python-based IoT Sensor Simulator, Kafka Producer, Kafka Consumer, Docker setup, and real-time data validation.

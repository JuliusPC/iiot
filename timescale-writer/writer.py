import os
import sys
import json
import time
from datetime import datetime, timezone
import psycopg2
import zenoh

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_NAME = os.getenv("DB_NAME", "telemetry")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "zenoh_password")
DB_PORT = os.getenv("DB_PORT", "5432")

ZENOH_CONNECT = os.getenv("ZENOH_CONNECT", "tcp/zenoh-router:7447")
ZENOH_SUBSCRIBE = "devices/**"

db_conn = None

def connect_db():
    global db_conn
    while True:
        try:
            db_conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            return
        except psycopg2.OperationalError:
            time.sleep(2)

def init_db():
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_telemetry (
                time TIMESTAMPTZ NOT NULL,
                vehicle_id VARCHAR(100) NOT NULL,
                payload JSONB NOT NULL
            );
        """)
        cur.execute("SELECT create_hypertable('raw_telemetry', 'time', if_not_exists => TRUE);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_telemetry (
                time TIMESTAMPTZ NOT NULL,
                vehicle_id VARCHAR(100) NOT NULL,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                temperature DOUBLE PRECISION,
                avg_temperature DOUBLE PRECISION,
                battery DOUBLE PRECISION,
                speed DOUBLE PRECISION,
                movement VARCHAR(50),
                is_anomaly BOOLEAN
            );
        """)
        cur.execute("SELECT create_hypertable('processed_telemetry', 'time', if_not_exists => TRUE);")
        db_conn.commit()

def handle_raw(vehicle_id, payload):
    global db_conn
    try:
        msg_time = payload.get("time")
        if not msg_time:
            msg_time = datetime.now(timezone.utc).isoformat()
            
        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw_telemetry (time, vehicle_id, payload) VALUES (%s, %s, %s)",
                (msg_time, vehicle_id, json.dumps(payload))
            )
            db_conn.commit()
    except Exception:
        db_conn.rollback()
        connect_db()

def handle_processed(vehicle_id, data):
    global db_conn
    try:
        msg_time = data.get("time")
        if not msg_time:
            msg_time = datetime.now(timezone.utc).isoformat()

        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_telemetry (
                    time, vehicle_id, latitude, longitude, temperature, 
                    avg_temperature, battery, speed, movement, is_anomaly
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    msg_time,
                    vehicle_id,
                    data.get("latitude"),
                    data.get("longitude"),
                    data.get("temperature"),
                    data.get("avg_temperature"),
                    data.get("battery"),
                    data.get("speed"),
                    data.get("movement"),
                    data.get("is_anomaly")
                )
            )
            db_conn.commit()
    except Exception:
        db_conn.rollback()
        connect_db()

def main():
    connect_db()
    init_db()

    conf = zenoh.Config.from_json5(f'{{"connect": {{"endpoints": ["{ZENOH_CONNECT}"]}}}}')
    with zenoh.open(conf) as session:
        with session.declare_subscriber(ZENOH_SUBSCRIBE) as subscriber:
            for sample in subscriber:
                key = str(sample.key_expr)
                payload_str = sample.payload.to_string()

                try:
                    data = json.loads(payload_str)
                except json.JSONDecodeError:
                    continue

                parts = key.split("/")
                if len(parts) >= 3 and parts[0] == "devices":
                    vehicle_id = parts[1]
                    topic = parts[2]
                else:
                    continue

                if topic == "raw":
                    handle_raw(vehicle_id, data)
                elif topic == "processed":
                    handle_processed(vehicle_id, data)

if __name__ == "__main__":
    main()
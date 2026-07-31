import os
import sys
import json
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple
import psycopg2
import zenoh

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TimescaleRepository:
    def __init__(self, host: str, dbname: str, user: str, password: str, port: str):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        self.conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        logging.info("Attempting to connect to TimescaleDB...")
        while True:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.dbname,
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
                logging.info("Successfully connected to TimescaleDB.")
                return
            except psycopg2.OperationalError as e:
                logging.warning(f"Database connection failed: {e}. Retrying in 2 seconds...")
                time.sleep(2)

    def init_database(self) -> None:
        if not self.conn or self.conn.closed != 0:
            self.connect()
        
        with self.conn.cursor() as cur:
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
            self.conn.commit()
        logging.info("Database schemas initialized.")

    def insert_raw(self, vehicle_id: str, payload: Dict[str, Any]) -> None:
        msg_time = payload.get("time")
        if not msg_time:
            msg_time = datetime.now(timezone.utc).isoformat()

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO raw_telemetry (time, vehicle_id, payload) VALUES (%s, %s, %s)",
                    (msg_time, vehicle_id, json.dumps(payload))
                )
                self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert raw payload, rolling back: {e}")
            self.conn.rollback()
            self.connect()

    def insert_processed(self, vehicle_id: str, data: Dict[str, Any]) -> None:
        msg_time = data.get("time")
        if not msg_time:
            msg_time = datetime.now(timezone.utc).isoformat()

        try:
            with self.conn.cursor() as cur:
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
                self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert processed data, rolling back: {e}")
            self.conn.rollback()
            self.connect()


class TelemetryRouter:
    @staticmethod
    def extract_route_metadata(key: str) -> Optional[Tuple[str, str]]:
        parts = key.split("/")
        if len(parts) >= 3 and parts[0] == "devices":
            return parts[1], parts[2]
        return None


class ZenohTelemetrySubscriber:
    def __init__(self, zenoh_connect: str, key_expr: str, repository: TimescaleRepository):
        self.zenoh_connect = zenoh_connect
        self.key_expr = key_expr
        self.repository = repository

    def start(self) -> None:
        self.repository.init_database()
        conf = zenoh.Config.from_json5(f'{{"connect": {{"endpoints": ["{self.zenoh_connect}"]}}}}')
        
        logging.info("Opening Zenoh session...")
        with zenoh.open(conf) as session:
            logging.info(f"Subscribing to key expression: {self.key_expr}")
            with session.declare_subscriber(self.key_expr) as subscriber:
                for sample in subscriber:
                    self._handle_sample(sample)

    def _handle_sample(self, sample: zenoh.Sample) -> None:
        key = str(sample.key_expr)
        payload_str = sample.payload.to_string()

        metadata = TelemetryRouter.extract_route_metadata(key)
        if not metadata:
            return

        vehicle_id, topic = metadata

        try:
            data = json.loads(payload_str)
        except json.JSONDecodeError:
            logging.warning(f"Discarding invalid JSON payload on topic {key}")
            return

        if topic == "raw":
            self.repository.insert_raw(vehicle_id, data)
        elif topic == "processed":
            self.repository.insert_processed(vehicle_id, data)


def main() -> None:
    db_host = os.getenv("DB_HOST", "timescaledb")
    db_name = os.getenv("DB_NAME", "telemetry")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "zenoh_password")
    db_port = os.getenv("DB_PORT", "5432")

    zenoh_connect = os.getenv("ZENOH_CONNECT", "tcp/zenoh-router:7447")
    zenoh_subscribe = os.getenv("ZENOH_SUBSCRIBE", "devices/**")

    repository = TimescaleRepository(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )

    subscriber = ZenohTelemetrySubscriber(
        zenoh_connect=zenoh_connect,
        key_expr=zenoh_subscribe,
        repository=repository
    )
    
    subscriber.start()

if __name__ == "__main__":
    main()
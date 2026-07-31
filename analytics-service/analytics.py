import os
import json
from collections import deque
import zenoh

ZENOH_CONNECT = os.getenv("ZENOH_CONNECT", "tcp/zenoh-router:7447")
INPUT_TOPIC = "devices/*/raw"
WINDOW_SIZE = 5
TEMP_THRESHOLD = 40.0

buffers = {}

def process_message(session, key, payload_str):
    parts = key.split("/")
    if len(parts) >= 3 and parts[0] == "devices" and parts[2] == "raw":
        device_id = parts[1]
    else:
        return

    try:
        data = json.loads(payload_str)
    except json.JSONDecodeError:
        return

    msg_time = data.get("time")
    temp = data.get("temperature")
    battery = data.get("battery")
    lat = data.get("latitude")
    lon = data.get("longitude")
    speed = data.get("speed")
    movement = data.get("movement", "UNKNOWN")

    if lat == 0.0 and lon == 0.0:
        lat = None
        lon = None

    if device_id not in buffers:
        buffers[device_id] = deque(maxlen=WINDOW_SIZE)

    if temp is not None:
        buffers[device_id].append(temp)

    valid_temps = [t for t in buffers[device_id] if t is not None]
    if valid_temps:
        avg_temp = sum(valid_temps) / len(valid_temps)
        is_anomaly = avg_temp > TEMP_THRESHOLD
    else:
        avg_temp = None
        is_anomaly = False

    processed = {
        "time": msg_time,
        "latitude": lat,
        "longitude": lon,
        "temperature": temp,
        "avg_temperature": avg_temp,
        "battery": battery,
        "speed": speed,
        "movement": movement,
        "is_anomaly": is_anomaly
    }

    output_key = f"devices/{device_id}/processed"
    session.put(output_key, json.dumps(processed))

def main():
    conf = zenoh.Config.from_json5(f'{{"connect": {{"endpoints": ["{ZENOH_CONNECT}"]}}}}')
    with zenoh.open(conf) as session:
        with session.declare_subscriber(INPUT_TOPIC) as subscriber:
            for sample in subscriber:
                key = str(sample.key_expr)
                payload_str = sample.payload.to_string()
                process_message(session, key, payload_str)

if __name__ == "__main__":
    main()
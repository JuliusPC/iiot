import os
import json
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import zenoh

TTN_APP_ID = os.getenv("TTN_APP_ID", "hsel-iiot-tracker@ttn")
TTN_API_KEY = os.getenv("TTN_API_KEY", "NNSXS.MP67DLNA3AJ7PG6WQPVO4APIFZDCECQCKC3RULY.DAGWFSXKUXVHIGLYAYOL6AKVO5YF7M5D4NXKEO5Y3Y6OCFRPBFBQ")
TTN_SERVER = os.getenv("TTN_SERVER", "eu1.cloud.thethings.network")
ZENOH_CONNECT = os.getenv("ZENOH_CONNECT", "tcp/zenoh-router:7447")

conf = zenoh.Config.from_json5(f'{{"connect": {{"endpoints": ["{ZENOH_CONNECT}"]}}}}')
session = zenoh.open(conf)

def on_message(client, userdata, msg):
    try:
        raw_payload = json.loads(msg.payload.decode("utf-8"))
        uplink = raw_payload.get("uplink_message", {})
        decoded = uplink.get("decoded_payload")
        
        if not decoded:
            return

        msg_time = decoded.get("timestamp")
        if not msg_time:
            msg_time = raw_payload.get("received_at")
        if not msg_time:
            msg_time = datetime.now(timezone.utc).isoformat()

        clean_data = {
            "time": msg_time,
            "temperature": decoded.get("temperature"),
            "battery": decoded.get("battery"),
            "speed": decoded.get("speed"),
            "latitude": decoded.get("latitude"),
            "longitude": decoded.get("longitude"),
            "movement": decoded.get("movement")
        }
        
        session.put("devices/0/raw", json.dumps(clean_data))
        
        metrics = {
            "devices/0/longitude": decoded.get("longitude"),
            "devices/0/latitude": decoded.get("latitude"),
            "devices/0/temperature": decoded.get("temperature"),
            "devices/0/battery": decoded.get("battery"),
            "devices/0/speed": decoded.get("speed"),
            "devices/0/movement": decoded.get("movement")
        }
        
        for topic, value in metrics.items():
            if value is not None:
                session.put(topic, str(value))
                
    except Exception as e:
        print(f"Error: {e}", flush=True)

def main():
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.username_pw_set(TTN_APP_ID, TTN_API_KEY)
    client.on_message = on_message
    client.connect(TTN_SERVER, 1883, 60)
    client.subscribe("v3/+/devices/+/up")
    client.loop_forever()

if __name__ == "__main__":
    main()
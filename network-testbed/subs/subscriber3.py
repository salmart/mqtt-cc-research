import paho.mqtt.client as mqtt
import sys
import time
import json

first_timestamp = None

def get_device_mac():
    mac_str = "89:73:74:74"
    return mac_str

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[SUBSCRIBER] Connected successfully.")
        device_mac = userdata["device_mac"]

        # Subscribe to your parameterized topic
        topic_for_qos = (
            f"{device_mac}/subscriber/"
            "tasks=ThermalCamera,Temperature;"
            "Max_Latency=202,190;"
            "Accuracy=99,99;"
            "Min_Frequency=.11,6;"
            "Reliability=98,98;"
        )
        print(f"[SUBSCRIBER] Subscribing to topic: {topic_for_qos}")
        client.subscribe(topic_for_qos, qos=1)
    else:
        print(f"[SUBSCRIBER] Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    global first_timestamp

    arrival_time = time.time()
    topic = msg.topic
    payload = msg.payload.decode("utf-8")

    print("\n[SUBSCRIBER] Received a message!")
    print(f"  Topic: {topic}")
    print(f"  Raw Payload: {payload}")

    # Handle subscription trigger: "MAC/TaskName"
    if "/" in payload and not payload.strip().startswith("{"):
        parts = payload.split("/", 1)
        if len(parts) == 2 and parts[1].strip():
            client.subscribe(payload)
            print(f"Just subscribed to the task name: '{payload}'")
        else:
            print("[WARNING] Task name after slash is empty or malformed.")
        return

    # Otherwise assume JSON sensor data
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print("[WARNING] Payload is not valid JSON, skipping JSON parsing.")
        return

    # Compute time delta
    if "timestamp" in data:
        current_ts = data["timestamp"]
        if first_timestamp is None:
            first_timestamp = current_ts
            delta = 0.0
            print(f"[INIT] Stored initial timestamp: {first_timestamp}")
        else:
            delta = current_ts - first_timestamp
            print(f"[Δ] Time since first message: {delta:.3f} seconds")
    else:
        if first_timestamp is None:
            first_timestamp = arrival_time
            delta = 0.0
        else:
            delta = arrival_time - first_timestamp
        print(f"[Δ] Time since first arrival: {delta:.3f} seconds")

    # Log power if present
    power_val = data.get("capacity")
    if power_val is not None:
        log = userdata["power_log"]
        log.write(f"{delta:.6f},{power_val}\n")
        log.flush()
        print(f"[POWER] Recorded power={power_val} at t={delta:.3f}s")
    else:
        print("[INFO] 'power' field not found in JSON, skipping power log.")

def main():
    device_mac = get_device_mac()
    print(f"[SUBSCRIBER] Device MAC is {device_mac}")

    # Open CSV for logging power vs time
    power_log = open("power_data_3.csv", "w")
    power_log.write("time_seconds,power\n")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.user_data_set({
        "device_mac": device_mac,
        "power_log": power_log
    })

    broker_host = "localhost"
    broker_port = 1883
    print(f"[SUBSCRIBER] Connecting to {broker_host}:{broker_port}...")
    client.connect(broker_host, broker_port, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[SUBSCRIBER] Interrupted by user, closing log file.")
    finally:
        power_log.close()
        print("[SUBSCRIBER] power_data_3.csv closed.")

if __name__ == "__main__":
    main()

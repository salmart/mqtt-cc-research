import paho.mqtt.client as mqtt
import sys
import time
import uuid
import re
import json

def get_device_mac():
    """
    Retrieves the local machine's MAC address and formats it like "AA:BB:CC:DD:EE:FF".
    Replace with your own method if needed.
    """
    mac_str = "89:33:44:44"
    return mac_str

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[SUBSCRIBER] Connected successfully.")
        device_mac = userdata["device_mac"]

        # Example: "AA:BB:CC:DD:EE:FF/subscriber/tasks=Motion,Humidity;Max_Latency=...etc..."
        topic_for_qos = (
            f"{device_mac}/subscriber/"
            "tasks=Motion,Humidity;"
            "Max_Latency=290,335;"
            "Accuracy=0.9,0.8;"
            "Min_Frequency=5,10;"
        )
        print(f"[SUBSCRIBER] Subscribing to topic: {topic_for_qos}")
        client.subscribe(topic_for_qos, qos=1)
    else:
        print(f"[SUBSCRIBER] Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """
    Called when a message is received on a subscribed topic.
    If the payload is something like '89:33:44:44/Temperature',
    we extract 'Temperature' and subscribe to it directly.
    """
    topic = msg.topic
    payload = msg.payload.decode("utf-8")

    print("\n[SUBSCRIBER] Received a message!")
    print(f"  Topic: {topic}")
    print(f"  Raw Payload: {payload}")

    # If the payload has a slash, assume it's MAC/TaskName
    if "/" in payload:
        parts = payload.split("/", 1)  # split once on the first slash
        if len(parts) == 2:
            mac_part, task_name = parts
            task_name = task_name.strip()
            
            if task_name:
                # Subscribe to just the task name
                client.subscribe(task_name)
                print(f"Just subscribed to the task name: '{task_name}'")
            else:
                print("[WARNING] Task name after slash is empty.")
        else:
            print("[WARNING] Payload format didn't match 'MAC/TaskName'.")
    else:
        print("No slash found in payload; skipping subscription to the task name.")
        data=json.loads(payload)
        init_time= data.get("initialtime")
        endtime=time.perf_counter()
        print(f"Time it took to travel: {endtime - init_time} seconds")

def main():
    device_mac = get_device_mac()
    print(f"[SUBSCRIBER] Device MAC is {device_mac}")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.user_data_set({"device_mac": device_mac})

    broker_host = "localhost"
    broker_port = 1883
    print(f"[SUBSCRIBER] Connecting to {broker_host}:{broker_port}...")
    client.connect(broker_host, broker_port, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()

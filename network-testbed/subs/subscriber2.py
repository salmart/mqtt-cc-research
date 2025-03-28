import paho.mqtt.client as mqtt
import sys
import time
import uuid
import re

def get_device_mac():
    """
    Retrieves the local machine's MAC address and formats it like "AA:BB:CC:DD:EE:FF".
    Replace with your own method if needed.
    """
    mac_str = "99:33:44:99"
    return mac_str

def on_connect(client, userdata, flags, rc):
    """
    Called when the subscriber connects to the MQTT broker.
    Subscribes to a topic that includes device_mac/subscriber plus
    the semicolon-delimited parameters for your C parser.
    """
    if rc == 0:
        print("[SUBSCRIBER] Connected successfully.")
        device_mac = userdata["device_mac"]

        topic_for_qos = f"{device_mac}/subscriber/tasks=Temperature;Max_Latency=900;Accuracy=4;Min_Frequency=5;"
        print(f"[SUBSCRIBER] Subscribing to topic: {topic_for_qos}")
        client.subscribe(topic_for_qos)
    else:
        print(f"[SUBSCRIBER] Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """
    Called when a message is received on a subscribed topic.
    If the payload looks like 'MAC/TaskName', we subscribe to just 'TaskName'.
    """
    topic = msg.topic
    payload = msg.payload.decode("utf-8")

    print("\n[SUBSCRIBER] Received a message!")
    print(f"  Topic: {topic}")
    print(f"  Raw Payload: {payload}")

    # Check for MAC/TaskName format
    if "/" in payload:
        parts = payload.split("/", 1)  # split once on the first slash
        if len(parts) == 2:
            mac_part, task_name = parts
            task_name = task_name.strip()
            if task_name:
                # Subscribe only to the task name
                client.subscribe(task_name)
                print(f"Just subscribed to the task name: '{task_name}'")
            else:
                print("[WARNING] Task name after slash is empty.")
        else:
            print("[WARNING] Payload format didn't match 'MAC/TaskName'.")
    else:
        print("No slash found in payload; skipping subscription to the task name.")

def main():
    """
    1) Get the local device MAC address.
    2) Create an MQTT subscriber client.
    3) Connect to broker & subscribe to "devicemac/subscriber/..." for your QoS parse.
    4) If the payload is 'MAC/TaskName', subscribe to just 'TaskName'.
    """
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

    # Loop forever to handle incoming messages
    client.loop_forever()

if __name__ == "__main__":
    main()

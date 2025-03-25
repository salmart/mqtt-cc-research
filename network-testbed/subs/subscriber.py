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
    mac_int = uuid.getnode()  # 48-bit MAC as an integer
    mac_str = ':'.join(re.findall('..', f'{mac_int:012x}')).upper()
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

        # 1. Build a topic that your C code can parse:
        #    Example:
        #      "AA:BB:CC:DD:EE:FF/subscriber/tasks=task1,task2;Max_Latency=10,15;Accuracy=0.9,0.8;Min_Frequency=5,10"
        #    If you want to subscribe to EXACTLY this topic:
        topic_for_qos = (
            f"{device_mac}/subscriber/"
            "tasks=Temperature,Humidity;"
            "Max_Latency=10,15;"
            "Accuracy=0.9,0.8;"
            "Min_Frequency=5,10"
        )

        print(f"[SUBSCRIBER] Subscribing to topic: {topic_for_qos}")
        client.subscribe(topic_for_qos, qos=1)

        # 2. (Optional) If you prefer using a wildcard so you can catch
        #    variations like tasks=..., tasks=taskA,taskB, etc.:
        # wildcard_topic = f"{device_mac}/subscriber/#"
        # print(f"[SUBSCRIBER] Subscribing to wildcard topic: {wildcard_topic}")
        # client.subscribe(wildcard_topic, qos=1)

    else:
        print(f"[SUBSCRIBER] Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """
    Called when a message is received on a subscribed topic.
    - Prints the full topic (so you can see what the C code would parse).
    - If the payload is "MAC_TASK", transform it to "MAC/TASK".
    """
    topic = msg.topic
    payload = msg.payload.decode("utf-8")

    print("\n[SUBSCRIBER] Received a message!")
    print(f"  Topic: {topic}")

    # 1. Show the raw payload
    print(f"  Raw Payload: {payload}")

    # 2. Example transformation from "MAC_TASK" -> "MAC/TASK"
    if "_" in payload:
        mac_part, task_part = payload.split("_", 1)
        transformed = f"{mac_part}/{task_part}"
        print(f"  Transformed Payload: {transformed}")
    else:
        print("  No transformation needed for payload.")

    # 3. Done. Your C code is primarily interested in parsing the TOPIC, not the payload.
    #    The topic you subscribed to includes semicolon-delimited key=value pairs (tasks=..., etc.)
    #    so your C plugin can run `get_qos_metrics()` on `msg.topic`.

def main():
    """
    1) Get the local device's MAC address.
    2) Create an MQTT subscriber client.
    3) Connect to broker & subscribe to "devicemac/subscriber/..." for your QoS parse.
    4) Transform any "MAC_TASK" payload to "MAC/TASK" for demonstration.
    """
    # 1) Retrieve local device MAC
    device_mac = get_device_mac()
    print(f"[SUBSCRIBER] Device MAC is {device_mac}")

    # 2) Create MQTT client
    client = mqtt.Client()

    # 3) Assign callbacks & store device_mac in user_data
    client.on_connect = on_connect
    client.on_message = on_message
    client.user_data_set({"device_mac": device_mac})

    # (Optional) Set username/password if the broker requires auth
    # client.username_pw_set(username="YOUR_USERNAME", password="YOUR_PASSWORD")

    # 4) Connect to your broker
    broker_host = "localhost"  # Replace with your broker's IP/domain
    broker_port = 1883
    print(f"[SUBSCRIBER] Connecting to {broker_host}:{broker_port}...")
    client.connect(broker_host, broker_port, keepalive=60)

    # 5) Loop forever to handle incoming messages
    client.loop_forever()

if __name__ == "__main__":
    main()

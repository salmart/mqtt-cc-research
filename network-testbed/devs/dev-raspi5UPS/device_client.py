import paho.mqtt.client as mqtt
import time
import json
import threading
import sensing as s # Import the sensor data module


# MQTT Broker Configuration
BROKER = "141.215.217.6"
# PORT = 1883


# Predefined sensing topics for the publisher
SENSING_TOPICS = ["Temperature", "Humidity"]


# Shared state between subscriber and publisher
publisher_active_tasks = set()  # Topics that should publish indefinitely


### ================== PUBLISHER CLIENT ================== ###
def on_connect_publisher(client, userdata, flags, rc):
    """Callback when the publisher connects to the broker."""
    print(f"[PUBLISHER] Connected with result code {rc}")
   
    # Step 1: Publish once to each sensing topic
    for task in SENSING_TOPICS:
        topic = f"{s.device_mac}/{task}"
        message = s.get_sensor_data()  # Fetch sensor data
        client.publish(topic, message)
        print(f"[PUBLISHER] Initial Publish: '{message}' to {topic}")


def publisher_loop():
    """Continuously publishes messages for active tasks."""
    while True:
        for topic in list(publisher_active_tasks):
            message = sensor_data.get_sensor_data(topic)  # Get sensor data
            client_pub.publish(topic, message)
            print(f"[PUBLISHER] Published '{message}' to {topic}")
        time.sleep(1)  # Publish every 1 second


def start_publisher():
    """Starts the MQTT publisher client."""
    global client_pub
    client_pub = mqtt.Client()
    client_pub.on_connect = on_connect_publisher
    client_pub.connect(BROKER, 1883, 60)
    client_pub.loop_start()  # Runs in the background


    # Start the publisher's background loop
    threading.Thread(target=publisher_loop, daemon=True).start()


### ================== SUBSCRIBER CLIENT ================== ###
def on_connect_subscriber(client, userdata, flags, rc):
    """Callback when the subscriber connects to the broker."""
    print(f"[SUBSCRIBER] Connected with result code {rc}")
    topic = f"{s.device_mac}/subscriber"
    client.subscribe(topic)
    print(f"[SUBSCRIBER] Subscribed to {topic}")


def on_message_subscriber(client, userdata, msg):
    """Processes incoming messages and instructs the publisher to publish continuously."""
    global publisher_active_tasks


    print(f"[SUBSCRIBER] Received message: {msg.payload.decode()}")


    try:
        data = json.loads(msg.payload.decode())
        if "task" in data:
            task = data["task"]
            topic = f"{PUBLISHER_DEVICE_MAC}/{task}"
           
            if topic in SENSING_TOPICS:
                publisher_active_tasks.add(topic)  # Add to active publishing tasks
                print(f"[SUBSCRIBER] Publisher will now publish to '{topic}' indefinitely.")
            else:
                print(f"[SUBSCRIBER] Error: Task '{task}' is not a known sensing topic.")
   
    except json.JSONDecodeError:
        print("[SUBSCRIBER] Error: Received invalid JSON payload")


def start_subscriber():
    """Starts the MQTT subscriber client without blocking execution."""
    client_sub = mqtt.Client()
    client_sub.on_connect = on_connect_subscriber
    client_sub.on_message = on_message_subscriber
    client_sub.connect(BROKER, 1883, 60)
    client_sub.loop_start()  # Runs in the background


### ================== MAIN ================== ###
if __name__ == "__main__":
    start_publisher()  # Start publisher in background
    start_subscriber()  # Start subscriber in background


    while True:
        time.sleep(1)  # Keep main thread alive




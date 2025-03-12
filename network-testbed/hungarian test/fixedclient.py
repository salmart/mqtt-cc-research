import dbutil as db
import paho.mqtt.client as mqtt

def on_connect_publisher(client, userdata, flags, rc):
    """Publishes commands to MQTT topics upon connecting."""
    if rc == 0:
        print("[PUBLISHER] Connected to MQTT Broker")
        db.main()
        command = db.rel[0] + db.rel[1]  # Example message

        for mac, tasks in db.sub_dict.items():  # Corrected looping
            for task in tasks:  # Handle multiple tasks per subscriber
                topic = f"{mac}/{task}"  # Correct topic format
                client.publish(topic, command)
                print(f"[PUBLISHER] Published '{command}' to {topic}")
    else:
        print(f"[PUBLISHER] Connection failed with code {rc}")

if __name__ == "__main__":
    client_pub = mqtt.Client()
    client_pub.on_connect = on_connect_publisher
    client_pub.connect("localhost", 1883, 60)  # Corrected port type

    client_pub.loop_forever()  # Keeps client running

import paho.mqtt.client as mqtt
import json  # For JSON serialization
import dbutil as db

def on_connect_publisher(client, userdata, flags, rc):
    """
    Called when the publisher connects to the MQTT broker.
    1) Publishes commands to the relevant subscribers based on the Hungarian assignment.
    2) Publishes a second message over publisher_mac/publisher for each chosen publisher.
    """
    if rc == 0:
        print("[PUBLISHER] Connected to MQTT Broker")

        # 1) rel() now returns 2 dicts:
        #    final_assignments (subscriber -> [(publisher, task), ...])
        #    publisher_assignments (publisher -> [task1, task2, ...])
        final_assignments, publisher_assignments = db.rel()

        # 2) Publish to subscribers on "sub_mac/subscriber"
        #    The payload is now a JSON object, e.g. {"task": "Temperature"}
        for sub_mac, pub_task_list in final_assignments.items():
            for (publisher_mac, task) in pub_task_list:
                # Create a JSON payload of the form: {"task": "<task>"}
                command_payload = json.dumps({"task": task})

                topic = f"{sub_mac}/publisher/tasks=Temperature,Humidity;Min_Frequency=5,10;Max_Latency=190,185;Accuracy=0.9,0.8;Energy=3.0,1.3;"
                client.publish(topic, command_payload)
                print(f"[PUBLISHER] Published JSON '{command_payload}' to '{topic}'")

        # 3) Publish a second message over "publisher_mac/publisher"
        #    to notify each chosen publisher about its tasks
        for publisher_mac, tasks_list in publisher_assignments.items():
            # Example payload: "task1,task2,task3"
            tasks_str = ",".join(tasks_list)
            pub_topic = f"{publisher_mac}/publisher/*"
            pub_payload = f"AssignedTasks={tasks_str}"
            client.publish("s"+pub_topic, pub_payload)#sala
            print(f"[PUBLISHER] Published '{pub_payload}' to '{pub_topic}'")

    else:
        print(f"[PUBLISHER] Connection failed with code {rc}")

if __name__ == "__main__":
    # Create MQTT client
    client_pub = mqtt.Client()

    # Set the on_connect callback
    client_pub.on_connect = on_connect_publisher

    # Connect to the broker
    client_pub.connect("localhost", 1883, 60)

    # Start the loop
    client_pub.loop_forever()

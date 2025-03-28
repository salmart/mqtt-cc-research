import paho.mqtt.client as mqtt
import json  # For JSON serialization
import dbutil as db
import time
def on_connect_publisher(client, userdata, flags, rc):
    """
    Called when the publisher connects to the MQTT broker.
    1) Publishes commands to the relevant subscribers based on the Hungarian assignment.
    2) Publishes a second message over publisher_mac/publisher for each chosen publisher.
    """
    if rc == 0:
        print("[PUBLISHER] Connected to MQTT Broker")

        time.sleep(1)# adjust this based on how long it takes for the database to populate with your experiment setup

        # IDEA: put a while(this key is not pressed) wait for key
        
        # Experiment Setup Conditions: 
            # All subscribing topics in table
            # All devices subscribed on their command line topic
            # All devices are NOT publishing, they are waiting for a CMD message
            
        # 1) rel() now returns 2 dicts:
        #    final_assignments (subscriber -> [(publisher, task), ...])
        #    publisher_assignments (publisher -> [task1, task2, ...])
        final_assignments, publisher_assignments = db.rel()

        # Rename loop variable for clarity
        for sub_topic, pub_task_list in final_assignments.items():
            for (publisher_mac, task) in pub_task_list:
                command_payload = f"{publisher_mac}/{task}"

                # Use sub_topic directly, don't add a slash
                topic = sub_topic
                
                client.publish(topic, command_payload)
                print(f"[PUBLISHER] Published '{command_payload}' to '{topic}'")


        # 3) Publish a second message over "publisher_mac/publisher"    
        #    to notify each chosen publisher about its tasks
        for publisher_topic, tasks_list in publisher_assignments.items():
            tasks_str = ",".join(tasks_list)
            payload = f"AssignedTasks={tasks_str}"

            client.publish(publisher_topic, payload)
            print(f"[PUBLISHER] Published '{payload}' to '{publisher_topic}'")

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

import dbutil as db
import paho.mqtt.client as mqtt


def on_connect_publisher(client,userdata,flags,rc):
    command = db.relevantmac1 + db.relevanttask1
    for i in db.sub_dict:
        mac = list(db.sub_dict.keys())[i]
        task = list(db.sub_dict.values())[i]
        topic = "/".join(mac+task)
        client.publish(topic,command)

if __name__ == "__main__":
    client_pub = mqtt.Client()
    client_pub.on_connect = on_connect_publisher
    client_pub.connect("localhost", "1883", 60)


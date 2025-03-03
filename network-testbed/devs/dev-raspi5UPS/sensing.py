import time
import seeed_dht
import paho.mqtt.client as mqtt
import struct
import smbus2
import subprocess as call
import json
import uuid

MQTT_BROKER = "141.215.217.6"  
MQTT_PORT = 1883
MQTT_TOPIC = "sensor/dht11"
last_publish_time = time.time()

sensor = seeed_dht.DHT("11", 12)

client = mqtt.Client()

client.connect(MQTT_BROKER, MQTT_PORT, 60)

def publish_sensor_data():
    while True:
        humi, temp = sensor.read()
        voltage = readVoltage(bus)
        capacity = readCapacity(bus)
        publish_interval_seconds = current_time - last_publish_time
        publish_frequency_hz = 1 / publish_interval_seconds
        if humi is not None and temp is not None and voltage is not None and capacity is not None:
            message = {
                "mac_address": device_mac,
                "temperature": round (temp,1),
                "humidity" : round (humi,1),
                "voltage" : round(voltage, 2),
                "capacity" : round (capacity,2),
                "publish_frequency_hz" : round (publish_frequency_hz, 2),
                "accuracy" : {
                    "+-": 2,
                    "-+": 5
                },
                "tasks" : {
                    "Task 1": "Humidity",
                    "Task 2": "Temperature"
                }
            }
            message_str = json.dumps(message)
            client.publish(MQTT_TOPIC, message_str)
            last_publish_time = time.time()
            print(f"Published: {message_str}")
        else:
            print("Failed to read sensor data.")

        time.sleep(2)  

def readVoltage(bus):

     address = 0x36
     read = bus.read_word_data(address, 2)
     swapped = struct.unpack("<H", struct.pack(">H", read))[0]
     voltage = swapped * 1.25 /1000/16
     return voltage


def readCapacity(bus):

     address = 0x36
     read = bus.read_word_data(address, 4)
     swapped = struct.unpack("<H", struct.pack(">H", read))[0]
     capacity = swapped/256
     return capacity

def get_mac_address(): 
    mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) for elements in range(0, 2 * 6, 2)][::-1]) 
    return mac

device_mac = get_mac_address()

bus = smbus2.SMBus(1)

if __name__ == '__main__':
    device_mac = get_mac_address()
    try:
        publish_sensor_data()
    except KeyboardInterrupt:
        print("\nDisconnected from MQTT broker")
        client.disconnect()

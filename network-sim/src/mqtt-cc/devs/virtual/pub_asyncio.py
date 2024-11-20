import asyncio
import socket
import sys
import paho.mqtt.client as mqtt
import pub_utils
import psutil
import json
from datetime import datetime

utils = pub_utils.PublisherUtils()

class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write
        self.start_up = True

    def on_socket_open(self, client, userdata, sock):

        def cb():
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):

        def cb():
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break

class AsyncMqtt:
    def __init__(self, loop):
        self.loop = loop
        self.tasks = set()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 5:
            sys.exit()
        client.subscribe(utils._CMD_TOPIC, qos=1)
        print("This on_connect publisher function gets called sala!!")

    async def waitForCmd(self):
        cmd = await self.got_message
        return cmd

    def on_message(self, client, userdata, msg):
        if mqtt.topic_matches_sub(msg.topic, utils._CMD_TOPIC):
            print(f"{utils._deviceMac} received command: {msg.payload.decode()} this is the topic the devices is subscribed to: {msg.topic}")
            self.got_message.set_result(msg.payload.decode())

    def on_disconnect(self, client, userdata, rc):
        self.disconnected.set_result(rc)

    async def waitForStatus(self):
        while True:
            print("starting window")

            await asyncio.sleep(utils._timeWindow)

            print("end window, sending status now")

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Get the Battery Information
            if utils._IN_SIM:
                if not utils.decreaseSimEnergy():
                    print("battery is 0, no longer executing")
                    sys.exit()
            else:
                utils.getExperimentEnergy()

            status_json = {
                "time": current_time,
                "deviceMac": utils._deviceMac,
                "battery": utils._battery,
                "cpu_temperature": "None",
                "cpu_utilization_percentage": "None",
                "memory_utilization_percentage": "None"
            }

            status_str = json.dumps(status_json)
            print("status =")
            print(status_str)
            # publish status to status topic
            self.client.publish(topic=utils._STATUS_TOPIC, payload=status_str, qos=1)
            print(f"{utils._deviceMac} publishing status")

    async def publish_to_topic(self, sense_topic, freq):
        msg = "1" * 500000
        while True:
            self.client.publish(topic=sense_topic, payload=msg, qos=1)
            await asyncio.sleep(freq)
            print(f"{utils._deviceMac} publishing on {sense_topic}")

    async def separateExecutionsAndAssignments(self, command: str):
        # find the comma
        index = command.rfind(',')
        assignments = command[:index].strip()
        consumption = command[index + 1:].strip()
        print(f"{utils._deviceMac} assignments {assignments}")
        print(f"consuming {consumption} every minute")
        print("=================")
        return assignments, consumption

    async def main(self):
        # main execution
        self.disconnected = self.loop.create_future()

        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect  # this just assigns the reference to the function
        self.client.on_message = self.on_message  # this just assigns the reference to the function
        self.client.on_disconnect = self.on_disconnect  # this just assigns the reference to the function
        self.got_message = None

        aioh = AsyncioHelper(self.loop, self.client)
        self.client.connect("localhost", 1885, keepalive=1000)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        self.got_message = self.loop.create_future()

        if not utils._publishes:
            print("We are waiting to get something to publish sala")
            cmd = await self.got_message  # wait for command to come
            print(f"This is the command the publisher is getting: {cmd}")

            if utils._IN_SIM:
                cmd, consumption = await self.separateExecutionsAndAssignments(cmd)
                utils.saveConsumption(energy=consumption)

            # Convert the command to JSON format
            try:
                cmd_list = json.loads(cmd)
                print(f"Parsed cmd_list: {cmd_list}")
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                return

            # Process the valid tuples
            topics = []
            for item in cmd_list:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    topic, value = item
                    if isinstance(topic, str) and isinstance(value, (int, float)):
                        topics.append({'topic': topic, 'value': value})
                    else:
                        print(f"Invalid item format in cmd_list: {item}")
                else:
                    print(f"Invalid item in cmd_list: {item}")

            cmd_dict = {'topics': topics}
            cmd_json = json.dumps(cmd_dict)
            print(f"cmd_json: {cmd_json}")

            utils.setPublishing(json.loads(cmd_json))

            if utils._publishes:
                routines = [self.publish_to_topic(topic['topic'], topic['value']) for topic in utils._publishes.get('topics', [])]
            else:
                routines = []

            self.got_message = self.loop.create_future()

            for coro in routines:
                self.tasks.add(asyncio.create_task(coro))

            self.tasks.add(asyncio.create_task(self.waitForCmd()))
            self.tasks.add(asyncio.create_task(self.waitForStatus()))

        while True:
            try:
                print("running tasks")
                done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)

                result = done.pop().result()
                print(f"{utils._deviceMac} canceling other tasks")

                for unfinished_task in pending:
                    unfinished_task.cancel()
                self.tasks = set()

                if utils._IN_SIM:
                    result, consumption = await self.separateExecutionsAndAssignments(result)
                    utils.saveConsumption(energy=consumption)

                # Convert the command result to JSON format
                try:
                    result_list = json.loads(result)
                    print(f"Parsed result_list: {result_list}")
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    return

                # Process the valid tuples
                topics = []
                for item in result_list:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        topic, value = item
                        if isinstance(topic, str) and isinstance(value, (int, float)):
                            topics.append({'topic': topic, 'value': value})
                        else:
                            print(f"Invalid item format in result_list: {item}")
                    else:
                        print(f"Invalid item in result_list: {item}")

                result_dict = {'topics': topics}
                result_json = json.dumps(result_dict)
                print(f"result_json: {result_json}")

                utils.setPublishing(json.loads(result_json))

                if utils._publishes:
                    routines = [self.publish_to_topic(topic['topic'], topic['value']) for topic in utils._publishes.get('topics', [])]
                else:
                    routines = []

                for coro in routines:
                    self.tasks.add(asyncio.create_task(coro))

                self.tasks.add(asyncio.create_task(self.waitForCmd()))
                self.tasks.add(asyncio.create_task(self.waitForStatus()))

                self.got_message = self.loop.create_future()
            except asyncio.CancelledError:
                print("asyncio cancelled")

def run_async_publisher():
    print(f"{utils._deviceMac} Starting Publisher Context")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(AsyncMqtt(loop).main())
    loop.close()
    print(f"{utils._deviceMac} Finished")

if __name__ == "__main__":
    run_async_publisher()
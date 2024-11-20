from algo_utils import Processing_Unit, Devices
import json
from proto_utils import ProtoUtils
from proto_db import Database

def resetPublishingsAndDeviceExecutions():
    db = Database()
    db.openDB()
    db.resetPublishings()
    db.resetDeviceExecutions()
    db.resetDeviceConsumptions()
    db.closeDB()
#sala chalked code
def generateAssignments(changedTopic=None, subLeft=None):
    db = Database()
    db.openDB()
    bestMacNumTopics = 0
    
    if changedTopic:
        db.resetDevicesPublishingToTopic(changedTopic)
    elif subLeft:
        db.resetAllDevicesPublishing()

    publishers = Devices()

    bestMac = None
    Emin = None
    Einc = None
    Enew = None
    Eratio = None
    
    topicsWithNoPublishers = db.topicsWithNoPublishers()  # list of tuples with (topic, max_allowed_latency)

    # Dictionary to hold all subscriptions and their respective MAC addresses
    subscription_latency_dict = {}

    # Get all subscriptions with their max allowed latency
    all_subscriptions_query = '''SELECT subscription, max_allowed_latency FROM subscriptions'''
    all_subscriptions_results = db.execute_query_with_retry(query=all_subscriptions_query)

    # Get all devices with their publishing topics
    all_devices_query = '''SELECT deviceMac, topic FROM publish'''
    all_devices_results = db.execute_query_with_retry(query=all_devices_query)

    # Create a dictionary to map subscriptions to their max_allowed_latency
    subscription_latency_map = {row[0]: row[1] for row in all_subscriptions_results}

    # Combine deviceMac with their publishing subscriptions and latency
    for device in all_devices_results:
        deviceMac = device[0]
        topic = device[1]

        if topic in subscription_latency_map:
            latency = subscription_latency_map[topic]

            if deviceMac not in subscription_latency_dict:
                subscription_latency_dict[deviceMac] = []

            # Append (subscription, latency) tuple
            subscription_latency_dict[deviceMac].append((topic, latency))

    # For each topic with no publisher
    for task in topicsWithNoPublishers:
        topic = task[0]
        freq = task[1]

        # Get devices capable of publishing to the topic
        capableDevices = db.devicesCapableToPublish(topicName=topic)  # list of tuples with (deviceMac, battery, executions, consumption)

        for device in capableDevices:
            mac = device[0]
            battery = device[1]
            num_exec = device[2]
            consumption = device[3]

            # Get device publishing info with query
            devicePublishings = db.devicePublishing(MAC_ADDR=mac)  # list of tuples with (topic, max_allowed_latency)

            if mac not in publishers._units:
                publishers.addProcessingUnit(Processing_Unit(macAddr=mac, capacity=battery, executions=num_exec, consumption=consumption))

            if devicePublishings:
                publishers._units[mac].addPublishings(devicePublishings)
                if changedTopic and topic == changedTopic:
                    publishers._units[mac].resetExecutions()

            Einc = publishers._units[mac].energyIncrease(freq)
            Enew = publishers._units[mac].currentEnergy() + Einc
            Eratio = Enew / publishers._units[mac]._battery

            if bestMac is None:
                bestMac = mac
                Emin = Eratio
                bestMacNumTopics = len(publishers._units[bestMac]._assignments)
            elif Enew <= publishers._units[mac]._battery and Eratio < Emin and len(publishers._units[mac]._assignments) < bestMacNumTopics:
                bestMac = mac
                Emin = Eratio
                bestMacNumTopics = len(publishers._units[bestMac]._assignments)

        if bestMac is not None:
            publishers._units[bestMac].addAssignment(topic, freq)
            publishers._units[bestMac].resetExecutions()

            newConsumption = Emin * publishers._units[bestMac]._battery
            publishers._units[bestMac].updateConsumption(newConsumption)

            db.updateDeviceExecutions(MAC_ADDR=bestMac, NEW_EXECUTIONS=publishers._units[bestMac]._numExecutions)
            db.updateDeviceConsumptions(MAC_ADDR=bestMac, NEW_CONSUMPTIONS=publishers._units[bestMac]._consumption)

        bestMac = None
        Emin = None
        Einc = None
        Enew = None
        Eratio = None
        bestMacNumTopics = 0

    for macAddress, device in publishers._units.items():
        if not device._assignments:
            device._assignments = {"None": "None"}
        assignmentString = json.dumps(device._assignments)
        publishers.addAssignmentsToCommand(deviceMac=macAddress, taskList=assignmentString)
        db.updatePublishTableWithPublishingAssignments(MAC_ADDR=macAddress, TOPICS=device._assignments.keys())

    db.closeDB()
    publishers.resetUnits()
    print(f"GENERATED FINAL COMMAND SALA = {publishers._generated_cmd}")

    return subscription_latency_dict  # Returns the dictionary with MAC address and tuples of (subscription, latency)

def getPublisherExecutions():
    db = Database()
    db.openDB()
    rows = db.getAllDeviceExecutions() # list of tuples (deviceMac, executions)
    db.closeDB()       
    return rows 

def getPublisherConsumptions():
    db = Database()
    db.openDB()
    rows = db.getAllDeviceConsumptions()
    db.closeDB()
    return rows



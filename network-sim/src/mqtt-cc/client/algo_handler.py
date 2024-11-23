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
def generateAssignments():
    db = Database()
    db.openDB()

    # Step 1: Retrieve all subscriptions and their latency requirements
    subscriptions_query = '''
        SELECT DISTINCT subscription, max_allowed_latency
        FROM subscriptions
        '''
    subscriptions = db.execute_query_with_retry(query=subscriptions_query)
    # subscriptions example: [("sensor/temperature", 200), ("sensor/humidity", 300)]

    # Step 2: Retrieve all devices already publishing and their topics
    devices_query = '''
        SELECT deviceMac, topic FROM publish
        '''
    devices_publishing = db.execute_query_with_retry(query=devices_query)
    # devices_publishing example: [("00:11:22:33:44:55", "sensor/temperature")]

    # Step 3: Map subscriptions to their latency requirements
    subscription_latency_map = {row[0]: row[1] for row in subscriptions}

    # Step 4: Identify topics that need publishers
    topics_with_no_publishers_query = '''
        SELECT DISTINCT subscription, max_allowed_latency
        FROM publish
        LEFT JOIN subscriptions ON subscription = topic
        WHERE NOT EXISTS (
            SELECT 1 FROM publish
            WHERE subscription = topic AND publishing = 1
        )
    '''
    topics_with_no_publishers = db.execute_query_with_retry(query=topics_with_no_publishers_query)
    # topics_with_no_publishers example: [("sensor/pressure", 150)]

    # Step 5: Assign topics to devices
    assignments = []
    for topic, latency in topics_with_no_publishers:
        # Get devices capable of publishing to this topic
        capable_devices_query = '''
            SELECT deviceMac, capacity, executions, consumption
            FROM devices
            WHERE capabilities LIKE ?
        '''
        capable_devices = db.execute_query_with_retry(query=capable_devices_query, values=(f"%{topic}%",))

        # Select the first available capable device
        for device in capable_devices:
            mac, capacity, executions, consumption = device
            assignments.append({
                "deviceMac": mac,
                "topic": topic,
                "latency": latency
            })
            # Once a device is assigned, break out of the loop
            break

    # Update database and devices with assignments
    publishers = Devices()
    for assignment in assignments:
        mac = assignment["deviceMac"]
        topic = assignment["topic"]
        latency = assignment["latency"]

        # Add the processing unit for the device
        if mac not in publishers._units:
            publishers.addProcessingUnit(Processing_Unit(
                macAddr=mac,
                capacity=None,  # Retrieve from DB or logic
                executions=None,
                consumption=None
            ))

        # Add assignment and update DB
        publishers._units[mac].addAssignment(topic, latency)
        db.updatePublishTableWithPublishingAssignments(mac, [topic])

    db.closeDB()

    # Return the final assignment list
    return assignments

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



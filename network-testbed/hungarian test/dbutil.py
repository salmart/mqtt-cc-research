import sqlite3
import numpy as np
import hungarian_task_assignment as hung
import ast  # For safe string-to-list conversion
import time

np.set_printoptions(precision=2, suppress=True, linewidth=np.inf)

DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"

def fetch_subscribers():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT s.DeviceMac,
               s.Tasks,
               s.Min_Frequency,
               s.MaxAllowedLatency,
               s.Accuracy,
               t.TopicName
        FROM Subscribers s
        JOIN Topics t ON s.DeviceMac = t.DeviceMac
    """)
    rows = cursor.fetchall()
    conn.close()

    subscribers_dict = {}

    for row in rows:
        device_mac    = row[0]
        tasks         = ast.literal_eval(row[1])  # e.g. ["Motion","Humidity"]
        min_freq      = ast.literal_eval(row[2])
        max_latency   = ast.literal_eval(row[3])
        accuracy      = ast.literal_eval(row[4])
        topic_name    = row[5]  # <-- the second column in Topics table

        subscribers_dict[device_mac] = {
            "topic_name": topic_name,  # store it
            "tasks": tasks,
            "min_freq": dict(zip(tasks, min_freq)),
            "max_latency": dict(zip(tasks, max_latency)),
            "accuracy": dict(zip(tasks, accuracy)),
        }

    return subscribers_dict


def filter_publishers(publishers_dict):
    """Filter out publishers that do not match required tasks & constraints from subscribers."""
    subscribers_dict = fetch_subscribers()
    
    required_tasks = set()
    for sub in subscribers_dict.values():
        required_tasks.update(sub["tasks"])

    filtered_publishers = {}

    for device_mac, task_data in publishers_dict.items():
        valid_tasks = {}
        
        for task, energy in task_data["energy"].items():
            if task not in required_tasks:
                continue  # Skip tasks not needed

            for sub in subscribers_dict.values():
                if task in sub["tasks"]:
                    # Apply filtering constraints
                    meets_freq = (task in task_data["max_freq"]
                                  and task_data["max_freq"][task] >= sub["min_freq"][task])
                    meets_latency = (task in task_data["max_latency"]
                                     and task_data["max_latency"][task] <= sub["max_latency"][task])
                    meets_accuracy = (task in task_data["accuracy"]
                                      and task_data["accuracy"][task] >= sub["accuracy"][task])

                    if meets_freq and meets_latency and meets_accuracy:
                        valid_tasks[task] = energy

        if valid_tasks:
            filtered_publishers[device_mac] = valid_tasks
    
    return filtered_publishers, required_tasks

def fetch_publishers():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.DeviceMac,
               p.Tasks,
               p.Energy,
               p.Max_Frequency,
               p.Max_Latency,
               p.Accuracy,
               t.TopicName
        FROM Publishers p
        JOIN Topics t ON p.DeviceMac = t.DeviceMac
    """)
    data = cursor.fetchall()
    conn.close()

    device_task_mapping = {}
    for row in data:
        device_mac    = row[0]
        tasks         = ast.literal_eval(row[1])      # e.g. ["TaskA","TaskB"]
        energy_values = ast.literal_eval(row[2])
        max_freq      = ast.literal_eval(row[3])
        max_latency   = ast.literal_eval(row[4])
        accuracy      = ast.literal_eval(row[5])
        topic_name    = row[6]  # new from the JOIN

        if len(tasks) == len(energy_values) == len(max_freq) == len(max_latency) == len(accuracy):
            device_task_mapping[device_mac] = {
                "topic_name": topic_name,  # store it here
                "energy":      dict(zip(tasks, energy_values)),
                "max_freq":    dict(zip(tasks, max_freq)),
                "max_latency": dict(zip(tasks, max_latency)),
                "accuracy":    dict(zip(tasks, accuracy)),
            }
        else:
            print(f"[Warning] Mismatch in array lengths for {device_mac}")

    return device_task_mapping



def get_task_energy_matrix():

    device_task_mapping = fetch_publishers()
    # Apply filtering based on subscriber requirements
    filtered_publishers, sorted_tasks = filter_publishers(device_task_mapping)

    # Create the matrix: each row corresponds to a publisher, each column to a task
    energy_matrix = []
    sorted_tasks = sorted(sorted_tasks)  # Sort tasks to maintain consistent column order

    for device_mac, task_energy_map in filtered_publishers.items():
        row_values = [task_energy_map.get(task, 9999999) for task in sorted_tasks]  # Large cost for missing tasks
        energy_matrix.append(row_values)

    # Convert to NumPy array
    energy_matrix = np.array(energy_matrix, dtype=np.float64)

    return energy_matrix, filtered_publishers, sorted_tasks, device_task_mapping

def rel():
    """
    Runs the Hungarian algorithm and returns TWO dictionaries:
    1. final_assignments: {subscriber_topic: [(publisher_mac, task), ...]}
    2. publisher_assignments: {publisher_topic: [task1, task2, ...]}
    """
    # 1) Build the energy/cost matrix AND fetch the original mappings
    matrix, mapping, tasks, device_task_mapping = get_task_energy_matrix()
    #    where 'mapping' == the filtered_publishers, 
    #    and 'device_task_mapping' includes .topic_name for each publisher_mac

    # 2) Fetch subscriber data
    subscribers_dict = fetch_subscribers()

    # 3) Create reverse lookup for tasks -> subscribers
    task_to_subs = {}
    for sub_mac, sub_data in subscribers_dict.items():
        for t in sub_data["tasks"]:
            task_to_subs.setdefault(t, []).append(sub_mac)

    # Our output dictionaries
    final_assignments = {}       # {subscriber_topic: [(publisher_mac, task), ...]}
    publisher_assignments = {}   # {publisher_topic: [task1, task2, ...]}

    if matrix.size > 0:
        # This returns a list of (row_idx, col_idx)
        assignments = hung.hungarian_algorithm(matrix)

        # Publisher list for row->publisher_mac lookups
        publisher_list = list(mapping.keys())

        # For each assignment row/col
        for (row_idx, col_idx) in assignments:
            publisher_mac = publisher_list[row_idx]
            assigned_task = tasks[col_idx]

            # -- SUBSCRIBER PART --
            # Find all subscribers who want this task
            subs_for_this_task = task_to_subs.get(assigned_task, [])
            for sub_mac in subs_for_this_task:
                # Convert from sub_mac to sub_topic
                sub_topic = subscribers_dict[sub_mac]["topic_name"]
                if sub_topic not in final_assignments:
                    final_assignments[sub_topic] = []
                final_assignments[sub_topic].append((publisher_mac, assigned_task))

            # -- PUBLISHER PART --
            # Convert from publisher_mac to publisher_topic
            publisher_topic = device_task_mapping[publisher_mac]["topic_name"]
            if publisher_topic not in publisher_assignments:
                publisher_assignments[publisher_topic] = []
            publisher_assignments[publisher_topic].append(assigned_task)

    return final_assignments, publisher_assignments

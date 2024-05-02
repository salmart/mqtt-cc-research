from constants import ConfigUtils
from datetime import datetime
from subscriber_container import Subscriber_Container
from pub_container import Publisher_Container
from topic_container import Topic_Container
import random
import sys
import csv
from mqtt_cc import MQTTCC
from round_robin import RR

# EXPERIMENT SET UP
configuration = ConfigUtils()
configuration.setConstants("config.ini")
file_paths = {
    "pub_path": "../results_pubs/",
    "sub_path": "../results_subs/",
    "topic_path": "../results_topics/"
}
filename = "results_" + datetime.now() + "_"

# create containers
pub_c = Publisher_Container()
sub_c = Subscriber_Container()
topic_c = Topic_Container()

# Set Defaults
pub_c.setDefaultNumPubs(configuration._default_num_pubs)
sub_c.setDefaultNumSubs(configuration._default_num_subs)
topic_c.setDefaultNumTopics(configuration._default_num_topics)

# other constants
sub_c.setLatencyMinMax(min=configuration._LAT_QOS_MIN, max=configuration._LAT_QOS_MAX)
pub_c.setEnergies(sense_energy=configuration._sense_energy, comm_energy=configuration._comm_energy)


# create capability matrix
    # dictionary with
        # key = topic
        # value = tuple (index of publishing device, [list of all capable devices])
system_capability = {}

# Precondition: all the topic strings are created
def createSystemCapability():
    capability = {topic: (-1, []) for topic in topic_c._topic_dict.keys()}
    for topic in topic_c._topic_dict.keys(): # for every topic
        for device in pub_c._devices._units.values(): # find the device
            if device.capableOfPublishing(topic):
                system_capability[topic][1].append(device._device_mac)
    return capability

def setup_exp_vary_pub():
    exp_num_pub = random.randint(2, configuration._max_pubs)
    topic_c.setupTopicStrings(numTopics=0)
    sub_c.setUpLatQoS(num_subs=0)
    pub_c.setupDevices(num_pubs=exp_num_pub)

def setup_exp_vary_sub():
    exp_num_subs = random.randint(2, configuration._max_subs)
    topic_c.setupTopicStrings(numTopics=0)
    sub_c.setUpLatQoS(num_subs=exp_num_subs)
    pub_c.setupDevices(num_pubs=0)

def setup_exp_vary_topic():
    exp_num_topics = random.randint(2, configuration._max_topics)
    topic_c.setupTopicStrings(numTopics=exp_num_topics)
    sub_c.setUpLatQoS(num_subs=0)
    pub_c.setupDevices(num_pubs=0)

# performed once before the rounds start
def experiment_setup():
    # based on the vary_xxx config settings, begin setup functions for the containers
    if configuration._vary_pubs:
        setup_exp_vary_pub()
    elif configuration._vary_subs:
        setup_exp_vary_sub()        
    elif configuration._vary_topics:
        setup_exp_vary_topic()
    else:
        print("there is an error in your config file")
        sys.exit()
    topic_c.setupSenseTimestamps()
    global system_capability 
    system_capability = createSystemCapability()

# CSV Format for all files
    # algo_name, num_rounds, num_topic, num_pubs, num_subs, average_energy_consumption over all rounds
def saveResults(algo_name:str, avg_energy_consumption:float):
    if configuration._vary_pubs:
        file_path = file_paths["pub_path"] + filename + "pub"
    elif configuration._vary_subs:
        file_path = file_paths["sub_path"] + filename + "sub"
    elif configuration._vary_topics:
        file_path = file_paths["topic_path"] + filename + "topic"
    data = [algo_name, configuration._sim_rounds, avg_energy_consumption]
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)

def main():
    # create algo objects
    rr = RR()
    cc = MQTTCC()
    global system_capability
    for round in range(configuration._sim_rounds):
        # set up experiment
        experiment_setup()
        # set system capability and timestamps for algorithms
        rr.copyOfSystemCapability(system_capability)
        rr.copyOfTopicTimeStamps(topic_c._all_sense_timestamps)
        cc.copyOfSystemCapability(system_capability)
        cc.copyOfTopicTimeStamps(topic_c._all_sense_timestamps)

        # run RR first
        rr.rr_algo()
        # save the total energy consumption
        pub_c._devices.calculateTotalEnergyConsumption()
        rr_energy_consumption = pub_c._devices._all_devices_energy_consumption
        rr.saveDevicesTotalEnergyConsumed(round_energy_consumption=rr_energy_consumption)

        # after running the algorithm, clear everything
        pub_c._devices.clearUnits()
        pub_c._devices.clearAllDeviceEnergyConsumption()
        topic_c.clearTopicDict()
    rr_avg_energy_consumption = rr._total_energy_consumption / configuration._sim_rounds
    saveResults(algo_name=rr._algo_name, avg_energy_consumption=rr_avg_energy_consumption)

    # for loop num rounds
        # set up experiment 
        # run cc
        # reset anything in between algos
        # run rr
        # reset anyhting in between algos
    # run saveResults with algo + total_consumption / num_rounds

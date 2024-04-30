from typing import Dict
from copy import deepcopy
from topic_container import Topic_Container
import random

class Devices:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self) -> None:
        # possibly set some constants
        self._units: Dict[str, Processing_Unit] = dict()
        self.OBSERVATION_PERIOD_MILISEC = 1000 * 3600

    def setSensingEnergy(self, sense_energy):
        self.SENSING_ENERGY = sense_energy

    def setCommEnergy(self, comm_energy):
        self.COMMUNICATION_ENERGY = comm_energy

    def setThreshold(self, threshold):
        self.CONCURRENCY_THRESHOLD_MILISEC = threshold

    # Called after completing a round for 1 algorithm
    # resets any parameters that may differ between algorithms
    def resetUnits(self):
        for device in self._units.values():
            device.resetAssignments()
            device._consumption = 0
            device._battery = 100
            device.setExecutions(new_value=0)

class Processing_Unit:

    def __init__(self):
        self._assignments = {} # topic: publishing latency
        self._battery = 100
        self._consumption = 0
        self._capable_topics = []
        self._num_executions_per_hour = 0
        # For calculating total energy consumption (for all algorithms)
        self._sense_timestamp = []

    def setMac(self, mac):
        self._device_mac = mac

    def addAssignment(self, added_topic, added_qos):
        self._assignments[added_topic] = added_qos
    
    def resetAssignments(self):
        self._assignments.clear()    

    def setCapableTopics(self, capability:list):
        self._capable_topics = capability    

    def capableOfPublishing(self, topic):
        if topic in self._capable_topics:
            return True
        else:
            return False
        
    def setExecutions(self, new_value):
        self._num_executions_per_hour = new_value

    def updateConsumption(self, energy_increase):
        self._consumption+=energy_increase

    # Performed for MQTT-CC only
    def calculateExecutions(self, new_task_freq = None):
        threshold = Devices._instance.CONCURRENCY_THRESHOLD_MILISEC
        all_freqs = deepcopy(self._assignments.values())
        if new_task_freq:
            print("new frequency found")
            all_freqs.append(new_task_freq)
            print("new frequency added")
        if not all_freqs:
            # if no frequencies, there are no executions
            return 0
        freq_multiples = set(all_freqs)
        execution_group = []
        group_min = None
        num_executions = 0
        multiplier = 2

        for freq in freq_multiples:
            multiple = freq * multiplier
            while multiple < Devices._instance.OBSERVATION_PERIOD_MILISEC:
                freq_multiples.add(multiple)
                multiple+=1
                multiple = freq * multiplier
            multiple = 2
        freq_multiples = list(freq_multiples) 
        freq_multiples.sort()
        for i in range(len(freq_multiples)):
            if i == 0:
                execution_group.append(freq_multiples[i])
                group_min = freq_multiples[i]
                print("starting new execution")
            else:
                if abs(freq_multiples[i] - group_min) < threshold:
                    execution_group.append(freq_multiples[i])
                    print("freq occurs in same execution")
                else:
                    num_executions+=1
                    execution_group.clear()
                    execution_group.append(freq_multiples[i])
                    group_min = freq_multiples[i]
                    print("freq not in the same execution, resetting")
        if len(execution_group):
            num_executions+=1
            
        print(f"num execution = {num_executions}")

        return num_executions
    
    # Performed by MQTT-CC only
    def energyIncrease(self, task_freq):
        newExecutions = self.calculateExecutions(new_task_freq=task_freq)
        changeInExecutions = newExecutions - self._num_executions_per_hour
        energyUsed = newExecutions * Devices._instance.SENSING_ENERGY + changeInExecutions * Devices._instance.COMMUNICATION_ENERGY
        return energyUsed

class Publisher_Container:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self) -> None:
        # possibly set some constants
        self._devices = Devices()
        self._total_devices = 0
        pass
    # PERFORM THIS FUNCTION FIRST BEFORE ANYTHING ELSE
    def setDefaultNumPubs(self, default_num_pubs):
        self._default_num_pubs = default_num_pubs

    def setEnergies(self, sense_energy, comm_energy):
        self._devices.setSensingEnergy(sense_energy)
        self._devices.setCommEnergy(comm_energy)

    # Precondition: numPubs is a whole number > 0
    def generatePublisherMacs(numPubs):
        pub_macs = []
        for i in range(numPubs):
            name = f"dev00{i}"
            pub_macs.append(name)
        print(pub_macs)
        return pub_macs

    def setupDevices(self, num_pubs):
        if num_pubs == 0:
            print(f"setting default devices {self._default_num_pubs}")
            num_pubs = self._default_num_pubs
        self._total_devices = num_pubs
        print(f"creating {num_pubs} devices")
        device_macs = self.generatePublisherMacs(num_pubs)
        for mac in device_macs:
            self._devices._units[mac] = Processing_Unit()
            self._devices._units[mac].setMac(mac)
        self.generateDeviceCapability()
    
    # Precondition: Topics are created 
    def generateDeviceCapability(self):
        found = False
        topics = Topic_Container()
        for unit in self._devices._units.values():
            num_capable_publishes = random.randint(start=1, stop=topics._total_topics)
            # randomly sample this number of topics with their max_allowed_latency
            publishes = random.sample(population=topics._topic_dict.keys(), k=num_capable_publishes)
            unit.setCapableTopics(capability=publishes)
        for topic in topics._topic_dict.keys():
            for unit in self._devices._units.values():
                if unit.capableOfPublishing(topic):
                    found = True
                    break
            if not found:
                # if the topic is not covered by any device
                # get a random device
                rand_mac = random.choice(self._devices._units.keys())
                # assign the topic t topicInCapable(self,)o it
                self._devices._units[rand_mac]._capable_topics.append(topic)
            # reset found to False
            found = False
        # all topic capabilities are created, saved, and cover all topics
            

        

            
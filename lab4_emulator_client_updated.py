# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
import settings

# number of devices
device_count = settings.DEVICE_COUNT

TOPIC = "myTopic"

#Path to the dataset, modify this
data_path = "data/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "./certs/device_{}/device_{}.certificate.pem"
key_formatter = "./certs/device_{}/device_{}.private.key"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint(settings.AWS_ENDPOINT, 8883)
        self.client.configureCredentials("certs/aws_root_ca.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self, *args):
        # inconsistency in how customOnMessage is called, varying number of args
        # Extract the message object from the arguments. Assume it is always last arg
        message = args[-1]
        # decode byte string, since MQTT is a binary protocol
        message_payload = json.loads(message.payload.decode('utf-8'))
        print(message)
        print(message.topic)
        print("client {} received message {} from topic {}".format(self.device_id, message_payload.get("message"), message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass

    def subscribe(self, topic):
        self.client.subscribe(topic, settings.QUALITY_OF_SERVICE, self.customOnMessage)


    def publish(self, message="kek", device_id=0):
        #TODO4: fill in this function for your publish
        self.client.subscribeAsync(TOPIC, settings.QUALITY_OF_SERVICE, ackCallback=self.customSubackCallback)
        
        self.client.publishAsync(TOPIC, self.craftPayload(message), settings.QUALITY_OF_SERVICE, ackCallback=self.customPubackCallback)
    
    def craftPayload(self, message):
        # This function generates a json payload
        data = {
            "message": message,
            "device_id": self.device_id
        }
        print(json.dumps(data).encode("utf-8"))
        return json.dumps(data)



print("Loading vehicle data...")
data = []
for i in range(device_count):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_count):
    client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
    client.client.connect()
    client.subscribe(TOPIC)
    clients.append(client)

while True:
    print("send now? s/d")
    x = input()
    if x == "s":
        for i,c in enumerate(clients):
            c.publish(device_id=i)

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)
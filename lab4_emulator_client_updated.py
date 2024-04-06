# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
import settings

# number of devices
device_count = settings.DEVICE_COUNT

PUBLISH_TOPIC = "data/send/co2"
SUBSCRIBE_TOPIC = "data/receive/co2"

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
        # ignore messages not destined self
        if self.device_id != str(message_payload.get("device_id")):
            return
        
        # print(message)
        # print(message.topic)
        print("client {} received data {} from topic {}".format(self.device_id, message_payload.get("data"), message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass

    def subscribe(self, topic):
        # self.client.subscribe(topic, settings.QUALITY_OF_SERVICE, self.customOnMessage)
        self.client.subscribeAsync(topic, settings.QUALITY_OF_SERVICE, ackCallback=self.customSubackCallback)


    def publish(self, co2_reading):
        #TODO4: fill in this function for your publish
        # self.client.subscribeAsync(PUBLISH_TOPIC, settings.QUALITY_OF_SERVICE, ackCallback=self.customSubackCallback)
        
        self.client.publishAsync(PUBLISH_TOPIC, self.craftPayload(co2_reading), settings.QUALITY_OF_SERVICE, ackCallback=self.customPubackCallback)
    
    def craftPayload(self, co2_reading):
        # This function generates a json payload
        payload = {
            "co2_reading": co2_reading,
            "device_id": self.device_id
        }
        # print(json.dumps(payload).encode("utf-8"))
        return json.dumps(payload)
    

def read_csv_row(counter, device_id):
    data = pd.read_csv(data_path.format(device_id), header=0)
    total_rows = len(data)
    row_index = counter % total_rows
    selected_row = data.iloc[row_index]
    return selected_row['vehicle_CO2']


print("Initializing MQTTClients...")
clients = []
for device_id in range(device_count):
    client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
    client.client.connect()
    client.subscribe(SUBSCRIBE_TOPIC)
    clients.append(client)

counter = [0] * 5
while True:
    print("Enter device id to simulate. Or press d to exit simulation.")
    x = input()
    if x.isdigit():
        if (x:=int(x)) in range(device_count):
            co2_reading = read_csv_row(counter[x], x)
            clients[x].publish(co2_reading)
            counter[x] += 1
        else:
            print("Not a valid device")
            continue
    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)
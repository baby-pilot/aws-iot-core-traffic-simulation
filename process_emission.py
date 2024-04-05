import json
import logging
import sys
import awsiot.greengrasscoreipc.clientv2 as clientV2
import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data")

# Counter
my_counter = 1
def lambda_handler(event, context):
    global my_counter

    # TODO1: Get your data
	# Extract CO2 values from the 3rd index of each JSON row
    co2_values = [float(row[2]) for row in event['csv_data'][1:]]

    #TODO2: Calculate max CO2 emission
    # Calculate the maximum CO2 value
    max_co2 = max(co2_values)
    print(f"***** MAX CO2: {max_co2} *****")
    #TODO3: Return the result
    client.publish(
        topic=f"clients/{event['thingName']}/max_co2",
        payload=json.dumps(
            {"message": f"Max CO2: {max_co2}! Sent from Greengrass Core.  Invocation Count: {my_counter}"}
       ),
    )
    my_counter += 1

    return

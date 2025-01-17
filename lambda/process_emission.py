import json
import logging
import sys

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# SDK Client
client = greengrasssdk.client("iot-data")

# Counter
device_count = 5
max_co2 = [0] * device_count
my_counter = 0
base_topic = "data/receive/co2"

def lambda_handler(event, context):
    global my_counter
    
    device_id = event.get("device_id")
    co2_reading = event.get("co2_reading")
    logger.info("Entered the lambda")
    
    if device_id is None or co2_reading is None:
        logger.error("Missing device_id or co2_reading in the event.")
        return
    
    max_co2[device_id] = max(co2_reading,  max_co2[device_id])
    logger.info(max_co2)
    payload = {
        "seq_no": my_counter,
        "device_id": device_id, 
        "co2_reading": max_co2[device_id]
    }
    logger.info(payload)

    client.publish(
        topic=base_topic,
        payload=json.dumps(payload),
    )
    my_counter += 1

    return payload
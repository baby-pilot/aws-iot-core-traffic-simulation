import boto3
import json
import random
import string

nDevices = 5
thingClient = boto3.client('iot')  # Moved thingClient creation outside the functions

def createThing(index):
    global thingClient
    thingResponse = thingClient.create_thing(
        thingName=thingName + f'{index}'
    )
    data = json.loads(json.dumps(thingResponse, sort_keys=False, indent=4))
    thingArn = data['thingArn']  # Moved thingArn assignment inside the function
    thingId = data['thingId']    # Moved thingId assignment inside the function
    createCertificate(index, thingArn)

def createCertificate(index, thingArn):
    certResponse = thingClient.create_keys_and_certificate(setAsActive=True)
    data = json.loads(json.dumps(certResponse, sort_keys=False, indent=4))
    certificateArn = data['certificateArn']
    PublicKey = data['keyPair']['PublicKey']
    PrivateKey = data['keyPair']['PrivateKey']
    certificatePem = data['certificatePem']
    certificateId = data['certificateId']

    with open(f'public{index}.key', 'w') as outfile:
        outfile.write(PublicKey)
    with open(f'private{index}.key', 'w') as outfile:
        outfile.write(PrivateKey)
    with open(f'cert{index}.pem', 'w') as outfile:
        outfile.write(certificatePem)

    response = thingClient.attach_policy(
        policyName=defaultPolicyName,
        target=certificateArn
    )
    response = thingClient.attach_thing_principal(
        thingName=thingName + f'{index}',  # Fixed thingName reference
        principal=certificateArn
    )

thingName = 'device'
defaultPolicyName = 'lab4_iot_policy'

for n in range(nDevices):
    createThing(n)

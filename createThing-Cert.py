import boto3
import json
import random
import string
import os  # Added import for handling file paths

nDevices = 5
thingClient = boto3.client('iot')

def createThing(index):
    global thingClient
    thingResponse = thingClient.create_thing(
        thingName=thingName + f'{index}'
    )
    data = thingResponse  # No need to serialize and deserialize the response
    thingArn = data['thingArn']
    thingId = data['thingId']
    createCertificate(index, thingArn)

def createCertificate(index, thingArn):
    certResponse = thingClient.create_keys_and_certificate(setAsActive=True)
    data = certResponse  # No need to serialize and deserialize the response
    certificateArn = data['certificateArn']
    PublicKey = data['keyPair']['PublicKey']
    PrivateKey = data['keyPair']['PrivateKey']
    certificatePem = data['certificatePem']
    certificateId = data['certificateId']
    deviceName = f'{thingName}{index}'

    # Creating directories if they don't exist
    os.makedirs(f'certs/{deviceName}', exist_ok=True)

    # Writing certificate files
    with open(f'certs/{deviceName}/{deviceName}.public.key', 'w') as outfile:
        outfile.write(PublicKey)
    with open(f'certs/{deviceName}/{deviceName}.private.key', 'w') as outfile:
        outfile.write(PrivateKey)
    with open(f'certs/{deviceName}/{deviceName}.certificate.pem', 'w') as outfile:
        outfile.write(certificatePem)

    response = thingClient.attach_policy(
        policyName=defaultPolicyName,
        target=certificateArn
    )
    response = thingClient.attach_thing_principal(
        thingName=thingName + f'{index}',
        principal=certificateArn
    )

thingName = 'device_'
defaultPolicyName = 'lab4_iot_policy'

for n in range(nDevices):
    createThing(n)

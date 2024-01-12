

# MQTT Client demo
# Continuously monitor two different MQTT topics for data,
# check if the received data matches two predefined 'commands'
 
import paho.mqtt.client as mqtt
import ast
import datetime
import yaml
import collections
import json
import ssl
from mintsXU4 import mintsSensorReader as mSR
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsLatest as mL
import sys
import pandas as pd

mqttPort              = mD.mqttPort
mqttBroker            = mD.mqttBrokerDC
mqttCredentialsFile   = mD.credentials
tlsCert               = mD.tlsCertsFile
nodeInfo              = mD.nodeInfoDC

sensorInfo            = mD.sensorInfo
credentials           = mD.credentials

connected             = False  # Stores the connection status
broker                = mqttBroker  
port                  = mqttPort  # Secure port
mqttUN                = credentials['mqtt']['username'] 
mqttPW                = credentials['mqtt']['password'] 

nodeIDs               = nodeInfo['nodeIDs']
# nodeIDsLN             = nodeInfoLN['nodeIDs']
sensorIDs             = sensorInfo['sensorID']

decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
 
    # Subscribing in on_connect() - if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for nodeID in nodeIDs:
        for sensorID in sensorIDs:
            topic = nodeID+"/"+ sensorID
            client.subscribe(topic)
            print("Subscrbing to Topic: "+ topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print()
    print(" - - - MINTS DATA RECEIVED - - - ")
    print()
    # print(msg.topic+":"+str(msg.payload))
    try:
        [nodeID,sensorID] = msg.topic.split('/')
        sensorDictionary = decoder.decode(msg.payload.decode("utf-8","ignore"))
        print("Node ID   :" + nodeID)
        print("Sensor ID :" + sensorID)
        print("Data      : " + str(sensorDictionary))
        
        if sensorID== "FRG001":
            dateTime  = datetime.datetime.strptime(sensorDictionary["dateTime"], '%Y-%m-%d %H:%M:%S')
        else:
            dateTime  = datetime.datetime.strptime(sensorDictionary["dateTime"], '%Y-%m-%d %H:%M:%S.%f')
        writePath = mSR.getWritePathMQTT(nodeID,sensorID,dateTime)
        exists    = mSR.directoryCheck(writePath)
        sensorDictionary = decoder.decode(msg.payload.decode("utf-8","ignore"))
        print("Writing MQTT Data")
        print(writePath)
        mSR.writeCSV2(writePath,sensorDictionary,exists)
        mL.writeJSONLatestMQTT(sensorDictionary,nodeID,sensorID)

    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))

# Create an MQTT client and attach our routines to it.
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(mqttUN,mqttPW)

client.tls_set(ca_certs=tlsCert, certfile=None,
                            keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                            tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)


client.tls_insecure_set(True)
client.connect(broker, port, 60)
client.loop_forever()

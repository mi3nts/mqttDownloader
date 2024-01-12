






import threading
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
from mintsXU4 import mintsLoRaReader as mLR

import sys
import pandas as pd

# Common Inputs
mqttCredentialsFile   = mD.credentials
tlsCert               = mD.tlsCertsFile

sensorInfo            = mD.sensorInfo
credentials           = mD.credentials

portInfo              = mD.portInfo
nodeInfo              = mD.nodeInfo

connected             = False  # Stores the connection status

nodeIDs               = nodeInfo['nodeIDs']
sensorIDs             = sensorInfo['sensorID']

## DC Inputs
mqttPortDC              = mD.mqttPortDC
mqttBrokerDC            = mD.mqttBrokerDC

mqttUNDC                = credentials['mqtt']['username'] 
mqttPWDC                = credentials['mqtt']['password'] 

## LN Inputs
mqttPortLN              = mD.mqttPortLoRa
mqttBrokerLN            = mD.mqttBrokerLoRa

mqttUNLN                = credentials['LoRaMqtt']['username'] 
mqttPWLN                = credentials['LoRaMqtt']['password'] 



decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)


# The callback for when the client receives a CONNACK response from the server.
def on_connect_DC(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
 
    # Subscribing in on_connect() - if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for nodeID in nodeIDs:
        for sensorID in sensorIDs:
            topic = nodeID+"/"+ sensorID
            client.subscribe(topic)
            print("Subscrbing to Topic: "+ topic)

# The callback for when a PUBLISH message is received from the server.
def on_message_DC(client, userdata, msg):
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



def on_connect_LN(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    topic = "utd/lora/app/5/device/+/event/up"
    client.subscribe(topic)
    print("Subscrbing to Topic: "+ topic)

def on_message_LN(client, userdata, msg):
    try:
        print("==================================================================")
        print(" - - - MINTS DATA RECEIVED - - - ")
        # print(msg.payload)
        dateTime,gatewayID,nodeID,sensorID,framePort,base16Data = \
            mLR.loRaSummaryWrite(msg,portInfo)
        

        print("Node ID         : " + nodeID)
        print("Sensor ID       : " + sensorID)
        print(nodeID in nodeIDs)
        if nodeID in nodeIDs:
            print("Date Time       : " + str(dateTime))
            print("Port ID         : " + str(framePort))
            print("Base 16 Data    : " + base16Data)
            mLR.sensorSendLoRa(dateTime,nodeID,sensorID,framePort,base16Data)
        
    
    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))


# Create an MQTT client and attach our routines to it.
def mqtt_client_DC():       
    clientDC = mqtt.Client()
    clientDC.on_connect = on_connect_DC
    clientDC.on_message = on_message_DC
    clientDC.username_pw_set(mqttUNDC,mqttPWDC)

    clientDC.tls_set(ca_certs=tlsCert, certfile=None,
                                keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                                tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)


    clientDC.tls_insecure_set(True)
    clientDC.connect(mqttBrokerDC, mqttPortDC, 60)
    clientDC.loop_forever()


def mqtt_client_LN():       
# Create an MQTT client and attach our routines to it.
    clientLN = mqtt.Client()
    clientLN.on_connect = on_connect_LN
    clientLN.on_message = on_message_LN
    clientLN.username_pw_set(mqttUNLN,mqttPWLN)
    clientLN.connect(mqttBrokerLN, mqttPortLN, 60)
    clientLN.loop_forever()


threadDC = threading.Thread(target=mqtt_client_DC)
threadLN = threading.Thread(target=mqtt_client_LN)

# Start the threads
threadDC.start()
threadLN.start()

# Wait for all threads to finish
threadDC.join()
threadLN.join()
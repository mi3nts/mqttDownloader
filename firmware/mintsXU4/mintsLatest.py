import json
import serial
import datetime
import os
import csv
#import deepdish as dd
import time
import paho.mqtt.client as mqttClient
import yaml
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsSensorReader as mSR
import ssl

macAddress              = mD.macAddress
mqttPort                = mD.mqttPortDC
mqttBroker              = mD.mqttBrokerDC
dataFolderMQTT          = mD.dataFolderMQTT

dataFolderMQTTReference = mD.dataFolderMQTTReference
tlsCert                 = mD.tlsCertsFile

credentials             = mD.credentials
# FOR MQTT 

connected   = False  # Stores the connection status
broker      = mqttBroker
port        = mqttPort # Secure port
mqttUN      = credentials['mqtt']['username']  
mqttPW      = credentials['mqtt']['password'] 

mqtt_client = mqttClient.Client()

def on_connect(client, userdata, flags, rc):
    global connected  # Use global variable
    if rc == 0:

        print("[INFO] Connected to broker")
        connected = True  # Signal connection
    else:
        print("[INFO] Error, connection failed")


def on_publish(client, userdata, result):
    print("MQTT Published!")


def connect(mqtt_client, mqtt_username, mqtt_password, broker_endpoint, port):
    global connected

    if not mqtt_client.is_connected():
        print("Reconnecting")
        mqtt_client.username_pw_set(mqtt_username, password=mqtt_password)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_publish = on_publish
        mqtt_client.tls_set(ca_certs=tlsCert, certfile=None,
                            keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                            tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
        mqtt_client.tls_insecure_set(False)
        mqtt_client.connect(broker_endpoint, port=port)
        mqtt_client.loop_start()

        attempts = 0

        while not connected and attempts < 5:  # Wait for connection
            print(connected)
            print("Attempting to connect...")
            time.sleep(1)
            attempts += 1

    if not connected:
        print("[ERROR] Could not connect to broker")
        return False

    return True


def writeMQTTLatest(sensorDictionary,sensorName):

    if connect(mqtt_client, mqttUN, mqttPW, broker, port):
        try:
            mqtt_client.publish(macAddress+"/"+sensorName,json.dumps(sensorDictionary))

        except Exception as e:
            print("[ERROR] Could not publish data, error: {}".format(e))
    
    return True
    
def writeMQTTLatestMock(sensorDictionary,sensorName):

    if connect(mqtt_client, mqttUN, mqttPW, broker, port):
        try:
            mqtt_client.publish("MOCKID/"+sensorName,json.dumps(sensorDictionary))

        except Exception as e:
            print("[ERROR] Could not publish data, error: {}".format(e))
    
    return True

def writeJSONLatest(sensorDictionary,sensorName):
    directoryIn  = dataFolder+"/"+macAddress+"/"+sensorName+".json"
    print(directoryIn)
    try:
        with open(directoryIn,'w') as fp:
            mSR.directoryCheck(directoryIn)
            json.dump(sensorDictionary, fp)

    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))
        print("Json Data Not Written")

def writeJSONLatestMQTT(sensorDictionary,nodeID,sensorID):
    directoryIn  = dataFolderMQTT+"/"+nodeID+"/"+sensorID+".json"
    # print(directoryIn)
    try:
        with open(directoryIn,'w') as fp:
            mSR.directoryCheck(directoryIn)
            json.dump(sensorDictionary, fp)
    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))
        print("Json Data Not Written")

def writeJSONLatestMQTTReference(sensorDictionary,nodeID,sensorID):
    directoryIn  = dataFolderMQTTReference+"/"+nodeID+"/"+sensorID+".json"
    print(directoryIn)
    try:
        with open(directoryIn,'w') as fp:
            mSR.directoryCheck(directoryIn)
            json.dump(sensorDictionary, fp)
    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))
        print("Json Data Not Written")


def writeJSONLatestReference(sensorDictionary,sensorName):
    directoryIn  = dataFolderReference+"/"+macAddress+"/"+sensorName+".json"
    print(directoryIn)
    try:
        with open(directoryIn,'w') as fp:
            mSR.directoryCheck(directoryIn)
            json.dump(sensorDictionary, fp)

    except:
        print("[ERROR] Could not publish data, error: {}".format(e))
        print("Json Data Not Written")


def readJSONLatestAll(sensorName):
    try:
        directoryIn  = dataFolder+"/"+macAddress+"/"+sensorName+".json"
        with open(directoryIn, 'r') as myfile:
            # dataRead=myfile.read()
            dataRead=json.load(myfile)

        time.sleep(0.01)
        return dataRead, True;
    except:
        print("Data Conflict!")
        return "NaN", False
        

def readJSONLatestAllMQTT(nodeID,sensorID):
    directoryIn  = dataFolderMQTTReference+"/"+nodeID+"/"+sensorID+".json"
    try:
        with open(directoryIn, 'r') as myfile:
            # dataRead=myfile.read()
            dataRead=json.load(myfile)

        time.sleep(0.01)
        return dataRead, True;
    except:
        print("Data Conflict!")
        return "NaN", False



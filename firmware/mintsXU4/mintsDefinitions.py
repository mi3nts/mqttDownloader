from getmac import get_mac_address
import serial.tools.list_ports
import yaml
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import yaml

from mintsXU4 import mintsDownloader as mDL
import tkinter as tk
from tkinter import filedialog

dataFolder                = mDL.choose_folder("Please choose a downloads directory")
dataFolderReference       = dataFolder+ "/reference"
dataFolderMQTTReference   = dataFolder+ "/referenceMqtt"  # The path of your MQTT Reference Data 
dataFolderMQTT            = dataFolder + "/rawMqtt"        # The path of your MQTT Raw Data 
tlsCertsFile              = mDL.choose_file("Please choose TLS Certs file","crt")  # The path of your TLS cert

latestOn                  = False
mqttOn                    = True

credentials               = mDL.load_yaml_file(mDL.choose_file("Please choose the mints credentials file","yaml"))

sensorInfo                = pd.read_csv('https://raw.githubusercontent.com/mi3nts/mqttSubscribersV2/main/lists/sensorIDs.csv')
portInfo                  = pd.read_csv('https://raw.githubusercontent.com/mi3nts/mqttSubscribersV2/main/lists/portIDs.csv')
# nodeInfo                  = pd.read_csv('https://raw.githubusercontent.com/mi3nts/AirQualityAnalysisWorkflows/main/influxdb/nodered-docker/id_lookup.csv')


nodeInfo                   = mDL.load_yaml_file(mDL.choose_file("Please choose the MINTS Nodes List","yaml"))
# nodeInfoDC                 = load_yaml_file(choose_file("Please choose the mints DC Nodes List","*.yaml"))
# nodeInfoLN                 = load_yaml_file(choose_file("Please choose the mints LN Nodes List","*.yaml"))

mqttBrokerDC                = "mqtt.circ.utdallas.edu"
mqttBrokerLoRa              = "mqtt.lora.trecis.cloud"

mqttPortDC                  = 8883  # Secure port
mqttPortLoRa                = 1883  # Secure port

def findMacAddress():
    macAddress= get_mac_address(interface="eth0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="docker0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="enp1s0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    return "xxxxxxxx"

macAddress                = findMacAddress()

print()
print("----- MQTT Downloaders -----")
print(" ")
print("Node IDs:")
print(nodeInfo)
print(" ")
print("Sensor Info:")
print(sensorInfo)
print(" ")
print("Port Info:")
print(portInfo)
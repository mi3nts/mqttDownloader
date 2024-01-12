# // # ***************************************************************************
# // #   MQTT Subscribers V2
# // #   ---------------------------------
# // #   Written by: Lakitha Omal Harindha Wijeratne
# // #   - for -
# // #   MINTS:  Multi-scale Integrated Sensing and Simulation
# // #   &  
# // #   TRECIS: Texas Research and Education Cyberinfrastructure Services
# // #   ---------------------------------
# // #   Date: October 30th, 2023
# // #   ---------------------------------
# // #   This module is written for generic implimentation of LoRaWAN MINTS projects
# // #   --------------------------------------------------------------------------
# // #   https://github.com/mi3nts
# // #   https://mints.utdallas.edu/
# // #   https://trecis.cyberinfrastructure.org/
# // #  ***************************************************************************


import serial
import datetime
from datetime import timedelta
import os
import csv
#import deepdish as dd
from mintsXU4 import mintsLatest as mL
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsSensorReader as mSR
from getmac import get_mac_address
import time
import serial
import pynmea2
from collections import OrderedDict
import netifaces as ni
import math
import base64
import json
import struct

macAddress              = mD.macAddress
dataFolder              = mD.dataFolder
dataFolderMQTT          = mD.dataFolderMQTT
dataFolderMQTTReference = mD.dataFolderMQTTReference
latestOn                = mD.latestOn
mqttOn                  = mD.mqttOn
decoder                 = json.JSONDecoder(object_pairs_hook=OrderedDict)

def sensorSendLoRa(dateTime,nodeID,sensorID,framePort,base16Data):
    if(sensorID=="PM"):
        PMLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="PMSalor"):
        PMSalorLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="INA219Duo"):
        INA219DuoLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)  
    if(sensorID=="MLRPS001"):
        MLRPS001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="PMPoLo"):
        PMPoLoLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)        
    if(sensorID=="GPGGALR"):
        GPGGALRLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="PA1010D"):
        PA1010DLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    # if(sensorID=="GPGGA"):
    #     GPGGALoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    # if(sensorID=="GPRMC"):
    #     GPRMCLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="GPGGAPL"):
        GPGGAPLLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="GPRMCPL"):
        GPRMCPLLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="MacAD"):
        MacADLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    # if(sensorID=="OPCN2"):
    #     OPCN2LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    # if(sensorID=="OPCN3"):
    #     OPCN3LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)    
    if(sensorID=="IPS7100"):
        IPS7100LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="IPS7100CNR"):
        IPS7100CNRLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="BME280"):
        BME280LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)        
    if(sensorID=="BME280V2"):
        BME280V2LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)    
    if(sensorID=="BME688CNR"):
        BME688CNRLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)            
    if(sensorID=="SCD30"):
        SCD30LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="MGS001"):
        MGS001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data) 
    if(sensorID=="MBCLR001"):
        MBCLR001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="MBCLR002"):
        MBCLR002LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="AS7265X"):
        AS7265XLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="AS7265X1"):
        AS7265X1LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="AS7265X2"):
        AS7265X2LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
    if(sensorID=="RG15"):
        RG15LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data)
              

# ADD THE REST OF THE SENSORS HERE 

def PMSalorLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 102 and len(base16Data) ==2):
        sensorDictionary =  OrderedDict([
                    ("dateTime"      ,str(dateTime)),
                    ("powerMode",struct.unpack('<B',bytes.fromhex(base16Data[0:2]))[0])
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  

def PMPoLoLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 4 and len(base16Data) ==2):
        sensorDictionary =  OrderedDict([
                    ("dateTime"      ,str(dateTime)),
                    ("powerMode",struct.unpack('<B',bytes.fromhex(base16Data[0:2]))[0])
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  


def PA1010DLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 105 and len(base16Data) ==76):
        sensorDictionary =  OrderedDict([
                    ("dateTime"               ,str(dateTime)),
                    ("latitudeCoordinate"     ,struct.unpack('<d',bytes.fromhex(base16Data[0:16]))[0]),
                    ("longitudeCoordinate"    ,struct.unpack('<d',bytes.fromhex(base16Data[16:32]))[0]),
                    ("altitude"               ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
                    ("speed"                  ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
                    ("magVariation"           ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),        
                    ("year"                   ,struct.unpack('<H',bytes.fromhex(base16Data[56:60]))[0]),
                    ("month"                  ,struct.unpack('<B',bytes.fromhex(base16Data[60:62]))[0]),
                    ("day"                    ,struct.unpack('<B',bytes.fromhex(base16Data[62:64]))[0]),
                    ("hour"                   ,struct.unpack('<B',bytes.fromhex(base16Data[64:66]))[0]),
                    ("minute"                 ,struct.unpack('<B',bytes.fromhex(base16Data[66:68]))[0]),
                    ("second"                 ,struct.unpack('<B',bytes.fromhex(base16Data[68:70]))[0]),
                    ("satellites"             ,struct.unpack('<B',bytes.fromhex(base16Data[70:72]))[0]),
                    ("fixQuality"              ,struct.unpack('<B',bytes.fromhex(base16Data[72:74]))[0]),
                    ("fixQuality3D"            ,struct.unpack('<B',bytes.fromhex(base16Data[74:76]))[0]),

            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  


def MacADLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 8 ):
        sensorDictionary =  OrderedDict([
                    ("dateTime"      ,str(dateTime)),
                    ("macAddress"    ,base16Data),
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  


def MLRPS001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 103 and len(base16Data) ==72):
        sensorDictionary =  OrderedDict([
                    ("dateTime"               ,str(dateTime)),
                    ("batteryShuntVoltage"    ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
                    ("batteryBusVoltage"      ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                    ("batteryCurrent"         ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                    ("batteryPower"           ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
                    ("solarShuntVoltage"      ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),        
                    ("solarBusVoltage"        ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
                    ("solarCurrent"           ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
                    ("solarPower"             ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]),
                    ("internalBatteryVoltage" ,struct.unpack('<f',bytes.fromhex(base16Data[64:72]))[0]),    
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  
            



def BME280V2LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 22 and len(base16Data) ==40):
        sensorDictionary =  OrderedDict([
                    ("dateTime"     ,str(dateTime)),
                    ("temperature"  ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
                    ("pressure"     ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                    ("humidity"     ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                    ("dewPoint"     ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
                    ("altitude"     ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),        
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  

def RG15LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 61 and len(base16Data) ==32):
        sensorDictionary =  OrderedDict([
                    ("dateTime"           ,str(dateTime)),
                    ("accumulation"       ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
                    ("eventAccumulation"  ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                    ("totalAccumulation"  ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                    ("rainPerInterval"    ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  

def MBLS001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 71 and len(base16Data) ==76):
        sensorDictionary =  OrderedDict([
                    ("dateTime"            ,str(dateTime)),
                    ("batteryLevelRaw"     ,struct.unpack('<H',bytes.fromhex(base16Data[0:4]))[0]),
                    ("cellVoltage"         ,struct.unpack('<f',bytes.fromhex(base16Data[4:12]))[0]),
                    ("solarVoltage"        ,struct.unpack('<f',bytes.fromhex(base16Data[12:20]))[0]),
                    ("solarCurrent"        ,struct.unpack('<f',bytes.fromhex(base16Data[20:28]))[0]),
                    ("solarPower"          ,struct.unpack('<f',bytes.fromhex(base16Data[28:36]))[0]),
                    ("solarShuntVoltage"   ,struct.unpack('<f',bytes.fromhex(base16Data[36:44]))[0]), 
                    ("batteryVoltage"      ,struct.unpack('<f',bytes.fromhex(base16Data[44:52]))[0]),
                    ("batteryCurrent"      ,struct.unpack('<f',bytes.fromhex(base16Data[52:60]))[0]),
                    ("batteryPower"        ,struct.unpack('<f',bytes.fromhex(base16Data[60:68]))[0]),
                    ("batteryShuntVoltage" ,struct.unpack('<f',bytes.fromhex(base16Data[68:76]))[0])
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  

def MBCLR001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 42 and len(base16Data) ==16):
        dateTimePre = datetime.datetime.now() 
        lag = struct.unpack('<H',bytes.fromhex(base16Data[0:4]))[0]
        print(lag)
        dateTime = dateTimePre - timedelta(seconds = lag)
        sensorDictionary =  OrderedDict([
                ("dateTime"     ,str(dateTime)),
            	("label"        ,struct.unpack('<H',bytes.fromhex(base16Data[4:8]))[0]),
                ("confidence"   ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;  

def MBCLR002LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 43 and len(base16Data) ==130):
        dateTime = datetime.datetime.now() 
        lag0 = struct.unpack('<H',bytes.fromhex(base16Data[2:6]))[0]
        lag1 = struct.unpack('<H',bytes.fromhex(base16Data[18:22]))[0]
        lag2 = struct.unpack('<H',bytes.fromhex(base16Data[34:38]))[0]
        lag3 = struct.unpack('<H',bytes.fromhex(base16Data[50:54]))[0]    
        lag4 = struct.unpack('<H',bytes.fromhex(base16Data[66:70]))[0]    
        lag5 = struct.unpack('<H',bytes.fromhex(base16Data[82:86]))[0]    
        lag6 = struct.unpack('<H',bytes.fromhex(base16Data[98:102]))[0]    
        lag7 = struct.unpack('<H',bytes.fromhex(base16Data[114:118]))[0]            
        
        dateTime0 = dateTime - timedelta(seconds = lag0)
        dateTime1 = dateTime - timedelta(seconds = lag1)
        dateTime2 = dateTime - timedelta(seconds = lag2)
        dateTime3 = dateTime - timedelta(seconds = lag3)
        dateTime4 = dateTime - timedelta(seconds = lag4)
        dateTime5 = dateTime - timedelta(seconds = lag5)
        dateTime6 = dateTime - timedelta(seconds = lag6)
        dateTime7 = dateTime - timedelta(seconds = lag7)

        numOfCalls = struct.unpack('<B',bytes.fromhex(base16Data[0:2]))[0]


        label0       = struct.unpack('<H',bytes.fromhex(base16Data[6:10]))[0]
        confidence0  = struct.unpack('<f',bytes.fromhex(base16Data[10:18]))[0]
        label1       = struct.unpack('<H',bytes.fromhex(base16Data[22:26]))[0]
        confidence1  = struct.unpack('<f',bytes.fromhex(base16Data[26:34]))[0]
        label2       = struct.unpack('<H',bytes.fromhex(base16Data[38:42]))[0]
        confidence2  = struct.unpack('<f',bytes.fromhex(base16Data[42:50]))[0]
        label3       = struct.unpack('<H',bytes.fromhex(base16Data[54:58]))[0]
        confidence3  = struct.unpack('<f',bytes.fromhex(base16Data[58:66]))[0]
        label4       = struct.unpack('<H',bytes.fromhex(base16Data[70:74]))[0]
        confidence4  = struct.unpack('<f',bytes.fromhex(base16Data[74:82]))[0]
        label5       = struct.unpack('<H',bytes.fromhex(base16Data[86:90]))[0]
        confidence5  = struct.unpack('<f',bytes.fromhex(base16Data[90:98]))[0]
        label6       = struct.unpack('<H',bytes.fromhex(base16Data[102:106]))[0]
        confidence6  = struct.unpack('<f',bytes.fromhex(base16Data[106:114]))[0]
        label7       = struct.unpack('<H',bytes.fromhex(base16Data[118:122]))[0]
        confidence7  = struct.unpack('<f',bytes.fromhex(base16Data[122:130]))[0]

        sensorDictionary =  OrderedDict([
                        ("dateTime"      ,str(dateTime)),
                        ("numOfCalls"    ,struct.unpack('<B',bytes.fromhex(base16Data[0:2]))[0]),
                        ("dateTime0"     ,str(dateTime0)),
                        ("lag0"          ,struct.unpack('<H',bytes.fromhex(base16Data[2:6]))[0]),
                        ("label0"        ,struct.unpack('<H',bytes.fromhex(base16Data[6:10]))[0]),
                        ("confidence0"   ,struct.unpack('<f',bytes.fromhex(base16Data[10:18]))[0]),
                        ("dateTime1"     ,str(dateTime1)),
                        ("lag1"          ,struct.unpack('<H',bytes.fromhex(base16Data[18:22]))[0]),
                        ("label1"        ,struct.unpack('<H',bytes.fromhex(base16Data[22:26]))[0]),
                        ("confidence1"   ,struct.unpack('<f',bytes.fromhex(base16Data[26:34]))[0]),
                        ("dateTime2"     ,str(dateTime2)),
                        ("lag2"          ,struct.unpack('<H',bytes.fromhex(base16Data[34:38]))[0]),
                        ("label2"        ,struct.unpack('<H',bytes.fromhex(base16Data[38:42]))[0]),
                        ("confidence2"   ,struct.unpack('<f',bytes.fromhex(base16Data[42:50]))[0]),
                        ("dateTime3"     ,str(dateTime3)),
                        ("lag3"          ,struct.unpack('<H',bytes.fromhex(base16Data[50:54]))[0]),
                        ("label3"        ,struct.unpack('<H',bytes.fromhex(base16Data[54:58]))[0]),
                        ("confidence3"   ,struct.unpack('<f',bytes.fromhex(base16Data[58:66]))[0]),
                        ("dateTime4"     ,str(dateTime4)),
                        ("lag4"          ,struct.unpack('<H',bytes.fromhex(base16Data[66:70]))[0]),
                        ("label4"        ,struct.unpack('<H',bytes.fromhex(base16Data[70:74]))[0]),
                        ("confidence4"   ,struct.unpack('<f',bytes.fromhex(base16Data[74:82]))[0]),
                        ("dateTime5"     ,str(dateTime5)),
                        ("lag5"          ,struct.unpack('<H',bytes.fromhex(base16Data[82:86]))[0]),
                        ("label5"        ,struct.unpack('<H',bytes.fromhex(base16Data[86:90]))[0]),
                        ("confidence5"   ,struct.unpack('<f',bytes.fromhex(base16Data[90:98]))[0]),
                        ("dateTime6"     ,str(dateTime6)),
                        ("lag6"          ,struct.unpack('<H',bytes.fromhex(base16Data[98:102]))[0]),
                        ("label6"        ,struct.unpack('<H',bytes.fromhex(base16Data[102:106]))[0]),
                        ("confidence6"   ,struct.unpack('<f',bytes.fromhex(base16Data[106:114]))[0]),
                        ("dateTime7"     ,str(dateTime7)),
                        ("lag7"          ,struct.unpack('<H',bytes.fromhex(base16Data[114:118]))[0]),
                        ("label7"        ,struct.unpack('<H',bytes.fromhex(base16Data[118:122]))[0]),
                        ("confidence7"   ,struct.unpack('<f',bytes.fromhex(base16Data[122:130]))[0]),
                    ])

        print(sensorDictionary)
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)

        sensorID = "MBCLR001"
        if numOfCalls >0 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime0)),
                            ("label"        ,label0),
                            ("confidence"   ,confidence0),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime0,sensorDictionary)
        
        if numOfCalls >1 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime1)),
                            ("label"        ,label1),
                            ("confidence"   ,confidence1),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime1,sensorDictionary)

        if numOfCalls >2 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime2)),
                            ("label"        ,label2),
                            ("confidence"   ,confidence2),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime2,sensorDictionary)
        
        if numOfCalls >3 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime3)),
                            ("label"        ,label3),
                            ("confidence"   ,confidence3),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime3,sensorDictionary)

        if numOfCalls >4 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime4)),
                            ("label"        ,label4),
                            ("confidence"   ,confidence4),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime4,sensorDictionary)

        if numOfCalls >5 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime5)),
                            ("label"        ,label5),
                            ("confidence"   ,confidence5),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime5,sensorDictionary)

        if numOfCalls >6 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime6)),
                            ("label"        ,label6),
                            ("confidence"   ,confidence6),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime6,sensorDictionary)

        if numOfCalls > 7 :
            sensorDictionary =  OrderedDict([
                            ("dateTime"     ,str(dateTime7)),
                            ("label"        ,label7),
                            ("confidence"   ,confidence7),
                        ])
            loRaWriteFinisher(nodeID,sensorID,dateTime7,sensorDictionary)
        return ;  

    def PMPoLoLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
        if(framePort == 4 and len(base16Data) ==2):
            sensorDictionary =  OrderedDict([
                    ("dateTime"      ,str(dateTime)),
                    ("powerMode",struct.unpack('<B',bytes.fromhex(base16Data[0:2]))[0])
            ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
        return ;   

    def MacADLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
        print(len(base16Data))
        if(framePort == 8 and len(base16Data) ==12):
            sensorDictionary =  OrderedDict([
                    ("dateTime"      ,str(dateTime)),
                    ("macAddress" ,base16Data),
            ])
            print(sensorDictionary)        
            loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;    

def GPGGAPLLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 106 and len(base16Data) ==66) :
        sensorDictionary =  OrderedDict([
                ("dateTime"            ,str(dateTime)),
        		("hour"                ,struct.unpack('<B',bytes.fromhex(base16Data[0:2]))[0]),
                ("minute"              ,struct.unpack('<B',bytes.fromhex(base16Data[2:4]))[0]),
                ("second"              ,struct.unpack('<B',bytes.fromhex(base16Data[4:6]))[0]),
            	("latitudeCoordinate"  ,struct.unpack('<d',bytes.fromhex(base16Data[6:22]))[0]),
                ("longitudeCoordinate" ,struct.unpack('<d',bytes.fromhex(base16Data[22:38]))[0]),
	            ("gpsQuality"          ,struct.unpack('<B',bytes.fromhex(base16Data[38:40]))[0]),
                ("numberOfSatellites"  ,struct.unpack('<B',bytes.fromhex(base16Data[40:42]))[0]),
                ("horizontalDilution"  ,struct.unpack('<f',bytes.fromhex(base16Data[42:50]))[0]),
            	("altitude"            ,struct.unpack('<f',bytes.fromhex(base16Data[50:58]))[0]),
        		("undulation"          ,struct.unpack('<f',bytes.fromhex(base16Data[58:66]))[0]),
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def GPRMCPLLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 107 and len(base16Data) ==54) :
        sensorDictionary =  OrderedDict([
                ("dateTime"            ,str(dateTime)),
         		("year"                ,struct.unpack('<H',bytes.fromhex(base16Data[0:4]))[0]),
                ("month"               ,struct.unpack('<B',bytes.fromhex(base16Data[4:6]))[0]),
                ("day"                 ,struct.unpack('<B',bytes.fromhex(base16Data[6:8]))[0]),               
        		("hour"                ,struct.unpack('<B',bytes.fromhex(base16Data[8:10]))[0]),
                ("minute"              ,struct.unpack('<B',bytes.fromhex(base16Data[10:12]))[0]),
                ("second"              ,struct.unpack('<B',bytes.fromhex(base16Data[12:14]))[0]),
            	("latitudeCoordinate"  ,struct.unpack('<d',bytes.fromhex(base16Data[14:30]))[0]),
                ("longitudeCoordinate" ,struct.unpack('<d',bytes.fromhex(base16Data[30:46]))[0]),
        		("speedOverGround"     ,struct.unpack('<f',bytes.fromhex(base16Data[46:54]))[0]),
        ])

        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def AS7265X1LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 52 and len(base16Data) ==72) :
        sensorDictionary =  OrderedDict([
                ("dateTime"      ,str(dateTime)),
        		("channelA410nm" ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("channelB435nm" ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
        		("channelC460nm" ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
            	("channelD485nm" ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),                
        		("channelE510nm" ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
            	("channelF535nm" ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
        		("channelG560nm" ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
            	("channelH585nm" ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]),                
        		("channelR610nm" ,struct.unpack('<f',bytes.fromhex(base16Data[64:72]))[0]),
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;


def AS7265X2LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 53 and len(base16Data) ==72) :
        sensorDictionary =  OrderedDict([
                ("dateTime"      ,str(dateTime)),
        		("channelI645nm" ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("channelS680nm" ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
        		("channelJ705nm" ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
            	("channelT730nm" ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),                
        		("channelU760nm" ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
            	("channelV810nm" ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
        		("channelW860nm" ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
            	("channelK900nm" ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]),                
        		("channelL940nm" ,struct.unpack('<f',bytes.fromhex(base16Data[64:72]))[0]),
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;




def AS7265XLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    print(len(base16Data))
    if(framePort == 51 and len(base16Data) ==144) :
        sensorDictionary =  OrderedDict([
                ("dateTime"      ,str(dateTime)),
        		("channelA410nm" ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("channelB435nm" ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
        		("channelC460nm" ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
            	("channelD485nm" ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),                
        		("channelE510nm" ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
            	("channelF535nm" ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
        		("channelG560nm" ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
            	("channelH585nm" ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]),                
        		("channelR610nm" ,struct.unpack('<f',bytes.fromhex(base16Data[64:72]))[0]),
            	("channelI645nm" ,struct.unpack('<f',bytes.fromhex(base16Data[72:80]))[0]),
        		("channelS680nm" ,struct.unpack('<f',bytes.fromhex(base16Data[80:88]))[0]),
            	("channelJ705nm" ,struct.unpack('<f',bytes.fromhex(base16Data[88:96]))[0]),                
        		("channelT730nm" ,struct.unpack('<f',bytes.fromhex(base16Data[96:104]))[0]),
            	("channelU760nm" ,struct.unpack('<f',bytes.fromhex(base16Data[104:112]))[0]),
        		("channelV810nm" ,struct.unpack('<f',bytes.fromhex(base16Data[112:120]))[0]),
            	("channelW860nm" ,struct.unpack('<f',bytes.fromhex(base16Data[120:128]))[0]),                
        		("channelK900nm" ,struct.unpack('<f',bytes.fromhex(base16Data[128:136]))[0]),
            	("channelL940nm" ,struct.unpack('<f',bytes.fromhex(base16Data[136:144]))[0]),
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def BME688CNRLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):

    if(framePort == 25 and len(base16Data) ==56) :
        sensorDictionary =  OrderedDict([
                ("dateTime"    , str(dateTime)), 
        		("temperature" ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("humidity"    ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                ("pressure"    ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                ("vocAqi"      ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
            	("bvocEq"      ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
        		("gasEst"      ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]), 
            	("co2Eq"       ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
        ])

        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def IPS7100CNRLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 17 and len(base16Data) ==112) :
        sensorDictionary =  OrderedDict([
                ("dateTime" , str(dateTime)), 
        		("pc0_1"  ,struct.unpack('<L',bytes.fromhex(base16Data[0:8]))[0]),
            	("pc0_3"  ,struct.unpack('<L',bytes.fromhex(base16Data[8:16]))[0]),
                ("pc0_5"  ,struct.unpack('<L',bytes.fromhex(base16Data[16:24]))[0]),
                ("pc1_0"  ,struct.unpack('<L',bytes.fromhex(base16Data[24:32]))[0]),
            	("pc2_5"  ,struct.unpack('<L',bytes.fromhex(base16Data[32:40]))[0]),
        		("pc5_0"  ,struct.unpack('<L',bytes.fromhex(base16Data[40:48]))[0]), 
            	("pc10_0" ,struct.unpack('<L',bytes.fromhex(base16Data[48:56]))[0]),
        		("pm0_1"  ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]), 
            	("pm0_3"  ,struct.unpack('<f',bytes.fromhex(base16Data[64:72]))[0]),
                ("pm0_5"  ,struct.unpack('<f',bytes.fromhex(base16Data[72:80]))[0]),
                ("pm1_0"  ,struct.unpack('<f',bytes.fromhex(base16Data[80:88]))[0]),
            	("pm2_5"  ,struct.unpack('<f',bytes.fromhex(base16Data[88:96]))[0]),
        		("pm5_0"  ,struct.unpack('<f',bytes.fromhex(base16Data[96:104]))[0]), 
            	("pm10_0" ,struct.unpack('<f',bytes.fromhex(base16Data[104:112]))[0])
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def IPS7100LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 15 and len(base16Data) ==112) :
        sensorDictionary =  OrderedDict([
                ("dateTime" , str(dateTime)), 
        		("pc0_1"  ,struct.unpack('<L',bytes.fromhex(base16Data[0:8]))[0]),
            	("pc0_3"  ,struct.unpack('<L',bytes.fromhex(base16Data[8:16]))[0]),
                ("pc0_5"  ,struct.unpack('<L',bytes.fromhex(base16Data[16:24]))[0]),
                ("pc1_0"  ,struct.unpack('<L',bytes.fromhex(base16Data[24:32]))[0]),
            	("pc2_5"  ,struct.unpack('<L',bytes.fromhex(base16Data[32:40]))[0]),
        		("pc5_0"  ,struct.unpack('<L',bytes.fromhex(base16Data[40:48]))[0]), 
            	("pc10_0" ,struct.unpack('<L',bytes.fromhex(base16Data[48:56]))[0]),
        		("pm0_1"  ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]), 
            	("pm0_3"  ,struct.unpack('<f',bytes.fromhex(base16Data[64:72]))[0]),
                ("pm0_5"  ,struct.unpack('<f',bytes.fromhex(base16Data[72:80]))[0]),
                ("pm1_0"  ,struct.unpack('<f',bytes.fromhex(base16Data[80:88]))[0]),
            	("pm2_5"  ,struct.unpack('<f',bytes.fromhex(base16Data[88:96]))[0]),
        		("pm5_0"  ,struct.unpack('<f',bytes.fromhex(base16Data[96:104]))[0]), 
            	("pm10_0" ,struct.unpack('<f',bytes.fromhex(base16Data[104:112]))[0])
        ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def BME280LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 21 and len(base16Data) ==24):
        sensorDictionary =  OrderedDict([
                ("dateTime"    ,str(dateTime)), 
        		("Temperature" ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("Pressure"    ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                ("Humidity"    ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def SCD30LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 33 and len(base16Data) ==24):
        sensorDictionary =  OrderedDict([
                ("dateTime"    ,str(dateTime)), 
        		("CO2"         ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("Temperature" ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                ("Humidity"    ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def INA219MonoLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 4 and len(base16Data) ==40):
        sensorDictionary =  OrderedDict([
                ("dateTime"    ,str(dateTime)), 
        		("busVoltageBattery"   ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
                ("shuntVoltageSolar"   ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
            	("busVoltageSolar"     ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                ("currentSolar"        ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
                ("powerSolar"          ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def INA219DuoLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 3 and len(base16Data) ==64):
        sensorDictionary =  OrderedDict([
                ("dateTime"    ,str(dateTime)), 
        		("shuntVoltageBattery" ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("busVoltageBattery"   ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                ("currentBattery"      ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                ("powerBattery"        ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
	            ("shuntVoltageSolar"   ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
            	("busVoltageSolar"     ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
                ("currentSolar"        ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
                ("powerSolar"          ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]),
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;
def MGS001LoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 31 and len(base16Data) ==64):
        sensorDictionary =  OrderedDict([
                ("dateTime" ,str(dateTime)), 
        		("C2H5OH"   ,struct.unpack('<f',bytes.fromhex(base16Data[0:8]))[0]),
            	("C3H8"     ,struct.unpack('<f',bytes.fromhex(base16Data[8:16]))[0]),
                ("C4H10"    ,struct.unpack('<f',bytes.fromhex(base16Data[16:24]))[0]),
                ("CH4"      ,struct.unpack('<f',bytes.fromhex(base16Data[24:32]))[0]),
	            ("CO"       ,struct.unpack('<f',bytes.fromhex(base16Data[32:40]))[0]),
            	("H2"       ,struct.unpack('<f',bytes.fromhex(base16Data[40:48]))[0]),
                ("NH3"      ,struct.unpack('<f',bytes.fromhex(base16Data[48:56]))[0]),
                ("NO2"      ,struct.unpack('<f',bytes.fromhex(base16Data[56:64]))[0]),
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def GPGGALRLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 5 and len(base16Data) ==110):
        sensorDictionary =  OrderedDict([
                ("dateTime"   ,str(dateTime)), 
        		("Latitude"   ,struct.unpack('<d',bytes.fromhex(base16Data[0:16]))[0]),
            	("Longitude"  ,struct.unpack('<d',bytes.fromhex(base16Data[16:32]))[0]),
                ("Speed"      ,struct.unpack('<d',bytes.fromhex(base16Data[32:48]))[0]),
                ("Altitude"   ,struct.unpack('<d',bytes.fromhex(base16Data[48:64]))[0]),
	            ("Course"     ,struct.unpack('<d',bytes.fromhex(base16Data[64:80]))[0]),
            	("HDOP"       ,struct.unpack('<d',bytes.fromhex(base16Data[80:96]))[0]),# 42 bytes
                ("Year"       ,struct.unpack('<H',bytes.fromhex(base16Data[96:100]))[0]),# 2 bytes
                ("Month"      ,struct.unpack('<b',bytes.fromhex(base16Data[100:102]))[0]),
                ("Day"        ,struct.unpack('<b',bytes.fromhex(base16Data[102:104]))[0]),
                ("Hour"       ,struct.unpack('<b',bytes.fromhex(base16Data[104:106]))[0]),
                ("Minute"     ,struct.unpack('<b',bytes.fromhex(base16Data[106:108]))[0]),
                ("Second"     ,struct.unpack('<b',bytes.fromhex(base16Data[108:110]))[0]), #5 bytes 
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def PMLoRaWrite(dateTime,nodeID,sensorID,framePort,base16Data):
    if(framePort == 2 and len(base16Data) ==4):
        sensorDictionary =  OrderedDict([
                ("dateTime" ,str(dateTime)), 
        		("powerMode",struct.unpack('<b',bytes.fromhex(base16Data[0:2]))[0]),
          ])
        print(sensorDictionary)        
        loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary)
    return ;

def loRaWriteFinisher(nodeID,sensorID,dateTime,sensorDictionary):
    writePath = mSR.getWritePathMQTT(nodeID,sensorID,dateTime)
    exists    = mSR.directoryCheck(writePath)
    print(writePath)	
    mSR.writeCSV2(writePath,sensorDictionary,exists)
    mL.writeJSONLatestMQTT(sensorDictionary,nodeID,sensorID)
    return;

def loRaSummaryWrite(message,portInfo):

    nodeID = message.topic.split('/')[5]
    sensorPackage       =  decoder.decode(message.payload.decode("utf-8","ignore"))
    rxInfo              =  sensorPackage['rxInfo'][0]
    txInfo              =  sensorPackage['txInfo']
    loRaModulationInfo  =  txInfo['loRaModulationInfo']
    framePort           =  sensorPackage['fPort']
    # print(framePort)
    # print(portInfo)
    
    sensorID            =  getSensorFromPort(framePort,portInfo)
    dateTime            =  datetime.datetime.fromisoformat(sensorPackage['publishedAt'][0:26])
    base16Data          =  base64.b64decode(sensorPackage['data'].encode()).hex()
    gatewayID           =  base64.b64decode(rxInfo['gatewayID']).hex()

    sensorDictionary =  OrderedDict([
            ("dateTime"        , str(dateTime)),
            ("nodeID"          , nodeID),
            ("sensorID"        , sensorID),
            ("gatewayID"       , gatewayID),
            ("rssi"            , rxInfo["rssi"]),
            ("loRaSNR"         , rxInfo["loRaSNR"]),
            ("channel"         , rxInfo["channel"] ),
            ("rfChain"         , rxInfo["rfChain"] ),
            ("frequency"       , txInfo["frequency"]),
            ("bandwidth"       , loRaModulationInfo["bandwidth"]),
            ("spreadingFactor" , loRaModulationInfo["spreadingFactor"] ),
            ("codeRate"        , loRaModulationInfo["codeRate"] ),
            ("dataRate"        , sensorPackage['dr']),
            ("frameCounters"   , sensorPackage['fCnt']),
            ("framePort"       , framePort),
            ("base64Data"      , sensorPackage['data']),
            ("base16Data"      , base16Data),
            ("devAddr"         , sensorPackage['devAddr']),
            ("deviceAddDecoded", base64.b64decode(sensorPackage['devAddr'].encode()).hex())
        ])
    # print(sensorDictionary)

 
    # loRaWriteFinisher("LoRaNodes","Summary",dateTime,sensorDictionary)
    # loRaWriteFinisher(gatewayID,"Summary",dateTime,sensorDictionary)
    return dateTime,gatewayID,nodeID,sensorID,framePort,base16Data;

def getPortIndex(portIDIn,portIDs):
    indexOut = 0
    for portID in portIDs:
        if (portIDIn == portID['portID']):
            return indexOut; 
        indexOut = indexOut +1
    return -1;

def getSensorFromPort(framePort,portInfo):
    for currentPortIndex, currentPortID in enumerate(portInfo['portID']):
        if (framePort == currentPortID):
            # print("---------------")
            # print("Port ID:")
            # print(currentPortID)
            sensorID = portInfo['sensorID'][currentPortIndex]
            # print("Sensor ID:")
            # print(sensorID)
            return sensorID; 
    return "UnknownSensor";

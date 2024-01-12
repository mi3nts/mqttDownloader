# mqttDownloader
This repository contains firmware designed for downloading MQTT data.


## Instructions on how use 

### On Unix based systems
   
1. Obtain the following files from a Mints team member:
- Credentials file
- Node list file
- Certificates file

2. Create a virtual environment and activate it:
```
python3 -m venv mDVE
source mDVE/bin/activate
```

3. Clone mqttDownloader to a preferred location:
```
cd /home/teamlary/gitHubRepos
git clone https://github.com/mi3nts/mqttDownloader.git
```
4. Install dependancies
```
cd mqttDownloader
pip3 install -r requirements.txt
```
   
5. Implement the program:
```
cd firmware
python3 dataDownloader.py
```

6. Follow the prompts to provide the following information:
- Downloads location
- Location of the provided files (credentials, node list, certificates)


## Downloads data structure

The files will be downloaded on the following folder organization structure.
```
Chosed Downloads Folder
└── rawMqtt
    ├── 001e064a87a6
    │   ├── 2024
    │   │   └── 01
    │   │       └── 12
    │   │           ├── MINTS_001e064a87a6_AS7265X_2024_01_12.csv
    │   │           ├── MINTS_001e064a87a6_BME280V2_2024_01_12.csv
    │   │           ├── MINTS_001e064a87a6_GPSGPGGA2_2024_01_12.csv
    │   │           ├── MINTS_001e064a87a6_IPS7100_2024_01_12.csv
    │   │           ├── MINTS_001e064a87a6_RG15_2024_01_12.csv
    │   │           └── MINTS_001e064a87a6_SCD30V2_2024_01_12.csv
    │   ├── AS7265X.json
    │   ├── BME280V2.json
    │   ├── GPSGPGGA2.json
    │   ├── IPS7100.json
    │   ├── RG15.json
    │   └── SCD30V2.json
    ├── 2cf7f12032303bdd
    │   ├── 2024
    │   │   └── 01
    │   │       └── 12
    │   │           ├── MINTS_2cf7f12032303bdd_AS7265X_2024_01_12.csv
    │   │           ├── MINTS_2cf7f12032303bdd_BME688CNR_2024_01_12.csv
    │   │           ├── MINTS_2cf7f12032303bdd_GPGGAPL_2024_01_12.csv
    │   │           ├── MINTS_2cf7f12032303bdd_GPRMCPL_2024_01_12.csv
    │   │           ├── MINTS_2cf7f12032303bdd_IPS7100CNR_2024_01_12.csv
    │   │           └── MINTS_2cf7f12032303bdd_SCD30_2024_01_12.csv
    │   ├── AS7265X.json
    │   ├── BME688CNR.json
    │   ├── GPGGAPL.json
    │   ├── GPRMCPL.json
    │   ├── IPS7100CNR.json
    │   └── SCD30.json
```    
- Node: Each "node" represents a distinct entity or device.
   - Sensor Files: For each node, there is a collection of sensor files.
       - Daily .csv Files: Every sensor within a node generates a unique .csv file on a daily basis. This file contains data specific to that sensor for the given day.
       - Latest Sensor Values (.json): Additionally, the most recent sensor values for each sensor are stored in a .json file. This file captures the latest readings from all sensors within the node.

For Example
- rawMqtt: This is the root directory.
   - 001e064a87a6: This directory corresponds to a specific sensor node identified by the code "001e064a87a6."
      - 2024: Subdirectory for the year 2024.
         - 01: Subdirectory for the month of January.
           - 12: Subdirectory for the 12th day of the month.
              - MINTS_001e064a87a6_AS7265X_2024_01_12.csv: CSV file containing data from the AS7265X sensor for January 12, 2024.





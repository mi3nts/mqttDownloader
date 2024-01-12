# mqttDownloader
This repository contains firmware designed for downloading MQTT data.
   
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

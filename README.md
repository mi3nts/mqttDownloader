# mqttDownloader
This repository contains firmware designed for downloading MQTT data.
   
1. Obtain the following files from a Mints team member:
- Credentials file
- Node list file
- Certificates file

2. **Create a virtual environment:
```
python3 -m venv mDVE
```


3. Clone mqttDownloader to a preferred location:
```
cd /home/teamlary/gitHubRepos
git clone https://github.com/mi3nts/mqttDownloader.git
```

4. Implement the program:
```
cd mqttDownloader/firmware
python3 dataDownloader.py
```

5. Follow the prompts to provide the following information:
- Downloads location
- Location of the provided files (credentials, node list, certificates)

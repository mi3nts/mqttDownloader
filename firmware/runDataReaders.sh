#!/bin/sh

kill $(pgrep -f 'python3 DCDataDownloader.py')
sleep 1
python3 DCDataDownloader.py &

kill $(pgrep -f 'python3 LNDataDownloader.py')
sleep 1
python3 LNDataDownloader.py &



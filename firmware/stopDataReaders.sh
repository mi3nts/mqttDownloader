#!/bin/sh


kill $(pgrep -f 'DCDataReader.py')
sleep 1


kill $(pgrep -f 'LNDataReader.py')
sleep 1



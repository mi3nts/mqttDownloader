import os
import sys
import shutil
import datetime
from mintsXU4 import mintsSensorReader as mSR
from mintsXU4 import mintsDefinitions as mD

dataFolder    = mD.dataFolder
macAddress    = mD.macAddress


def main():
    dateStart      = datetime.date(2016, 12, 6)
    deleteDaysBack = 14
    dateEnd =  datetime.date.today() -  datetime.timedelta(deleteDaysBack)

    deleteDays = [dateStart + datetime.timedelta(days=x) for x in range((dateEnd-dateStart).days + 1)]

    for deleteDate in deleteDays:
        try:
            dirPath = os.path.normpath(getDeletePath(deleteDate))
            print("Deleting: "+ dirPath)
            if os.path.exists(dirPath):
                shutil.rmtree(dirPath)

        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))


def getDeletePath(deleteDate):
    # deleteDate =  datetime.datetime.now() -  datetime.timedelta(daysBefore)
    deletePath = dataFolder+"/"+macAddress+"/"+str(deleteDate.year).zfill(4)  + \
    "/" + str(deleteDate.month).zfill(2)+ "/"+str(deleteDate.day).zfill(2)
    # print(deletePath)
    return deletePath;


if __name__ == '__main__':
  main()

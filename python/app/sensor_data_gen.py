#!/usr/bin/python           

import os
import sys
from random import randint
import time
import socket  
import uuid
import threading
import Queue
from datetime import datetime
sys.path.append(os.path.abspath("../lib"))
from util import *

numSensors = int(sys.argv[1])	
duration = int(sys.argv[2])	
samplingInterval = float(sys.argv[3])
meanReading = int(sys.argv[4])	
normModeDuration = int(sys.argv[5])
devModeDuration = int(sys.argv[6])


numReading = int(duration / samplingInterval)

#print "numSensors=%d duration=%d samplingInterval=%f meanReading=%d numReading=%d" %(numSensors, duration, samplingInterval, meanReading, numReading)

sensorIds = []
exitMutexes = [False] * (numSensors)

for i in range(numSensors):
	sensorIds.append(genID(8))

# generate sensor reading and write to queue   		               
def sensorReader(id, sensorID, numReading, meanReading, samplingInterval, normModeDuration, devModeDuration, threadLock):
	mode = 0
	modeNumReading = getModeNumSamples(mode, samplingInterval, normModeDuration, devModeDuration)
	modeReadingCnt = 0
	#print "sensor %s mode %d numReading %d" %(sensorID,mode,modeNumReading)
		
	for i in range(numReading):
		if (modeReadingCnt < modeNumReading):
			reading = getReading(meanReading, mode)
			modeReadingCnt += 1
		else:
			mode = switchMode(mode)
			modeNumReading = getModeNumSamples(mode, samplingInterval, normModeDuration, devModeDuration)
			#print "sensor %s mode %d numReading %d" %(sensorID,mode,modeNumReading)
			modeReadingCnt = 0
			reading = getReading(meanReading, mode)
			modeReadingCnt += 1
		#print reading
		curTime = int(time.time() * 1000)	
		readRec = "%s,%d,%d" %(sensorID, curTime, reading)	
		threadLock.acquire(1) 	  		
		print readRec
		threadLock.release()
		time.sleep(samplingInterval)
   	exitMutexes[id] = True
		
		
def getReading(meanReading, mode):
	if mode == 0:
		reading = meanReading + randomNoise()
	elif mode == 1:
		reading = meanReading + 6 + randomNoise()
	else:
		reading = meanReading - 6 + randomNoise()
	
	return reading

def switchMode(mode):
	r = randint(0, 100)
	if (mode == 0):
		if (r < 60):
			newMode = 1
		else:
			newMode = -1
	elif (mode == 1):
		if (r < 90):
			newMode = 0
		else:
			newMode = -1
	else:
		if (r < 80):
			newMode = 0
		else:
			newMode = 1
	return newMode			

def getModeNumSamples(mode, samplingInterval, normModeDuration, devModeDuration):
	if (mode == 0):
		modeNumReading = int((normModeDuration + randint(-normModeDuration/2,normModeDuration/2)) / samplingInterval)
	else:
		modeNumReading = int((devModeDuration + randint(-devModeDuration/2, devModeDuration/2)) / samplingInterval)
	return 	modeNumReading
		

def randomNoise():
	r = randint(0, 100)	
	if r < 40:
		noise = 0
	elif r < 60:
		noise = 1
	elif r < 80:
		noise = -1
	elif r < 90:
		noise = 2
	else:
		noise = -2
	return noise	
	
	
####################################################################   

#sensor threads
threadLock = threading.Lock()
for i in range(numSensors):
	t = threading.Thread(target=sensorReader, args=(i, sensorIds[i], numReading, meanReading, 
		samplingInterval, normModeDuration, devModeDuration, threadLock))
	t.start()


#wait for threads to complete
while False in exitMutexes: 
	pass
#print "exiting"

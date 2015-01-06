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
numReading = int(duration / samplingInterval)

print "numSensors=%d duration=%d samplingInterval=%f meanReading=%d numReading=%d" %(numSensors, duration, samplingInterval, meanReading, numReading)

queue = Queue.Queue()
sensorIds = []
exitMutexes = [False] * (numSensors + 1)

for i in range(numSensors):
	sensorIds.append(genID(8))

def server(id, numReading, queue):
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
		host = socket.gethostname() 
		port = 9147                
		s.bind((host, port))     
		print "bound to %s %d" %(host, port)   

		s.listen(5)     
		done = False    
		readingCount = 1        
		while not done:
   			c, addr = s.accept()     
   			print 'got connection from ', addr
   			reading = queue.get(False)
   			c.send(reading)
   			#c.close() 
   			print "sent data:%s" %(reading)
   			readingCount += 1
   			if (readingCount == numReading):
   				done = True
   		s.close()
   		exitMutexes[id] = True
   	except socket.error:
   		print 'socket error in server'
    	sys.exit()

# generate sensor reading and write to queue   		               
def sensorReader(id, sensorID, numReading, meanReading, samplingInterval, queue):
	mode = 0
	modeNumReading = getModeNumSamples(mode, samplingInterval)
	modeReadingCnt = 0
	print "sensor %s mode %d numReading %d" %(sensorID,mode,modeNumReading)
		
	for i in range(numReading):
		if (modeReadingCnt < modeNumReading):
			reading = getReading(meanReading, mode)
			modeReadingCnt += 1
		else:
			mode = switchMode(mode)
			modeNumReading = getModeNumSamples(mode, samplingInterval)
			print "sensor %s mode %d numReading %d" %(sensorID,mode,modeNumReading)
			modeReadingCnt = 0
			reading = getReading(meanReading, mode)
			modeReadingCnt += 1
		#print reading
		curTime = int(time.time() * 1000)	
		readRec = "%s,%d,%d\n" %(sensorID, curTime, reading)	 	  		
		queue.put(readRec, False)
		time.sleep(samplingInterval)
		
		
def getReading(meanReading, mode):
	if mode == 0:
		reading = meanReading + randomNoise()
	elif mode == 1:
		reading = meanReading + 5 + randomNoise()
	else:
		reading = meanReading -5 + randomNoise()
	
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

def getModeNumSamples(mode, samplingInterval):
	if (mode == 0):
		modeNumReading = int((20 + randint(0,20)) / samplingInterval)
	else:
		modeNumReading = int((10 + randint(0,10)) / samplingInterval)
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

# server thread		
t = threading.Thread(target=server, args=(0, numReading * len(sensorIds), queue))
t.start()

#sensor threads
for i in range(numSensors):
	t = threading.Thread(target=sensorReader, args=(i+1, sensorIds[i], numReading, meanReading, samplingInterval, queue))
	t.start()


#wait for threads to complete
while False in exitMutexes: 
	pass
print "exiting"

#!/usr/bin/python

import os
import sys
from random import randint
import time
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

op = sys.argv[1]
secInHour = 60 * 60
secInDay = 24 * secInHour
secInWeek = 7 * secInDay
secInQuarterDay = 6 * secInHour

if op == "usage":
	numDays = int(sys.argv[2])
	sampIntv = int(sys.argv[3])
	numMeters = int(sys.argv[4])
	usageDistr = []
	usageDistr.append((GaussianRejectSampler(.012,.002), GaussianRejectSampler(.018,.004), \
	GaussianRejectSampler(.014,.003), GaussianRejectSampler(.020,.001)))
	usageDistr.append((GaussianRejectSampler(.010,.003), GaussianRejectSampler(.019,.002), \
	GaussianRejectSampler(.022,.003), GaussianRejectSampler(.016,.002)))

	meterList = []
	for i in range(numMeters):
		meterList.append(genID(10))

	curTime = int(time.time())
	pastTime = curTime - (numDays + 1) * secInDay
	sampTime = pastTime

	while(sampTime < curTime):
		secIntoDay = sampTime % secInDay
		quartIntoday = secIntoDay / secInQuarterDay
		for mId in meterList:
			h = hash(mId)
			if (h < 0):
				h = -h
			cl = h % 2
			#print "%d, %d" %(cl, quartIntoday)
			distr = usageDistr[cl][quartIntoday]
			usage = distr.sample()
			st = sampTime + randint(-2,2)
			print "%s,%d,%.3f" %(mId, st, usage)
			
		sampTime = sampTime + sampIntv		
			
			
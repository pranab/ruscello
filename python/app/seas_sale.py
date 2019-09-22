#!/usr/bin/python

# avenir-python: Machine Learning
# Author: Pranab Ghosh
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You may
# obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

import os
import sys
from random import randint
import time
from datetime import datetime
sys.path.append(os.path.abspath("../lib"))
from util import *
from mlutil import *
from sampler import *


# generate product sales data with trend, weekly cycle, daily cycle and gaussian remainder
if __name__ == "__main__":
	op = sys.argv[1]

	#generate sales stat
	if op == "stat":
		bTrend = 0.05
		bWeekSeas = [1500,1300,1200,1100,1300,1600,1800]
		hours =      [0,1,2,3,4,5,6,7, 8, 9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
		bDailySeas = [9,8,6,4,3,2,4,7,10,14,17,28,40,48,38,33,38,42,31,42,56,44,26,16]
		bRemStdDev = 50
		
		nProd = int(sys.argv[2])
		for i in range(nProd):
			remStdDev = preturbScalar(bRemStdDev, .40)			
			trend = preturbScalar(bTrend, .20)
			weekSeas = toIntList(preturbVector(bWeekSeas, .20))
			weekSeas = toStrFromList(weekSeas, 0)
			dailySeas = toIntList(preturbVector(bDailySeas, .30))
			dailySeas = toStrFromList(dailySeas, 0)
			id = genID(10)
			print "%s,%.3f,%.6f,%s,%s" %(id,remStdDev,trend,weekSeas,dailySeas)

	#generate sales data		
	elif op == "gen":
		statFile = sys.argv[2]
		numDays = int(sys.argv[3])

		prStat = dict()
		for rec in fileRecGen(statFile, ","):
			offset = 0
			id = rec[offset]
			
			offset += 1
			remStdDev = float(rec[offset])
			sampler = GaussianRejectSampler(0,remStdDev)
			
			offset += 1
			trend = float(rec[offset])
			
			offset += 1
			weekSeas = rec[offset:offset+7]
			weekSeas = toIntList(weekSeas)
			
			offset += 7
			dailySeas = rec[offset:]
			dailySeas = toIntList(dailySeas)
			
			prStat[id] = (trend, weekSeas, dailySeas, sampler)
		
		numHours = 	24 * numDays 
		(cTime, pTime) = pastTime(numDays)
		pTime = hourAlign(pTime)
		sTime = pTime
		sIntv = secInHour
		
		ids = prStat.keys()
		for i in range(numHours):
			for id in ids:
				(trend, weekSeas, dailySeas, sampler) = prStat[id]
				#print sTime, trend, weekSeas, dailySeas, sampler
				trC = i * trend
				dow = dayOfWeek(sTime)
				wkSeasC = weekSeas[dow]
				hod = hourOfDay(sTime)
				daySeasC = dailySeas[hod]
				remainC = sampler.sample()
				total = int(trC + wkSeasC + daySeasC + remainC)
				print "%s,%d,%d" %(id,sTime,total)  
			sTime += sIntv
		
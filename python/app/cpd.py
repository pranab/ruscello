#!/usr/local/bin/python3

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

# Package imports
import os
import sys
import random
import jprops
import numpy as np
from scipy.stats import ks_2samp
sys.path.append(os.path.abspath("../lib"))
sys.path.append(os.path.abspath("../mlextra"))
from util import *
from optpopu import *
from optsolo import *
from sampler import *

class ChangePointDetector(object):
	"""
	optimize with evolutionary search
	"""
	def __init__(self, filePath, col):
		"""
		intialize
		"""
		data = getFileColumnAsFloat(filePath ,col,  ",")
		self.data = np.array(data)
		self.size = len(self.data)
		
	def isValid(self, args):
		args.sort()
		prev = 0
		status = True
		for i in range(len(args)):
			if i == 0:
				slen = args[i]
			else:
				slen = args[i] - args[i-1]
			status = slen > 20
			if not status:
				break
				
		if status:
			slen = self.size - args[-1]
			status = slen > 20
		return status

	def evaluate(self, args):
		"""
		"""
		prev = 0
		costs = list()
		for i in range(len(args)):
			if i == 0:
				d1 = self.data[:args[i]]
				d2 = self.data[args[i]:args[i+1]]
			elif i == len(args) - 1:
				d1 = self.data[args[i-1]:args[i]]
				d2 = self.data[args[i]:]
			else:	
				d1 = self.data[args[i-1]:args[i]]
				d2 = self.data[args[i]:args[i+1]]
				
			result = ks_2samp(d1, d2)
			st = result.statistic
			costs.append(1.0 - st)
		return mean(costs)
			
def createOptimizer(name, configFile, domain):
	"""
	creates optimizer
	"""
	if name == "eo":
		optimizer = EvolutionaryOptimizer(configFile, domain)
	elif name == "ga":
		optimizer = GeneticAlgorithmOptimizer(configFile, domain)
	elif name == "sa":
		optimizer = SimulatedAnnealing(configFile, domain)
	else:
		raise ValueError("invalid optimizer name")
	return optimizer
		
if __name__ == "__main__":
	assert len(sys.argv) == 5, "wrong command line args"
	filePath = sys.argv[1]
	col = int(sys.argv[2])
	optName = sys.argv[3]
	optConfFile = sys.argv[4]
	
	detector = ChangePointDetector(filePath, col)
	optimizer = createOptimizer(optName, optConfFile, detector)
	optimizer.run()
	print("best soln found")
	print(optimizer.getBest())
	if optimizer.trackingOn:
		print("soln history")
		print(str(optimizer.tracker))

			
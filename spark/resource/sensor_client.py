#!/usr/bin/python           

import os
import sys
from random import randint
import time
import socket  
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *

def client():
	try:
		print "socket created" 
		host = 'Pranab-Ghoshs-MacBook-Pro.local'
		port = 9147

		numCalls = int(sys.argv[1])
		for i in range(numCalls):	
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)     
			s.connect((host, port))
			print "connected"
			data = s.recv(1024)
			print "got:%s" %(data)
			s.close()
			print "closed"
	except socket.error, msg:
		print "socket error in client " + str(msg[0]) + "  " + str(msg[1])
		sys.exit()
	finally:
		#s.close()
		print "done"
	
client()	
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

if len(sys.argv) < 4:
	print "usage: ./event_clust.py <num_hosts>  <num_error_types> <num_events>"
	sys.exit()

num_hosts = int(sys.argv[1])
num_error_types = int(sys.argv[2])
num_events = int(sys.argv[3])

def sel_host_app():
	sel = randint(0, len(hosts)-1)
	host = hosts[sel]
	app = apps[sel]
	return (host,app)

hosts = []
apps = []
for i in range(0, num_hosts):
	hosts.append(genID(8))
	apps.append(genID(6))
	
# error codes for each app
app_errors = {}
for a in apps:
	num_errors = num_error_types + randint(1, 50)
	errors = []
	error_code = random.randint(1, 6) * 10000
	for i in range(num_errors):
		errors.append(error_code)
		error_code += 1
	app_errors[a] = errors

#generate events
time_ms = curTimeMs() - 30 * 24 * 60 * 60 * 1000
av_cluster_gap = num_events / 10;
next_cluster_gap = av_cluster_gap / 2 + randint(1, av_cluster_gap)
inter_cluster_counter = 0
cluster_mode = False
clust_event_interval_base = 400
clust_event_interval_dev = 200
event_interval_base = 10000
event_interval_dev = 30000

for i in range(num_events):
	if cluster_mode:
		time_ms += clust_event_interval_base + randint(1, clust_event_interval_dev)
		print "%s,%s,%d,%d" %(clust_host, clust_app, cluster_event, time_ms)
		cluster_progress_counter += 1
		
		# non cluster event
		if randint(0, 100) < 10:
			(host, app) = sel_host_app()
			event = selectRandomFromList(app_errors[app])
			time_ms += randint(100,1000)
			print "%s,%s,%d,%d" %(host, app, event, time_ms)
		
		# cluster end
		if (cluster_progress_counter == cluster_size):
			#print "ending cluster"
			cluster_mode = False
			inter_cluster_counter = 0
			next_cluster_gap = av_cluster_gap / 2 + randint(1, av_cluster_gap)
	else:
		(host, app) = sel_host_app()
		event = selectRandomFromList(app_errors[app])
		time_ms += event_interval_base + randint(1, event_interval_dev)
		print "%s,%s,%d,%d" %(host, app, event, time_ms)
		inter_cluster_counter += 1

		# cluster start
		if inter_cluster_counter == next_cluster_gap:
			inter_cluster_counter = 0
			#print "starting cluster"
			cluster_mode = True
			cluster_size = 20 + random.randint(1, 20)
			cluster_progress_counter = 0
			(clust_host, clust_app) = sel_host_app()
			cluster_event = selectRandomFromList(app_errors[clust_app])
			
		
			
	

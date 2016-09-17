#!/usr/bin/python

import os
import sys
from random import randint
import time
import uuid
import threading
from thread import *
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *
from sockserv import *

if len(sys.argv) < 5:
	print "usage: ./event_clust.py <num_hosts>  <num_error_types> <num_events> <port>"
	sys.exit()

num_hosts = int(sys.argv[1])
num_error_types = int(sys.argv[2])
num_events = int(sys.argv[3])
port = int(sys.argv[4])

mlock = threading.Lock()
messages = []

def sel_host_app(hosts,apps):
	sel = randint(0, len(hosts)-1)
	host = hosts[sel]
	app = apps[sel]
	return (host,app)

def gen_data(num_hosts,num_error_types,num_events,messages,mlock):
	apps = []
	hosts = []
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
	av_cluster_gap = num_events / 20;
	next_cluster_gap = av_cluster_gap / 2 + randint(1, av_cluster_gap)
	inter_cluster_counter = 0
	cluster_mode = False
	clust_event_interval_base = 200
	clust_event_interval_dev = 160
	event_interval_base = 1000
	event_interval_dev = 1000

	for i in range(num_events):
		if cluster_mode:
			delta = clust_event_interval_base + randint(1, clust_event_interval_dev)
			#print delta
			time.sleep(float(delta) / 1000)
			time_ms += delta
			msg = "%s,%s,%d,%d" %(clust_host, clust_app, cluster_event, time_ms)
			with mlock:
				messages.append(msg)
			
			cluster_progress_counter += 1
		
			# non cluster event
			if randint(0, 100) < 10:
				(host, app) = sel_host_app(hosts,apps)
				event = selectRandomFromList(app_errors[app])
				time_ms += randint(100,1000)
				msg =  "%s,%s,%d,%d" %(host, app, event, time_ms)
				with mlock:
					messages.append(msg)
			
			# cluster end
			if (cluster_progress_counter == cluster_size):
				#print "ending cluster"
				cluster_mode = False
				inter_cluster_counter = 0
				next_cluster_gap = av_cluster_gap / 2 + randint(1, av_cluster_gap)
		else:
			(host, app) = sel_host_app(hosts,apps)
			event = selectRandomFromList(app_errors[app])
			delta = event_interval_base + randint(1, event_interval_dev)
			#print delta
			time.sleep(float(delta) / 1000)
			time_ms += delta
			msg =  "%s,%s,%d,%d" %(host, app, event, time_ms)
			with mlock:
				messages.append(msg)
			inter_cluster_counter += 1

			# cluster start
			if inter_cluster_counter == next_cluster_gap:
				inter_cluster_counter = 0
				#print "starting cluster"
				cluster_mode = True
				cluster_size = 30 + random.randint(1, 20)
				cluster_progress_counter = 0
				(clust_host, clust_app) = sel_host_app(hosts,apps)
				cluster_event = selectRandomFromList(app_errors[clust_app])
			
#### main ###		
start_new_thread(gen_data ,(num_hosts,num_error_types,num_events,messages,mlock, ))
print "started data generation thread"

print "starting server"
sock = create_socket(port)
sock.listen(10)
print 'socket now listening'
	
while True:
    #wait to accept a connection - blocking call
	conn, addr = sock.accept()
	print 'connected with ' + addr[0] + ':' + str(addr[1])
     
	start_new_thread(client_connection ,(conn,messages,mlock, ))
 
sock.close()


	

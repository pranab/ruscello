#!/usr/bin/python

#Simple socket server using threads

 
import socket
import sys
from thread import *
 

def create_socket(port): 
	host = ''
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	print 'socket created'
 
	#Bind socket to local host and port
	try:
		sock.bind((host, port))
	except socket.error as msg:
		print 'bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
		sys.exit()
     
	print 'socket bind complete'
	return sock

#Function for handling connections. This will be used to create threads
def client_connection(conn, messages, mlock):
    #Sending message to connected client
    with mlock:
    	size = len(messages)
    	if size > 0:
    		msg = '\n'.join(messages)
    		del messages[:]
    		conn.send(msg) 
    		print "num of messages sent: %d" %(size)
    conn.close()

	
def socket_listen(sock, messages, mlock):
	#Start listening on socket
	sock.listen(10)
	print 'socket now listening'
	
	while True:
    	#wait to accept a connection - blocking call
		conn, addr = sock.accept()
    	print 'connected with ' + addr[0] + ':' + str(addr[1])
     
    	#start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    	start_new_thread(client ,(conn,messages,mlock, ))
 
	sock.close()
	

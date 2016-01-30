#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Ignacio Gilbaja
# date: 2016-01-12
# mail: jose-ignacio.gilbaja@deimos-space.com
# version: 1.1

import pika
from multiprocessing import Process, Queue, Value
import time
import os
from random import uniform
from Queue import Empty
import multiprocessing
import datetime
import MySQLdb
import random
from random import choice
from string import ascii_uppercase



BROKER = "172.26.30.221"
QUEUE = "TestQUEUE"
ProducerCount = 10

MsgCount = 10000

TIME_WAIT = 0.1

def genRandomMsg():
	return str(int(time.time())*1000.0) + '---' + ''.join(choice(ascii_uppercase) for i in range(100))
 
def worker(queue, brokerConnection, channel):
	while True:
		try:
			msg = queue.get()
			#print 'Process %d --> Content read: %s' % (os.getpid(), str(content))
			channel.queue_declare(queue=QUEUE, durable=True)
			channel.basic_publish(exchange='', routing_key=QUEUE, body=msg, properties=pika.BasicProperties(delivery_mode = 2, ))
			print "Process %d sent: %s" % (os.getpid(), msg)
			time.sleep(TIME_WAIT)
		except Exception, error:
			print 'Error sendind data: %s' % (error)
			time.sleep(TIME_WAIT)
		if queue.qsize() == 0:
			print "Queue Msg is empty!!"
			brokerConnection.close
			break
     
def main():
	
	queue = Queue()
	print "Generating random messages and saving them at local queue...."
	for i in range (1,MsgCount+1):
		timestamp = int(time.time())
		msg = genRandomMsg()
		queue.put(msg)
	print "%d messages has been saved at local queue!!" % (MsgCount+1)
	
	
	processes = []
	print "Starting producers..."
	for i in range(ProducerCount):
		try:
			brokerConnection = pika.BlockingConnection(pika.ConnectionParameters(host=BROKER))
			channel = brokerConnection.channel()
		except Exception, error:
			print "Problem connecting to Broker: " + str(error)
		processes.append(Process(target=worker, args=(queue, brokerConnection, channel, )))
		processes[i].start()
		print "Process %d launched." % (i + 1)
	for process in processes:
		process.join()
  
  
if __name__ == '__main__':
	main()

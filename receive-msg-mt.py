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

MsgCount = 1000000

TIME_WAIT = 0.1

def callback(ch, method, properties, body):
    time.sleep(TIME_WAIT)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print "read: " + body

 
def worker(brokerConnection, channel):
	while True:
		try:
			#print 'Process %d --> Content read: %s' % (os.getpid(), str(content))
			channel.queue_declare(queue=QUEUE, durable=True)
			channel.basic_qos(prefetch_count=1)
			channel.basic_consume(callback, queue='TestQUEUE')
			print "Producer with PID %d started!!" % (os.getpid())
			time.sleep(TIME_WAIT)
		except Exception, error:
			print 'Error reading data: %s' % (error)
			time.sleep(TIME_WAIT)
     
def main():
	
	processes = []
	print "Starting producers..."
	for i in range(ProducerCount):
		try:
			brokerConnection = pika.BlockingConnection(pika.ConnectionParameters(host=BROKER))
			channel = brokerConnection.channel()
		except Exception, error:
			print "Problem connecting to Broker: " + str(error)
		processes.append(Process(target=worker, args=( brokerConnection, channel, )))
		processes[i].start()
		print "Process %d launched." % (i + 1)
	for process in processes:
		process.join()
  
  
if __name__ == '__main__':
	main()

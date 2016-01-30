#!/usr/bin/env python
import pika
import sys
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Mensaje"
messageToSent = time.ctime() + ": " + message 

for i in range (10):
	channel.basic_publish(exchange='',
                      routing_key='task_queue',
                      body=messageToSent + " " + str(i),
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
	print(" [x] Sent %r" % messageToSent + str(i))
connection.close()
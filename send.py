#!/usr/bin/env python
import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

for i in range (10):
	mensaje = time.ctime() + ': Mensaje ' + str(i) 
	channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=mensaje,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
	print(" [x] Sent : " + mensaje)

connection.close()
#!/usr/bin/env python
import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='AIS')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(2)

channel.basic_consume(callback,
                      queue='AIS',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

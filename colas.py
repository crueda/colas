from multiprocessing import Process, Queue, Value
import time
import os
from random import uniform
from Queue import Empty
import multiprocessing
import datetime
import MySQLdb
 
WORKER_NUMBER = 5
TIME_WAIT = 0.01
ITERATIONS = 100000

DB_IP = "172.26.30.29"
DB_NAME = "correos"
DB_USER = "root"
DB_PASSWORD = "dat1234"

 
def worker(queue, dbConnection):
	read = 0
	totalSpent = 0
	while True:
		try:
			content = queue.get()
			lon = str(content[0])
			lat = str(content[1])
			#print 'Process %d --> Content read: %s' % (os.getpid(), str(content))
			query = """SELECT OGR_FID, (st_distance_sphere(SHAPE, POINT(%s, %s))) as distance FROM routes ORDER BY distance limit 1""" % (lon,lat,)
			cursor = dbConnection.cursor()
			initialTime = datetime.datetime.now()
			cursor.execute(query)
			result = cursor.fetchall()
			endTime = datetime.datetime.now()
			timeSpent = ((endTime-initialTime)*1000).seconds
			read +=1
			totalSpent = totalSpent + timeSpent
			print "Process %d: distance to [%s,%s] result: %s -----> time: %s" % (os.getpid(), lon, lat, str(result[0][1]), str(timeSpent))
			time.sleep(TIME_WAIT)
		except Exception, error:
			print 'Error while reading Queue: %s' % (error)
			time.sleep(TIME_WAIT)
		if queue.qsize() ==0:
			average = totalSpent/read
			print "Queue is empty!! Total reads for process %d: %d. Average: %d miliseconds" % (os.getpid(),read, average)
			cursor.close
			dbConnection.close
			break
     
def main():
	
	queue = Queue()
	for i in range (1,ITERATIONS+1):
		lon = uniform(-180.00,180.00)
		lat = uniform(-90.00, 90.00)
		coordinate = [lon,lat]
		queue.put(coordinate)
	
	processes = []
	print "Starting processes..."
	for i in range(WORKER_NUMBER):
		try:
			dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		except Exception, error:
			print "Problem connecting to DB: " + str(error)
		processes.append(Process(target=worker, args=(queue, dbConnection, )))
		processes[i].start()
		#print "Process %d launched." % (i + 1)
	for process in processes:
		process.join()
  
  
if __name__ == '__main__':
	main()

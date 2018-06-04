import os
import re
import csv
from datetime import datetime, date, time, timedelta

file = open(os.curdir+'/input/inactivity_period.txt','r')
inactivity_period =  int(file.read())
file.close()

print('inactivity_period=',inactivity_period)

logfile = open(os.curdir+'/input/log.csv','r')
reader = csv.reader(logfile)
headers = next(reader)

ip_index = headers.index('ip')
date_index = headers.index('date')
time_index = headers.index('time')
cik_index = headers.index('cik')
accession_index = headers.index('accession')
extention_index = headers.index('extention')



data = {}
counter_logfileline = 0

for row in reader:

	counter_logfileline += 1

	time_now = datetime.strptime(row[time_index], '%H:%M:%S')
	#print('now, time is = ',time_now)

	if counter_logfileline == 1:
		time_start = time_now

	time_diff = time_now - time_start
	#isinstance(time_diff, timedelta)
	time_diff_sec = time_diff.total_seconds()
	if time_diff_sec > inactivity_period:
		print('session is detected!')
	 
	key = (row[ip_index],(row[date_index],row[time_index]))
	if key in data:
		data[key] += 1 
	else:
		data[key] = 1

# 	removing item from dic: del faster than pop but pop has a second argument for avoiding exception errot
#	del data[(row[ip_index],(row[date_index],row[time_index]))] 
#	data.pop((row[ip_index],(row[date_index],row[time_index])),0)

#	print('ip=',row[ip_index])
#	print('date=',row[date_index])
#	print('time=',row[time_index])
#	print('cik=',row[cik_index])
#	print('accession=',row[accession_index])
#	print('extention=',row[extention_index])
#	print('')

#print('number of lines in log file = ',counter_logfileline)
#print(headers)
print(data)
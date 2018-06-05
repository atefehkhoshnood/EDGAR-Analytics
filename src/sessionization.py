import os
import re
import csv
from datetime import datetime, date, time, timedelta

def is_ip_repeated(ip,data_dict):
	i = 0
	count_webpage_request = 0

	for key in data_dict:
		if ip in key:
			count_webpage_request += data_dict[key]
			i +=1
	if i == 1:
		return (i,count_webpage_request)
	else:
		return (i,count_webpage_request)


outputfile = open(os.curdir+'/output/sessionization.txt','w')

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
		time_past = time_now

	if time_past != time_now:
		data_maybe_inactive = []
		data_maybe_inactive_repeated = {}
		data_inactive = []


		for key in data:
			time_item = datetime.strptime(key[1][1],'%H:%M:%S')
			time_diff = time_now - time_item
			time_diff_sec = time_diff.total_seconds()
			if time_diff_sec > inactivity_period:
				data_maybe_inactive.append(key)
				print('time difference is=',time_diff_sec)

		for key in data_maybe_inactive:
			ip_item = key[0]
			i = 0
			for k in data:
				if ip_item in k:
					i += 1
			if i == 1:
				outputfile.write(key[0]+','+key[1][0]+' '+key[1][1]+','+key[1][0]+' '+key[1][1]+',1,'+str(data[key])+'\n')
				data_inactive.append(key)
				print('session is detected!')
			else:
				data_maybe_inactive_repeated[key]=i

		for key in data_maybe_inactive_repeated:
			ip_item = key[0]
			j = 0
			for k in data_maybe_inactive_repeated:
				if ip_item in k:
					j += 1
			if j == data_maybe_inactive_repeated[key]:
				count_webpage_request = 0
				i = 0
				for k in data:
					if k[0] == ip_item:
						i +=1
						count_webpage_request += data[k]
						if i == 1:
							time_start_0 = k[1][0]
							time_start_1 = k[1][1]
							time_start = datetime.strptime(k[1][1],'%H:%M:%S')
						if i == j:
							time_end_0 = k[1][0]
							time_end_1 = k[1][1]
							time_end = datetime.strptime(k[1][1],'%H:%M:%S')
				time_diff = time_end - time_start
				time_diff_sec = time_diff.total_seconds() + 1
				outputfile.write(ip_item+','+time_start_0+' '+time_start_1+','+time_end_0+' '+time_end_1+','+str(int(time_diff_sec))+','+str(count_webpage_request)+'\n')
				data_inactive.append(key)
				print('session is detected!')


		for key in data_inactive:
			data.pop(key,0)

	key = (row[ip_index],(row[date_index],row[time_index]))
	if key in data:
		data[key] += 1 
	else:
		data[key] = 1


data_repeated = {}
for key in data:
	ip_item = key[0]
	print(ip_item)
	no_repeated, count_webpage_request = is_ip_repeated(ip_item,data)
	if no_repeated != 1 and ip_item not in data_repeated:
			data_repeated[ip_item] = (key[1][1],no_repeated, count_webpage_request)
print('this is data repeated')
print(data_repeated)
print('\n')
for key in data:
	ip_item = key[0]
	if (ip_item in data_repeated) and (data_repeated[ip_item][0] == key[1][1]):
		i = 0
		date_item, no_repeated, count_webpage_request = data_repeated[ip_item] 
		for k in data:
			if k[0] == ip_item:
				i +=1
				if i == 1:
					time_start_0 = k[1][0]
					time_start_1 = k[1][1]
					time_start = datetime.strptime(k[1][1],'%H:%M:%S')
				if i == no_repeated:
					time_end_0 = k[1][0]
					time_end_1 = k[1][1]
					time_end = datetime.strptime(k[1][1],'%H:%M:%S')
		time_diff = time_end - time_start
		time_diff_sec = time_diff.total_seconds() + 1

		outputfile.write(ip_item+','+time_start_0+' '+time_start_1+','+time_end_0+' '+time_end_1+','+str(int(time_diff_sec))+','+str(count_webpage_request)+'\n')
	elif (ip_item not in data_repeated):
		outputfile.write(key[0]+','+key[1][0]+' '+key[1][1]+','+key[1][0]+' '+key[1][1]+',1,'+str(data[key])+'\n')
# 	removing item from dic: del faster than pop but pop has a second argument for avoiding exception errot
#	del data[(row[ip_index],(row[date_index],row[time_index]))] 
#	data.pop((row[ip_index],(row[date_index],row[time_index])),0)


#print('number of lines in log file = ',counter_logfileline)
#print(headers)
outputfile.close()
print(data)
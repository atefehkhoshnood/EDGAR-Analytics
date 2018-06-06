import os
import csv
from datetime import datetime, date, time, timedelta
from collections import Counter


start_t = datetime.now()


def is_ip_repeated_dic(ip,data_dict):
	i = 0
	count_webpage_request = 0
	for key in data_dict:
		if ip in key:
			count_webpage_request += data_dict[key]
			i +=1
	return (i,count_webpage_request)

def is_ip_repeated_list(ip,data_list):
	#i = data_list.count(ip)
	i = 0
	for key in data_list:
		if ip in key:
			i +=1
	return i

def main():

# creating output file
# openning inputfiles and check if they are not empty
	outputfile = open(os.curdir+'/output/sessionization.txt','w')
	if os.path.getsize(os.curdir+'/input/inactivity_period.txt')>0 and os.path.getsize(os.curdir+'/input/log.csv')>0:
		inactivity_periodfile = open(os.curdir+'/input/inactivity_period.txt','r')
		logfile = open(os.curdir+'/input/log.csv','r')
	else:
		print('one or both input files are empty.')
		return False
# reading the inactivity period from the file
	inactivity_period =  int(inactivity_periodfile.read())
	inactivity_periodfile.close()
# reading header of log file	
	logfile_content = csv.reader(logfile)
	headers = next(logfile_content)
# determining order of fields in log file's row
	ip_index = headers.index('ip')
	date_index = headers.index('date')
	time_index = headers.index('time')
	cik_index = headers.index('cik')
	accession_index = headers.index('accession')
	extention_index = headers.index('extention')

	data = {}
	counter_logfileline = 0
# reading log file line by line until end of file is reached while:
# - storing data such as ip, date, time, and number of visits in a dictinary structure 
# - calculating sessions using inactivity period and writing coresponding ip and other info in output file
# - building a list of inactive ips that should be deleted
# - deleting inactive dictionary keys
	for row in logfile_content:

		counter_logfileline += 1

		time_now = datetime.strptime(row[time_index], '%H:%M:%S')

		if counter_logfileline == 1:
			time_past = time_now

		if time_past != time_now:
			data_maybe_inactive = []
			data_written = []
			data_inactive = []
			for key in data:
				time_item = datetime.strptime(key[1][1],'%H:%M:%S')
				time_diff = time_now - time_item
				time_diff_sec = time_diff.total_seconds()
				if time_diff_sec > inactivity_period:
					data_maybe_inactive.append(key)
			for key in data_maybe_inactive:
				ip_item = key[0]
				i = 0
				no_repeated, count_webpage_request = is_ip_repeated_dic(ip_item,data)
				no_repeated_list = is_ip_repeated_list(ip_item,data_maybe_inactive)

				if no_repeated == 1:
					outputfile.write(key[0]+','+key[1][0]+' '+key[1][1]+','+key[1][0]+' '+key[1][1]+',1,'+str(count_webpage_request)+'\n')
					data_inactive.append(key)
				elif (no_repeated != 1) and (no_repeated_list == no_repeated) and (ip_item not in data_written):
					i = 0
					for k in data:
						if k[0] == ip_item:
							i +=1
							if i == 1:
								time_start_0, time_start_1 = k[1]
								time_start = datetime.strptime(k[1][1],'%H:%M:%S')
							if i == no_repeated_list:
								time_end_0, time_end_1 = k[1]
								time_end = datetime.strptime(k[1][1],'%H:%M:%S')
					time_diff = time_end - time_start
					time_diff_sec = time_diff.total_seconds() + 1
					outputfile.write(ip_item+','+time_start_0+' '+time_start_1+','+time_end_0+' '+time_end_1+','+str(int(time_diff_sec))+','+str(count_webpage_request)+'\n')
					data_written.append(ip_item)
					data_inactive.append(key)

			for key in data_inactive:
				data.pop(key,0)
		key = (row[ip_index],(row[date_index],row[time_index]))
		if key in data:
			data[key] += 1 
		else:
			data[key] = 1

# analysing what is left in dictionary, because end of log file is reached
	data_repeated = {}
	for key in data:
		ip_item = key[0]
		no_repeated, count_webpage_request = is_ip_repeated_dic(ip_item,data)
		if no_repeated != 1 and ip_item not in data_repeated:
				data_repeated[ip_item] = (key[1][1],no_repeated, count_webpage_request)
	for key in data:
		ip_item = key[0]
		if (ip_item in data_repeated) and (data_repeated[ip_item][0] == key[1][1]):
			i = 0
			date_item, no_repeated, count_webpage_request = data_repeated[ip_item] 
			for k in data:
				if k[0] == ip_item:
					i +=1
					if i == 1:
						time_start_0, time_start_1 = k[1]
						time_start = datetime.strptime(k[1][1],'%H:%M:%S')
					if i == no_repeated:
						time_end_0, time_end_1 = k[1]
						time_end = datetime.strptime(k[1][1],'%H:%M:%S')
			time_diff = time_end - time_start
			time_diff_sec = time_diff.total_seconds() + 1
			outputfile.write(ip_item+','+time_start_0+' '+time_start_1+','+time_end_0+' '+time_end_1+','+str(int(time_diff_sec))+','+str(count_webpage_request)+'\n')
		elif (ip_item not in data_repeated):
			outputfile.write(key[0]+','+key[1][0]+' '+key[1][1]+','+key[1][0]+' '+key[1][1]+',1,'+str(data[key])+'\n')

	outputfile.close()
	end_t = datetime.now()
	print ('Job is done in '+ str(end_t - start_t))

if __name__ == "__main__":
    main()

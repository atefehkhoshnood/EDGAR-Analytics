########################################
## Insight data engineering Challenge ##
## Log file sessionization code       ##
## by Atefeh Khoshnood                ##
## date: June 2018                    ##
########################################

import os
import csv
from datetime import datetime, date, time, timedelta
from sessionization_fun import is_ip_repeated_dic, is_ip_repeated_list, get_info_for_outputfile, random_pick_of_data


start_t = datetime.now()

def main():

# creating output file
	outputfile = open(os.curdir+'/output/sessionization.txt','w')
    
# openning inputfiles and check if they are not empty
	logfile_size = os.path.getsize(os.curdir+'/input/log.csv')
	inactivity_periodfile_size = os.path.getsize(os.curdir+'/input/inactivity_period.txt')
	if logfile_size>0 and inactivity_periodfile_size>0:
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
# determining order of fields in a row of log file
	ip_index = headers.index('ip')
	date_index = headers.index('date')
	time_index = headers.index('time')
	cik_index = headers.index('cik')
	accession_index = headers.index('accession')
	extention_index = headers.index('extention')

	data = {}
	counter_logfileline = 0
#  if log file is larger than 12 Mb, we randomly ignore 50% of data using random_pick_of_data function	
	Max_file_size = 12000000

# reading log file line by line until end of file is reached while:
# - storing data such as ip, date, time, and number of visits in a dictinary structure 
# - calculating sessions using inactivity period and writing corresponding ip and other info in output file
# - building a list of inactive ips that should be deleted
# - deleting inactive dictionary keys that are processed and written in output file as a session
	for row in logfile_content:

		counter_logfileline += 1
		time_now = datetime.strptime(row[time_index], '%H:%M:%S')
		if counter_logfileline == 1:
			time_past = time_now
		elif len(data)!= 0:
			oldest_key = next(iter(data))
			time_past = datetime.strptime(oldest_key[1][1], '%H:%M:%S')
# checking if the time has advanced at least as many seconds as inactivity period, if so start looking for inactive ips and sessions
		time_diff = time_now - time_past
		time_diff_sec = time_diff.total_seconds()
		if time_diff_sec > inactivity_period:
			data_maybe_inactive = []
			data_written = []
			data_inactive = []
# searching for ips that maybe inactive and can define a session then build a list of them
			for key in data:
				time_item = datetime.strptime(key[1][1],'%H:%M:%S')
				time_diff = time_now - time_item
				time_diff_sec = time_diff.total_seconds()
				if time_diff_sec > inactivity_period:
					data_maybe_inactive.append(key)
# searching for ips that are inactive and defining a session
# writing sessions to output file
# making a list of sessions so they be deleted from main data strcuture to release memory
			for key in data_maybe_inactive:
				ip_item = key[0]
				i = 0
				no_repeated, count_webpage_request = is_ip_repeated_dic(ip_item,data)
				no_repeated_list = is_ip_repeated_list(ip_item,data_maybe_inactive)

				if no_repeated == 1:
					outputfile.write(key[0]+','+key[1][0]+' '+key[1][1]+','+key[1][0]+' '+key[1][1]+',1,'+str(count_webpage_request)+'\n')
					data_inactive.append(key)
				elif (no_repeated != 1) and (no_repeated_list == no_repeated) and (ip_item not in data_written):
					dt_sec,time_s,time_e = get_info_for_outputfile(ip_item,no_repeated,data)
					outputfile.write(ip_item+','+time_s+','+time_e+','+str(int(dt_sec))+','+str(count_webpage_request)+'\n')
					data_written.append(ip_item)
					data_inactive.append(key)
# deleting sessions from main data structure to release memory
			for key in data_inactive:
				data.pop(key,0)
# adding data from log file to main data structure
		key = (row[ip_index],(row[date_index],row[time_index]))
		if key in data and random_pick_of_data(logfile_size,Max_file_size):
			data[key] += 1 
		elif random_pick_of_data(logfile_size,Max_file_size):
			data[key] = 1
# analysing what is left in main data structure, because end of log file is reached
# searching for repeated ips
	data_repeated = {}
	for key in data:
		ip_item = key[0]
		no_repeated, count_webpage_request = is_ip_repeated_dic(ip_item,data)
		if no_repeated != 1 and ip_item not in data_repeated:
				data_repeated[ip_item] = (key[1][1],no_repeated, count_webpage_request)

# writing sessions to output file
	for key in data:
		ip_item = key[0]
		if (ip_item in data_repeated) and (data_repeated[ip_item][0] == key[1][1]):
			date_item, no_repeated, count_webpage_request = data_repeated[ip_item]
			dt_sec,time_s,time_e = get_info_for_outputfile(ip_item,no_repeated,data)
			outputfile.write(ip_item+','+time_s+','+time_e+','+str(int(dt_sec))+','+str(count_webpage_request)+'\n')
		elif (ip_item not in data_repeated):
			outputfile.write(key[0]+','+key[1][0]+' '+key[1][1]+','+key[1][0]+' '+key[1][1]+',1,'+str(data[key])+'\n')

	outputfile.close()
	end_t = datetime.now()
	print ('Job is done in '+ str(end_t - start_t))

if __name__ == "__main__":
    main()

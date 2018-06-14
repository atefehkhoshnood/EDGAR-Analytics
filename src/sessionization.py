########################################
## Insight data engineering Challenge ##
## Log file sessionization code       ##
## by Atefeh Khoshnood                ##
## date: June 2018                    ##
## Second code by using NumPy         ##
########################################

import sys
import os
import numpy as np
import csv
from datetime import datetime, date, time, timedelta
from sessionization_fun import create_session, delete_inactive_session, get_info_for_outputfile

start_t = datetime.now()

global dateFormat, timeFormat, datetimeFormat
dateFormat = '%Y-%m-%d'
timeFormat = '%H:%M:%S'
datetimeFormat = dateFormat + ' ' + timeFormat

# setup input and output files

logfile_name = sys.argv[1]
inactivity_periodfile_name = sys.argv[2]
outputfile_name = sys.argv[3]

# creating output file
outputfile = open(outputfile_name,'w')

logfile_size = os.path.getsize(logfile_name)
inactivity_periodfile_size = os.path.getsize(inactivity_periodfile_name)
if logfile_size>0 and inactivity_periodfile_size>0:
    # openning inactivity period file
    inactivity_periodfile = open(inactivity_periodfile_name,'r')
    # openning of log file
    logfile = open(logfile_name,'r')
else:
    print('one or both input files are empty.')
    exit()

# reading the inactivity period from file
inactivity_period = int(inactivity_periodfile.read())
inactivity_periodfile.close()

logfile_content = csv.reader(logfile)

headers = next(logfile_content)

# determining order of fields in a row of log file
ip_index = headers.index('ip')
date_index = headers.index('date')
time_index = headers.index('time')
cik_index = headers.index('cik')
accession_index = headers.index('accession')
extention_index = headers.index('extention')


# Initializing NumPy arrays
IP = np.ndarray(0,dtype=str)
start_date_time = np.ndarray(0,dtype=datetime)
count_webpage_request = np.ndarray(0,dtype=int)
end_date_time = np.ndarray(0,dtype=datetime)

# reading log file line by line until end of file is reached while:
# - storing data such as ip, start date&time, number of visits, and time of last request in NumPy arrays 
# - checking if any ip is inactive and writing the session info to output file
# - deleting inactive ips and releasing space 
for row in logfile_content:
    
    ip = row[ip_index]
    accessDate = row[date_index]
    accessTime = row[time_index]
    current_date_time = accessDate + ' ' + accessTime
    
    current_date_time = datetime.strptime(current_date_time, datetimeFormat)
    #print (IP.size)
# checking if this is the first data  
    if IP.size == 0:
        IP, start_date_time, count_webpage_request, end_date_time = create_session(IP, start_date_time, count_webpage_request, end_date_time, ip, current_date_time)
        continue
    
# checking if the ip is new or it is a repeated ip   
    repeated_ip_mask = np.in1d( IP, np.array(ip) )
    repeated_ip_index = np.arange(IP.size)[repeated_ip_mask]
    
# if a repeated ip, increase webpage request by one and refresh end date&time tag to current date&time
    if repeated_ip_index.size == 1:
        end_date_time[repeated_ip_index[0]] = current_date_time
        count_webpage_request[repeated_ip_index[0]] = count_webpage_request[repeated_ip_index[0]] + 1
# if a new ip, add its info to corresponding arrays
    elif repeated_ip_index.size == 0:
        IP, start_date_time, count_webpage_request, end_date_time = create_session(IP, start_date_time, count_webpage_request, end_date_time, ip, current_date_time)
    
# searching for inactive ips in the entire dataset
    inactive_ip_mask = np.zeros(IP.size, dtype = bool)
    for sessIndex, sess_end_date_time in enumerate(end_date_time):
        time_diff = current_date_time - sess_end_date_time
        inactivity = time_diff.seconds > inactivity_period
        inactive_ip_mask[sessIndex] = inactivity
        if inactivity:
            outputfile_content = get_info_for_outputfile(IP[sessIndex], start_date_time[sessIndex], end_date_time[sessIndex], count_webpage_request[sessIndex])
            outputfile.write('%s\n' % outputfile_content)

# deleting inactive ip from data structure 
    IP, start_date_time, count_webpage_request, end_date_time = delete_inactive_session(IP, start_date_time, count_webpage_request, end_date_time, inactive_ip_mask)

# analysing what is left in main data structure, because end of log file is reached
for k in range(0,IP.size):
    outputfile_content = get_info_for_outputfile(IP[k], start_date_time[k], end_date_time[k], count_webpage_request[k])
    outputfile.write('%s\n' % outputfile_content)
# deleting all the created NumPy arrays
del IP, start_date_time, count_webpage_request, end_date_time

outputfile.close()
end_t = datetime.now()
print ('Job is done in '+ str(end_t - start_t))

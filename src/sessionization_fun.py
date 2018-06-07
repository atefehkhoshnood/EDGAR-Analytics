from datetime import datetime, date, time, timedelta
from collections import Counter
from random import randint


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
def get_info_for_outputfile(ip,no_repeated,data_dict):
	i = 0
	for k in data_dict:
		if k[0] == ip:
			i +=1
			if i == 1:
				time_start_0, time_start_1 = k[1]
				time_start = datetime.strptime(k[1][1],'%H:%M:%S')
			if i == no_repeated:
				time_end_0, time_end_1 = k[1]
				time_end = datetime.strptime(k[1][1],'%H:%M:%S')
	time_diff = time_end - time_start
	time_diff_sec = time_diff.total_seconds() + 1 
	return(time_diff_sec,time_start_0+' '+time_start_1,time_end_0+' '+time_end_1)

def random_pick_of_data(logsize,maxsize):
	i=randint(1, 2)
	if logsize>maxsize and i !=1:
		return False
	return True;
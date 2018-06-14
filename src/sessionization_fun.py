import numpy as np
from datetime import datetime, date, time, timedelta

global dateFormat, timeFormat, datetimeFormat
dateFormat = '%Y-%m-%d'
timeFormat = '%H:%M:%S'
datetimeFormat = dateFormat + ' ' + timeFormat


def create_new_session(IP, start_date_time, count_webpage_request, end_date_time, ip, current_date_time):
    new_IP = np.append(IP,ip)
    new_start_date_time = np.append(start_date_time,current_date_time)
    new_count_webpage_request = np.append(count_webpage_request,1)
    new_end_date_time = np.append(end_date_time,current_date_time)
    return new_IP, new_start_date_time, new_count_webpage_request, new_end_date_time

def delete_inactive_session(IP, start_date_time, count_webpage_request, end_date_time, inactive_ip_mask):
    active_ip_mask = np.logical_not(inactive_ip_mask)
    new_IP = IP[active_ip_mask]
    new_start_date_time = start_date_time[active_ip_mask]
    new_count_webpage_request = count_webpage_request[active_ip_mask]
    new_end_date_time = end_date_time[active_ip_mask]
    return new_IP, new_start_date_time, new_count_webpage_request, new_end_date_time

def get_info_for_outputfile(ip, start_date_time, end_date_time, count_webpage_request):
    dt = end_date_time - start_date_time + timedelta(seconds=1) # because session duration is inclusive
    outputfile_content = ip + ',' + start_date_time.strftime(datetimeFormat) + ',' + end_date_time.strftime(datetimeFormat) + ',' + str(dt.seconds) + ',' + str(count_webpage_request)
    return outputfile_content

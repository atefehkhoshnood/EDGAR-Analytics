########################################
## Insight data engineering Challenge ##
## Log file sessionization code       ##
## by Atefeh Khoshnood                ##
## date: June 2018                    ##
## Second code by using NumPy         ##
########################################

import sys
import numpy as np
import csv
from datetime import datetime, date, time, timedelta
from sessionization_fun import create_new_session, remove_expired_session, generate_ending_report

start_t = datetime.now()

# setup input and output files

logfile_name = sys.argv[1]
inactivity_periodfile_name = sys.argv[2]
outputfile_name = sys.argv[3]

# creating output file
outputfile = open(outputfile_name,'w')
# openning of log file
logfile = open(logfile_name)
# reading the inactivity period from file
inactivity_periodfile = open(inactivity_periodfile_name)
inactivity_period = int(inactivity_periodfile.read())
inactivity_periodfile.close()

'-- Set date-time formatting (ideally this should be read from something like a config file) --'
global __dateFormat__, __timeFormat__, __datetimeFormat__
__dateFormat__ = '%Y-%m-%d'
__timeFormat__ = '%H:%M:%S'
__datetimeFormat__ = __dateFormat__ + ' ' + __timeFormat__


logfile_content = csv.reader(logfile)

'-- Some filtering parameters for the log file --'
colnameDict = {'ip':0, 'date':1, 'time':2, 'zone':3, 'cik':4, 'accession':5, 'extention':6, 'code':7,
    'size':8, 'idx':9, 'norefer':10, 'noagent':11, 'find':12, 'crawler':13, 'browser':14}
itemOfInterest = [colnameDict['ip'],colnameDict['date'],colnameDict['time'],colnameDict['cik'],colnameDict['accession'] ]


# Create some arrays
IPs = np.ndarray(0,dtype=str)
StartDateTime = np.ndarray(0,dtype=datetime)
numDocRequested = np.ndarray(0,dtype=int)
LastRequestTime = np.ndarray(0,dtype=datetime)

' ---- Process each line of the csv as if it is streamed in real-time ---- '
#outputfile.mode = 'a'
for dataLine in logfile_content:
    if logfile_content.line_num == 1: continue
    
    '====== Start processing per stream ======'
    ip = dataLine[colnameDict['ip']]
    accessDate = dataLine[colnameDict['date']]
    accessTime = dataLine[colnameDict['time']]
    current_datetime = accessDate + ' ' + accessTime
    
    current_datetime = datetime.strptime(current_datetime, __datetimeFormat__)
    
    '---- Very first line being stream?? ----'
    if IPs.size == 0:
        IPs, StartDateTime, numDocRequested, LastRequestTime = create_new_session(IPs, StartDateTime, numDocRequested, LastRequestTime, ip, current_datetime)
        continue
    
    '---- Check if this IP is new or part of the previously opened session ----'
    intersectMask = np.in1d( IPs, np.array(ip) )
    intersectIndex = np.arange(IPs.size)[intersectMask]
    
    '---- If existed, add a request to the corresponding session ----'
    if intersectIndex.size == 1:
        LastRequestTime[intersectIndex[0]] = current_datetime
        numDocRequested[intersectIndex[0]] = numDocRequested[intersectIndex[0]] + 1
    elif intersectIndex.size == 0:
        '---- If new, create a new session and add to sessions list ----'
        IPs, StartDateTime, numDocRequested, LastRequestTime = create_new_session(IPs, StartDateTime, numDocRequested, LastRequestTime, ip, current_datetime)
    
    '---- Check all currently opened sessions to identify which session(s) has expired ----'
    expiredSessionsMask = np.zeros(IPs.size, dtype = bool)
    for sessIndex, sess_lastRequestTime in enumerate(LastRequestTime):
        elapsedTime = current_datetime - sess_lastRequestTime
        expirestatus = elapsedTime.seconds > inactivity_period
        expiredSessionsMask[sessIndex] = expirestatus
        '-- If expire, generate the session report --'
        if expirestatus:
            endingReport = generate_ending_report(IPs[sessIndex], StartDateTime[sessIndex], LastRequestTime[sessIndex], numDocRequested[sessIndex])
            outputfile.write('%s\n' % endingReport)

    '---- Remove expired sessions out of the list of current opened sessions ----'
    IPs, StartDateTime, numDocRequested, LastRequestTime = remove_expired_session(IPs, StartDateTime, numDocRequested, LastRequestTime, expiredSessionsMask)

    '====== End processing per stream ======'

'====== End of stream, set all current sessions to expire ======'
for k in list(range(0,IPs.size)):
    endingReport = generate_ending_report(IPs[k], StartDateTime[k], LastRequestTime[k], numDocRequested[k])
    outputfile.write('%s\n' % endingReport)

del IPs, StartDateTime, numDocRequested, LastRequestTime

outputfile.close()
end_t = datetime.now()
print ('Job is done in '+ str(end_t - start_t))

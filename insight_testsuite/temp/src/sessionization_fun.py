import numpy as np
from datetime import datetime, date, time, timedelta

'-- Set date-time formatting (ideally this should be read from something like a config file) --'
global __dateFormat__, __timeFormat__, __datetimeFormat__
__dateFormat__ = '%Y-%m-%d'
__timeFormat__ = '%H:%M:%S'
__datetimeFormat__ = __dateFormat__ + ' ' + __timeFormat__


def create_new_session(IPs, StartDateTime, numDocRequested, LastRequestTime, ip, current_datetime):
    new_IPs = np.append(IPs,ip)
    new_StartDateTime = np.append(StartDateTime,current_datetime)
    new_numDocRequested = np.append(numDocRequested,1)
    new_LastRequestTime = np.append(LastRequestTime,current_datetime)
    return new_IPs, new_StartDateTime, new_numDocRequested, new_LastRequestTime

def remove_expired_session(IPs, StartDateTime, numDocRequested, LastRequestTime, expiredSessionsMask):
    notExpiredMask = np.logical_not(expiredSessionsMask)
    new_IPs = IPs[notExpiredMask]
    new_StartDateTime = StartDateTime[notExpiredMask]
    new_numDocRequested = numDocRequested[notExpiredMask]
    new_LastRequestTime = LastRequestTime[notExpiredMask]
    return new_IPs, new_StartDateTime, new_numDocRequested, new_LastRequestTime

def generate_ending_report(ip, startDateTime, lastRequestTime, numDocRequested):
    sessionDuration = lastRequestTime - startDateTime
    sessionDuration = sessionDuration + timedelta(seconds=1) # because session duration is inclusive
    endingReport = ip + ',' + \
                    startDateTime.strftime(__datetimeFormat__) + ',' + \
                    lastRequestTime.strftime(__datetimeFormat__) + ',' + \
                    str(sessionDuration.seconds) + ',' + \
                    str(numDocRequested)
    return endingReport

# EDGAR Analytics
### An Insight Data Engineering Challenge
### by: Atefeh Khoshnood
### June 2018

# Table of Contents
1. [Challenge Summary](README.md#challenge-summary)
2. [Solutions](README.md#solutions)
3. [Requirements and General Description](README.md#requirements-and-general-description)

# Challenge Summary
The Securities and Exchange Commission's (SEC) Electronic Data Gathering, Analysis and Retrieval (EDGAR) system keeps track of which IP addresses have accessed which documents for what company, and at what day and time this occurred.

For this challenge, we're asked to take existing publicly available EDGAR weblogs and assume that each line represents a single web request for an EDGAR document that would be streamed into our program in real time. 

Using the data, we should identify when a user visits a website or accesses a document, calculate the duration of and number of documents requested during that visit, and then write the output to a file. The SEC data can be found [here](https://www.sec.gov/dera/data/edgar-log-file-data-set.html). More information and details of the challenge can be found [here](https://github.com/InsightDataScience/edgar-analytics/blob/master/README.md#repo-directory-structure)


# Solutions
## First Solution
For the purposes of this challenge, an IP address uniquely identifies a single user. The IP address along with the date-time label defines an event. I use Python dictionary as the data structure. Python dictionary is an unordered set of key: value pairs, with the requirement that the keys are unique. Here, a tuple of IP and date-time defines a unique key and serve the purpose. The value for each key is the total number of websites/documents that the user has visited at that specific time. For building the data structure, I read the log file line by line and and check if the tuple of IP and date-time is unique. If it is unique I add a new entry to the dictionary with value of 1. If not, I increase the value of the corresponding key which is already in the dictionary.

At each step, I check if the time difference between the oldest event in the dictionary and current time is larger than the inactivity period. If so, I stop adding more entries to the dictionary and search for inactive keys in it. While writing inactive keys into the output file, I build a list of them, so I can delete them from the dictionary later. This way, I have partial control over the size of the dictionary. If large number of users access different websites in a period smaller than the inactivity period, the dictionary becomes large and the program slows down. Run times for sample log files of different sizes are shown in the table below. The runs are done on a single processor (2.53 GHz Intel Core i5). 

| Approximate log file size | Run time (H:M:S.mS) |
| ------ | ------ |
| 1 k | 0:00:00.07 |
| 10 k | 0:00:00.07 |
| 50 k | 0:00:00.11 |
| 100 k | 0:00:00.51 |
| 500 k | 0:00:04.90 |
| 1 M | 0:00:05.76 |
| 10 M | 0:00:17.61 |

Note that for log files larger than 12 M, the program slows down. It does not crash for large log files but the run time can be several minutes. I use a function to randomly sample data and ignore them so I can reduce the execution time.

## Second Solution
The first solution is unable to handle large log files and slows down to levels that makes the use of Python dictionary questionable. The reason is behind the fact that searching for a key is time consuming when the dictionary gets large. Note that in the first solution the key is a tuple structure with type string. I code a second solution using NumPy arrays. They have optimized routines for fast computation and searching such as `Numpy.arange` and `Numpy.in1d`. Also arrays are indexed by integers and may be accessed faster, and occupy less memory. I use 4 different arrays to store unique sessions which are characterized by ip value, start date and time tag, end date and time tag, and number of webpage visits. If the time difference between the end date and time tag and current time is larger than the inactivity period, that session has ended. Then, it is written in the output file and all four arrays are refreshed by deleting the info of ended session.
The speed performance for the second solution is summarized in the below table.

| Approximate log file size | Run time (H:M:S.mS) |
| ------ | ------ |
| 1 k | 0:00:00.01 |
| 10 k | 0:00:00.02 |
| 50 k | 0:00:00.08 |
| 100 k | 0:00:00.14 |
| 500 k | 0:00:00.65 |
| 1 M | 0:00:01.40 |
| 10 M | 0:00:13.85 |

Clearly, the speed has improved. More importantly, the code works for large log files without significant increase in run time. For example, a log file with size ~20 Mb is done in 32 seconds compare with several minutes using the first solution.

# Requirements and General Description
The program is in Python. I use Python 3.6.2 to compile and run on OS X 10.11.6. 
The first code can be run from terminal by this command:

edgar-analytics~$ ./run_v1.sh

The code that uses NumPy arrays and is functional for all sizes of log file can be run by:

edgar-analytics~$ ./run.sh


There are 3 tests within the `insight_testsuite` folder which can be run by:

insight_testsuite~$ ./run_tests.sh

The tests are set to use the code with NumPy arrays. Both programs in the current versions pass all the tests.
The source files are within `src` directory. The code with Python dictionary is in `sessionization_v1.py` which includes the main program with functions in `sessionization_fun_v1.py`. The code with NumPy array is in `sessionization.py` which includes the main program with functions in `sessionization_fun.py`. The `input` directory includes several log files with different sizes, and the `inactivity_period.txt` file which contains the inactivity period. The `log.csv` file can be replaced by any other log files in the `input` directory with the following command:

input~$ cp log-10k.csv log.csv 






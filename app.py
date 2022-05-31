import argparse
from logging import exception
from os.path import exists
import json

#exceptions
class MalformedLog(Exception):
    pass

#log entry classes
class LogEntry:
    def __init__(self, timestamp, headerSize, clientIp, responseCode, responseSize, requestMethod, url, username, destinationIp, responseType):
        self.timestamp = float(timestamp)
        self.headerSize = int(headerSize)
        self.clientIp = clientIp
        self.responseCode = responseCode
        self.responseSize = int(responseSize)
        self.requestMethod = requestMethod
        self.url = url
        self.username = username
        self.destinationIp = destinationIp
        self.responseType = responseType
    
    def FromLogLine(lineString):
        el = list(filter(('').__ne__, lineString.split(' ')))
        if len(el) < 10:
            raise MalformedLog
        try:
            line = LogEntry(el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9])
            return line
        except:
            raise MalformedLog

#parser classes
class logFileCSVParser():
    def __parseLogFile(f):
        logs = []
        lines = f.splitlines()
        for l in lines:
            if not l:
                continue
            try:
                logs.append(LogEntry.FromLogLine(l))
            except MalformedLog:
                print('found a malformed log line')
        return logs
    
    def __init__(self, fileStr):
        self.logs = logFileCSVParser.__parseLogFile(fileStr)

parserFactory = {'csv': logFileCSVParser}

#log operations
def mostFrequentIp(logs):
    print('performing most frequent ip query')
    ips = [l.clientIp for l in logs]
    return max(set(ips), key = [l.clientIp for l in logs].count)

def leastFrequentIp(logs):
    print('performing least frequent ip query')
    ips = [l.clientIp for l in logs]
    return min(set(ips), key = [l.clientIp for l in logs].count)

def bytesExchanged(logs):
    print('performing total bytes exchanged query')
    return sum([l.headerSize for l in logs], 0) + sum([l.responseSize for l in logs], 0)

def eventPerSeconds(logs):
    print('performing event per seconds query')
    timestamps = [l.timestamp for l in logs]
    timestamps.sort()
    endTimestamp = timestamps[-1]
    totalTime = endTimestamp - timestamps[0]
    return len(logs) / totalTime

class logAction:
    def __init__(self, condition, key, delegate):
        self.condition = condition
        self.key = key
        self.delegate = delegate
    
    def __call__(self, dict, logs):
        if(self.condition):
            dict[self.key] = self.delegate(logs)



#main
argParser = argparse.ArgumentParser()
argParser.add_argument('--files', '--names-list', nargs='+', required=True)
argParser.add_argument('--outputFile', type=str, default='./output.json')
argParser.add_argument('--fileFormat', type=str, default='csv')
argParser.add_argument('--mostFrequentIp', action='store_true')
argParser.add_argument('--leastFrequentIp', action='store_true')
argParser.add_argument('--bytesExchanged', action='store_true')
argParser.add_argument('--eventPerSeconds', action='store_true')
args = argParser.parse_args()

actions = [
    logAction(args.mostFrequentIp, 'most_frequent_ip', mostFrequentIp),
    logAction(args.leastFrequentIp, 'least_frequent_ip', leastFrequentIp),
    logAction(args.bytesExchanged, 'total_bytes_exchanged', bytesExchanged),
    logAction(args.eventPerSeconds, 'event_per_seconds', eventPerSeconds)
]


#collecting all logs from all files provided, each file inside a parser object
parsers = []
for file in args.files:
    try:
        f = open(file).read()
    except OSError:
        print("Could not open/read file:", file)
        continue
    parsers.append(parserFactory[args.fileFormat](f))
    
allLogs = sum([p.logs for p in parsers], [])
if not allLogs:
    print('no log found in files')
    exit(0)

#performing all actions activated via command line on logs and writing output file
try:
    with open(args.outputFile, 'w') as outfile:
        jsonDict = {'total_log_count': len(allLogs)}
        for a in actions:
            a(jsonDict, allLogs)
        jsonstring = json.dumps(jsonDict)
        print(jsonstring)
        outfile.write(jsonstring)
except FileNotFoundError:
    print("The specified output directory does not exist")
except Exception as e:
    print(e)


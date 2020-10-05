import sys
import datetime
totalLines = []
with open(sys.argv[1], "r") as f:
    f.readline()
    startLine = f.readline()
with open(sys.argv[2], "r") as f:
    endLine =f.readlines()[403]

with open(sys.argv[2][:-18] + "_total_time_10ms.csv", "w") as f:
    splittedLine1 = startLine.split(",")
    splittedLine2 = endLine.split(",")
    startTime = splittedLine1[1][:-1]
    endTime = splittedLine2[1][:-1]
    print(startTime)
    print(endTime)
    totalTime = datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S.%f') - datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S.%f')
    f.write("Experiment duration: " + str(totalTime.total_seconds()) + " s\n")
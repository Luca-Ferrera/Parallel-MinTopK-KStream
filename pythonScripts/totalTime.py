import sys
import datetime
totalLines = []
with open(sys.argv[1], "r") as f:
    f.readline()
    startLine = f.readline()
with open(sys.argv[2], "r") as f:
    endLine =f.readlines()[-1]
with open(sys.argv[2][:-23] + "total_time" + sys.argv[2][-15:], "w") as f:
    splittedLine1 = startLine.split(",")
    splittedLine2 = endLine.split(",")
    startTime = splittedLine1[1][:-1]
    endTime = splittedLine2[1][:-1]
    print(startTime)
    print(endTime)
    totalTime = datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S.%f') - datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S.%f')
    f.write("Experiment duration:\n" + str(totalTime.total_seconds()) + " s\n" + str(totalTime.total_seconds()/60) + " min\n")
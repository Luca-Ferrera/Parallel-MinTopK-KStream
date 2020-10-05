import sys
import datetime
totalLines = []
with open(sys.argv[1], "r") as f:
    startLines =f.readlines()[1:]
with open(sys.argv[2], "r") as f:
    endLines =f.readlines()[1:]

with open(sys.argv[2][:-16] + "10ms_latency.csv", "w") as f:
    f.write("window,latency\n")
    for (l1,l2) in zip(startLines, endLines):
        splittedLine1 = l1.split(",")
        splittedLine2 = l2.split(",")
        startTime = splittedLine1[1][:-1]
        endTime = splittedLine2[1][:-1]
        latency = datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S.%f') - datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S.%f')
        f.write(splittedLine2[0] + "," + str(latency.total_seconds()*1000) + "\n")
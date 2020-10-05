import sys
import datetime
totalLines = []
for i in range(int(sys.argv[1]) + 1):
    with open(sys.argv[i+2], "r") as f:
        lines =f.readlines()[1:]
        totalLines.append(lines)

with open(sys.argv[2][:-29] + "_" + sys.argv[1] +"instances_latency.csv", "w") as f:
    f.write("window,latency\n")
    unpackedLines = zip(*totalLines)
    for (l1,l2,l3,l4,l5,l6,l7) in unpackedLines:
        splittedLine1 = l1.split(",")
        splittedLine2 = l2.split(",")
        splittedLine3 = l3.split(",")
        splittedLine4 = l4.split(",")
        splittedLine5 = l5.split(",")
        splittedLine6 = l6.split(",")
        splittedLine7 = l7.split(",")
        startTime = max( splittedLine2[1][:-1], splittedLine3[1][:-1], splittedLine4[1][:-1], splittedLine5[1][:-1], splittedLine6[1][:-1], splittedLine7[1][:-1])
        latency = datetime.datetime.strptime(splittedLine1[1][:-1], '%Y-%m-%d %H:%M:%S.%f') - datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S.%f')
        f.write(splittedLine1[0] + "," + str(latency.total_seconds()*1000) + "\n")
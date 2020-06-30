import sys
import datetime
with open(sys.argv[1], "r") as f:
    lines1 =f.readlines()[1:]
with open(sys.argv[2], "r") as f:
    lines2 =f.readlines()[1:]
with open(sys.argv[3], "r") as f:
    lines3 =f.readlines()[1:]
with open(sys.argv[4], "r") as f:
    startingLines =f.readlines()[1:]
with open(sys.argv[4][:-16] + "5ms.csv", "w") as f:
    f.write("window,latency\n")
    for (l1,l2,l3,l4) in zip(lines1,lines2,lines3, startingLines):
        splittedLine1 = l1.split(",")
        splittedLine2 = l2.split(",")
        splittedLine3 = l3.split(",")
        splittedLine4 = l4.split(",")
        startTime = max(splittedLine1[1][:-1], splittedLine2[1][:-1], splittedLine3[1][:-1])
        f.write(splittedLine4[0] + "," + str(datetime.datetime.strptime(splittedLine4[1][:-1], '%Y-%m-%d %H:%M:%S.%f') - datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S.%f')) + "\n")
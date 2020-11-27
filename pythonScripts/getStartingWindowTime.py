import sys
import datetime
with open(sys.argv[1], "r") as f:
    lines =f.readlines()[0::int(sys.argv[2])]
with open(sys.argv[1][:-4] + "_start_window.csv", "w") as f:
    f.write("window,latency\n")
    for line in lines:
        splittedLine = line.split()
        f.write(splittedLine[2] + "," + str(datetime.datetime.strptime(splittedLine[4], "%Y-%m-%dT%H:%M:%S.%fZ")) + "\n")

import sys
import numpy as np
seconds = []
minutes = []
for i in range(int(sys.argv[1])):
    with open(sys.argv[i+2], "r") as f:
        f.readline()
        seconds.append(f.readline())
        minutes.append(f.readline())
with open(sys.argv[2][:-15] + "_total_time_statistic.csv", "w") as f:
    f.write("average, std\n")
    times = []
    for l in seconds:
        times.append(float(l.split()[0]))
    average = np.mean(times)
    std = np.std(times)
    f.write(str(average) + " s, " + str(std) + " s\n")
    times = []
    for l in minutes:
        times.append(float(l.split()[0]))
    average = np.mean(times)
    std = np.std(times)
    f.write(str(average) + " min, " + str(std) + " min\n")
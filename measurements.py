import sys
import statistics
with open(sys.argv[1], "r") as f:
    lines1 =f.readlines()[211:611]
with open(sys.argv[2], "r") as f:
    lines2 =f.readlines()[211:611]
with open(sys.argv[3], "r") as f:
    lines3 =f.readlines()[211:611]
with open(sys.argv[4], "r") as f:
    lines4 =f.readlines()[211:611]
with open(sys.argv[5], "r") as f:
    lines5 =f.readlines()[211:611]
latency0 = []
latency1 = []
latency2 = []
latency3 = []
latency4 = []
for (l1,l2,l3,l4,l5) in zip(lines1,lines2,lines3, lines4, lines5):
    splittedLine1 = l1.split(",")
    splittedLine2 = l2.split(",")
    splittedLine3 = l3.split(",")
    splittedLine4 = l4.split(",")
    splittedLine5 = l5.split(",")
    latency0.append(float(splittedLine1[1][:-1]))
    latency1.append(float(splittedLine2[1][:-1]))
    latency2.append(float(splittedLine3[1][:-1]))
    latency3.append(float(splittedLine4[1][:-1]))
    latency4.append(float(splittedLine5[1][:-1]))
mean0 = statistics.mean(latency0)
mean1 = statistics.mean(latency1)
mean2 = statistics.mean(latency2)
mean3 = statistics.mean(latency3)
mean4 = statistics.mean(latency4)
stdev0 = statistics.stdev(latency0)
stdev1 = statistics.stdev(latency1)
stdev2 = statistics.stdev(latency2)
stdev3 = statistics.stdev(latency3)
stdev4 = statistics.stdev(latency4)

with open(sys.argv[1][:-14] + "measurements.csv", "w") as f:
    f.write("dataset,mean,stdev\n")
    f.write("0, " + str(mean0) + ", " + str(stdev0) + "\n")
    f.write("1, " + str(mean1) + ", " + str(stdev1) + "\n")
    f.write("2, " + str(mean2) + ", " + str(stdev2) + "\n")
    f.write("3, " + str(mean3) + ", " + str(stdev3) + "\n")
    f.write("4, " + str(mean4) + ", " + str(stdev4) + "\n")

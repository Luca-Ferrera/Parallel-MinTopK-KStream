import sys
import statistics
with open(sys.argv[1], "r") as f:
    lines = f.readlines()
latencies = [int(x.split()[4]) for x in lines]
mean = statistics.mean(latencies)
with open(sys.argv[1][:-4]+"_average.txt", "w") as f:
    f.write("Mean latency " + str(mean))
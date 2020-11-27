import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]]
files2 = [sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11], sys.argv[12], sys.argv[13], sys.argv[14]]
topks = [5, 10, 50, 100, 150, 200, 300]
data = pd.DataFrame()
data2 = pd.DataFrame()

for (topk,file) in zip(topks, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    min = np.min(df['time'])
    max = np.max(df['time'])
    average = np.mean(df['time'])
    data = pd.concat([data, pd.DataFrame({"min":[min], "max":[max], "average":[average], "topk":topk})])
for (topk,file) in zip(topks, files2):
    df2 = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    min = np.min(df2['time'])
    max = np.max(df2['time'])
    average = np.mean(df2['time'])
    data2 = pd.concat([data2, pd.DataFrame({"min":[min], "max":[max], "average":[average], "topk":topk})])

# plt.title(sys.argv[1].split("/")[1] + "'s total time comparison by top-k", fontsize=12)
plt.title('')
plt.suptitle('')
plt.xlabel('Top-K', fontsize=20)
plt.ylabel('Total_time (s)', fontsize=20)
plt.errorbar(x=range(len(files)),
             y=data["average"],
             yerr=[data["average"]-data["min"], data["max"]-data["average"]],
             fmt="o",
             markersize=8,
             capsize=10,
             capthick=3,
             label="Distributed Baseline Algorithm")
plt.errorbar(x=range(len(files)),
             y=data2["average"],
             yerr=[data2["average"]-data2["min"], data2["max"]-data2["average"]],
             fmt="r^",
             markersize=8,
             capsize=10,
             capthick=3,
             label="Distributed MinTop-K")
# plt.yscale("log")
plt.legend(loc='best', fontsize=20)
plt.xticks(range(len(files)),data["topk"], fontsize=20)
plt.yticks(fontsize=20)
plt.grid(axis="both")
plt.show()
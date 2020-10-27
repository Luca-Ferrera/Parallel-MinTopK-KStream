import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]]
topks = [5, 10, 50, 100, 150, 200, 300]
data = pd.DataFrame()

for (topk,file) in zip(topks, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    min = np.min(df['time'])
    max = np.max(df['time'])
    average = np.mean(df['time'])
    data = pd.concat([data, pd.DataFrame({"min":[min], "max":[max], "average":[average], "topk":topk})])

plt.title(sys.argv[1].split("/")[1] + "'s total time comparison by top-k", fontsize=12)
plt.suptitle('')
plt.xlabel('Top-K', fontsize=12)
plt.ylabel('Total_time (s)', fontsize=12)
plt.errorbar(x=range(len(files)),
             y=data["average"],
             yerr=[data["average"]-data["min"], data["max"]-data["average"]],
             fmt="o",
             markersize=8,
             capsize=10,
             capthick=3)
# plt.yscale("log")
plt.xticks(range(len(files)),data["topk"])
plt.grid(axis="both")
plt.show()
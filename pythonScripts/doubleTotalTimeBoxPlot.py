import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2]]
algos = [sys.argv[1].split("/")[1], sys.argv[2].split("/")[1]]
data = pd.DataFrame()

for (algo,file) in zip(algos, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    df['algo'] = algo
    data = pd.concat([data, df])

data.boxplot(by='algo', fontsize=20)
# plt.title("Algorithms's total time comparison", fontsize=12)
plt.title('')
plt.suptitle('')
plt.xlabel('algorithm', fontsize=20)
plt.ylabel('total_time (s)', fontsize=20)
# plt.yscale("log")
plt.show()
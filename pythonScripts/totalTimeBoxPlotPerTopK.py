import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]
topks = [5, 10, 50, 100, 300]
data = pd.DataFrame()

for (topk,file) in zip(topks, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    df['topk'] = topk
    data = pd.concat([data, df])

data.boxplot(by='topk')
plt.title(sys.argv[1].split("/")[1] + "'s total time comparison by top-k")
plt.suptitle('')
plt.xlabel('Top-K')
plt.ylabel('Total_time (s)')
plt.show()
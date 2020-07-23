import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3]]
topKs = [2, 10, 50]
data = pd.DataFrame()

for (topk,file) in zip(topKs, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['mean latency'])
    df['topK'] = topk
    data = pd.concat([data, df])

data.boxplot(by='topK')
plt.title(sys.argv[1][:18])
plt.suptitle('')
plt.xlabel('topK')
plt.ylabel('latency')
plt.show()
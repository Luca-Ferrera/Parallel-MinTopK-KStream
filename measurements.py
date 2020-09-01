import pandas as pd
import sys
import numpy as np

files = [sys.argv[1], sys.argv[2], sys.argv[3]]
topKs = [2, 10, 50]
data = pd.DataFrame()

for (topk,file) in zip(topKs, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['mean latency'])
    df['topK'] = topk
    data = pd.concat([data, df])

pivot = data.pivot_table(index=['topK'], values='mean latency', aggfunc=[np.mean, np.std])

pivot.to_csv(sys.argv[1][:-14] + 'measurements.csv')
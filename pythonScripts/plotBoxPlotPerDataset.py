import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]
datasets = ["dataset0", "dataset1", "dataset2", "dataset3", "dataset4"]
data = pd.DataFrame()

for (dataset,file) in zip(datasets, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['latency'])
    df = df.iloc[212:612]
    df['dataset'] = dataset
    data = pd.concat([data, df])

data.boxplot(by='dataset')
plt.title(sys.argv[6])
plt.suptitle('')
plt.xlabel('dataset')
plt.ylabel('latency')
plt.show()
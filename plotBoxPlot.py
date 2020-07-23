import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]
datasets = ["dataset0", "dataset1", "dataset2", "dataset3", "dataset4"]
data = pd.DataFrame()

for (dataset,file) in zip(datasets, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['latency'])
    df = df.iloc[212:400]
    df['dataset'] = dataset
    data = pd.concat([data, df])

plt.title(sys.argv[1][:-4])
plt.xlabel('dataset')
plt.ylabel('latency')

print(data)

plot = data.boxplot(by='dataset')
plot.set_xlabel('dataset')
plot.set_ylabel('latency')
plt.show()
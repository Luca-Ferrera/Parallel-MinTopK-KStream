import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]]
algos = ["CenMSSTopK", "DisMSSTopK", "CenMinTopK", "DisMinTopK"]
data = pd.DataFrame()

for (algo,file) in zip(algos, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    df['algo'] = algo
    data = pd.concat([data, df])

data.boxplot(by='algo')
plt.title("Algorithms's total time comparison")
plt.suptitle('')
plt.xlabel('algorithm')
plt.ylabel('total_time (s)')
plt.show()
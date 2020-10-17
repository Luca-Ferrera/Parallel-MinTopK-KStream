import pandas as pd
import matplotlib.pyplot as plt
import sys

file = sys.argv[1]
algo = sys.argv[1].split("/")[1]
data = pd.DataFrame()

df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
df['algo'] = algo
data = pd.concat([data, df])

data.boxplot(by='algo')
plt.title(algo)
plt.suptitle('')
plt.xlabel('')
plt.ylabel('total_time (s)')
plt.show()
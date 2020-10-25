import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3]]
instances = [3, 6, 10]
data = pd.DataFrame()

for (instance,file) in zip(instances, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    df['instance'] = instance
    data = pd.concat([data, df])

data.boxplot(by='instance')
plt.title(sys.argv[1].split("/")[1] + "'s total time comparison by number of instances")
plt.suptitle('')
plt.xlabel('Instances')
plt.ylabel('Total_time (s)')
plt.show()
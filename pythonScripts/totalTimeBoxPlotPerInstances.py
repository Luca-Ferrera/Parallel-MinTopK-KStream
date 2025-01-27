import pandas as pd
import matplotlib.pyplot as plt
import sys
files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]]
instances = [1, 3, 6, 10]
data = pd.DataFrame()

for (instance,file) in zip(instances, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    df['instance'] = instance
    data = pd.concat([data, df])
# d = {'instance':[0,0,0,0,0], "time": None}
# data = pd.concat([data,pd.DataFrame(data=d)])
# d = {'instance':[1,1,1,1,1], "time": None}
# data = pd.concat([data,pd.DataFrame(data=d)])
d = {'instance':[2,2,2,2,2], "time": None}
data = pd.concat([data,pd.DataFrame(data=d)])
d = {'instance':[4,4,4,4,4], "time": None}
data = pd.concat([data,pd.DataFrame(data=d)])
d = {'instance':[5,5,5,5,5], "time": None}
data = pd.concat([data,pd.DataFrame(data=d)])
d = {'instance':[7,7,7,7,7], "time": None}
data = pd.concat([data,pd.DataFrame(data=d)])
d = {'instance':[8,8,8,8,8], "time": None}
data = pd.concat([data,pd.DataFrame(data=d)])
d = {'instance':[9,9,9,9,9], "time": None}
data = pd.concat([data,pd.DataFrame(data=d)])
data.boxplot(by='instance', fontsize=20)
# plt.title(sys.argv[1].split("/")[1] + "'s total time comparison by number of instances", fontsize=12)
plt.title('')
plt.suptitle('')
plt.xlabel('Instances', fontsize=20)
plt.ylabel('Total_time (s)', fontsize=20)
plt.ylim(top=8000)
# plt.yscale("log")
plt.show()
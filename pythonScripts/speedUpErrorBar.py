import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np

files1 = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]]
files2 = [sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11], sys.argv[12], sys.argv[13], sys.argv[14]]
topks = [5, 10, 50, 100, 150, 200, 300]
data1 = pd.DataFrame()
data2 = pd.DataFrame()
final = pd.DataFrame()

for (topk,file) in zip(topks, files1):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    min = np.min(df['time'])
    max = np.max(df['time'])
    average = np.mean(df['time'])
    data1 = pd.concat([data1, pd.DataFrame({"min":[min], "max":[max], "average":[average], "topk":topk})])

for (topk,file) in zip(topks, files2):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['time'])
    min = np.min(df['time'])
    max = np.max(df['time'])
    average = np.mean(df['time'])
    data2 = pd.concat([data2, pd.DataFrame({"min":[min], "max":[max], "average":[average], "topk":topk})])
data1.set_index('topk', inplace=True)
data2.set_index('topk', inplace=True)

final['speed_up'] = data1['average']/data2['average']
final['worst_case_speed_up']= data1['min']/data2['max']
final['best_case_speed_up']= data1['max']/data2['min']

plt.title('')
plt.suptitle('')
plt.xlabel('Top-K', fontsize=20)
plt.ylabel('Speed-up', fontsize=20)
plt.errorbar(x=range(len(files1)),
             y=final['speed_up'],
             yerr=[final['speed_up'] - final['worst_case_speed_up'], final['best_case_speed_up'] - final['speed_up']],
             fmt='o',
             markersize=8,
             capsize=10,
             capthick=3)
# plt.yscale('log')
plt.xticks(range(len(files1)),final.index, fontsize=20)
plt.yticks(fontsize=20)
plt.grid(axis='both')
plt.show()
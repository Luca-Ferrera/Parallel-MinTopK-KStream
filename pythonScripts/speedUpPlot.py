import pandas as pd
import matplotlib.pyplot as plt
import sys

files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]]
files2 = [sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11], sys.argv[12], sys.argv[13], sys.argv[14]]
topks = [5, 10, 50, 100, 150, 200, 300]
data = pd.DataFrame()
data2 = pd.DataFrame()
final = pd.DataFrame()

for (topk,file) in zip(topks, files):
    df = pd.read_csv(file, skipinitialspace=True, usecols=['average'])
    df['topk'] = topk
    data = pd.concat([data, df])
for (topk,file) in zip(topks, files2):
    df2 = pd.read_csv(file, skipinitialspace=True, usecols=['average'])
    df2['topk'] = topk
    data2 = pd.concat([data2, df2])

for (av1,av2,topk, index) in zip(data["average"][0], data2["average"][0], topks, range(len(files))):
    final = pd.concat([final, pd.DataFrame({"speedup":[float(av1.split()[0])/float(av2.split()[0])], "topk":topk, "index":index})])

print(final)
final.plot.scatter(x='index', y='speedup', s=100)
plt.title('')
plt.suptitle('')
plt.xlabel('Top-K', fontsize=20)
plt.ylabel('Speed-up', fontsize=20)

plt.xticks(range(len(files)),final["topk"], fontsize=20)
plt.yticks(fontsize=20)
plt.grid(axis="both")
plt.show()
import sys
import pandas as pd
files = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]
data = pd.DataFrame()

for file in files:
    df = pd.read_csv(file)
    df = df.iloc[212:612]
    data = pd.concat([data, df])

pivot = data.pivot_table(index=['window'], values='latency', aggfunc={'mean'})

pivot.to_csv(sys.argv[1][:-14] + 'average.csv')

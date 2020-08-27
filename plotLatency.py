import pandas as pd
import plotly.express as px
import sys

df = pd.read_csv(sys.argv[1])

fig = px.line(df, x = 'window', y = 'latency', title=sys.argv[1][:-4])
fig.show()
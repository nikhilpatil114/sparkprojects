import matplotlib.pyplot as plt
import pandas as pd
df = pd.read_csv('E:\\bigdatafile\\plot.csv')
#print(df)
#df.plot.hist(x='salnew',y="sal")
#df.plot.bar(x='age',y='sal' )
#df.plot.line(x='age',y='sal')
#df.plot(x="Open",y="Close")
#df.plot(y='salnew',x='sal', figsize=(9,6))
#df.plot.line(y=['sal','age'], figsize=(10,6))
#df.plot(kind='bar', figsize=(10,6), xlabel='Detail')
#df.plot(kind='barh', figsize=(9,9))
#df.plot(kind='bar', stacked=True, figsize=(9,6),color=['skyblue','yellow'])
#df[['salnew', 'sal']].plot(kind='hist', bins=25, alpha=0.6, figsize=(9,6))
#df[['age', 'sal']].plot(kind='hist', bins=25, alpha=0.6, stacked=True, figsize=(9,6))
#df.plot(kind='box', figsize=(9,6))
#df.plot(kind='box', vert=False, figsize=(9,6))
#df.plot(kind='area', figsize=(9,6))
#df.plot(kind='area', stacked=False, figsize=(9,6))
#df.index=['March', 'April', 'May','jan','feb']
#df.plot(kind='pie', y='salnew', legend=True, autopct='%.f')
#df.plot(kind='hexbin', x='sal', y='salnew', gridsize=10, figsize=(10,6))
#df.plot(kind='kde', bw_method=1)
plt.show()
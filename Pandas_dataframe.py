import pandas as pd
data="E:\\a_python\\datafile\\datasets-df-series\\empdata.csv"
df=pd.read_csv(data)
print(df.head())
print(df['empid'])
print(df.loc[1:3,["empid","sal"]])  #loc not except last index
print(df.iloc[1:3,0:2])
print(df.iloc[1:3,:])  # iloc except the last index
print(df[df.sal>15000])
print(df[(df.sal>15000) & (df.ename=='Gaurav Gupta')])
print(df[['sal','ename']].sort_values("sal",ascending=False))
print(df[df.sal>15000].empid)
print(df[df.sal>15000][['empid','ename']])
print(df[(df.sal>15000) & (df.ename=='Gaurav Gupta')][['empid','ename']])
print(df.agg({'sal':['min','max','mean']}))
print(df['sal'].unique())
print(df.groupby(['dept']).size().to_frame('count'))
print(df.groupby(['dept']).size().to_frame('count').reset_index().sort_values('count',ascending=False))
print(df.groupby(['dept']).size().to_frame('count').reset_index().sort_values('count',ascending=False))
print(df[df.dept.isin(['hr','mr'])])
print(df[~df.dept.isin(['hr','mr'])])
data1="E:\\a_python\\datafile\\datasets-df-series\\emp.csv"
df1=pd.read_csv(data)

#print(pd.concat(df[df.sal==10000],df1[df1.sal==23000])).drop_duplicate
df3=df.append({'empid':1009,"ename":"jayesh patil","sal":23200,"doj":"11/11/2001","dept":"sales"},ignore_index=True)
print(df3.head(8))


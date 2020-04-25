import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
#import openpyxl

#import json

#f = open("querydata.csv", "r")
#print(f.readline())

#for x in f:
 # print(x)
  
f = glob.glob("success\*.csv")
#print(f)
#for f in glob.glob("*.csv"):
#    print(f)
df = pd.read_csv(f[0],delimiter=',',names=['Country', 'Count'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
#print(type(df))
#print(df.head(5))
"""writer = pd.ExcelWriter('output.xlsx')
df.to_excel(writer,'sheet1')
writer.save()

#all_data = pd.DataFrame()
#for f in glob.glob("*.xlsx"):
 #   df = pd.read_excel(f)
 #   all_data = all_data.append(df,ignore_index=True)"""

#print(type(df["Country"]))
#labels = df.iloc[0:5]['Country']

labels = all_data['Country']
print(labels)
string = ['heloo','kjjj']
print(string)
sizes = all_data['Count']

#listdata=str(df).split("\n")
#replacedata=str(listdata).replace(" ","")
#print(df)

#print(matplotlib.get_cachedir())
#def bar_graph():align='edge'
index = np.arange(len(labels))
print(index)
#which defines the length of total plot
plt.figure(figsize=(20, 3))
#which defines width of bar
plt.bar(index, sizes, width=0.3)
plt.xlabel('Name')
plt.ylabel('Count')
#which defines font size of xticks
plt.xticks(index, labels,fontsize=7)
plt.title('countries and its count')

plt.show()

#bar_graph()


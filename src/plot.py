"""
Program that plots the graphs for all the TPC-E transactions

Authors:
1. Ravikiran Jois Yedur Prabhakar
2. Karanjit Singh
3. Suhas Vijayakumar
"""

import json
import pandas as pd
import matplotlib.pyplot as plt


plots=['update','order','status','lookup']

for p in plots:
    f1=open('trade_'+p+'.json')
    data1=json.load(f1)

    f2=open('trade_'+p+'_wal'+'.json')
    data2=json.load(f2)

    f3=open('trade_'+p+'_inmemory'+'.json')
    data3=json.load(f3)


    data={'time_required':data1['time_required'],
        'on_disk':data1['number_of_transactions'],
        'with_logging':data2['number_of_transactions'],
        'in_memory':data3['number_of_transactions']
        }


    df=pd.DataFrame.from_dict(data)

    df.plot(kind='line',x='time_required',y=['on_disk','with_logging','in_memory'],marker='o')
    plt.ylabel('number of transactions')
    plt.title('trade_'+p)
    plt.show()

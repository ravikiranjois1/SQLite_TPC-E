"""
Program that implements TPC-E benchmark method in-memory: Trade Status

Authors:
1. Ravikiran Jois Yedur Prabhakar
2. Karanjit Singh
3. Suhas Vijayakumar
"""


import json
import sqlite3
import random
import time
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect(":memory;")
cur = conn.cursor()
print('creating tables')
sql = open('./SQLite_TPC-E/scripts/1_create_table.sql', mode='r', encoding='utf-8-sig').read()
cur.executescript(sql)
print('tables created')
disk_conn=sqlite3.connect('tpce')
disk_cur=disk_conn.cursor()
disk_cur.execute("select name from sqlite_master where type='table';")
tables=disk_cur.fetchall()
tables=[t[0] for t in tables]
for file in tables:
    print(file)
    query='select * from {};'.format(file)
    df=pd.read_sql_query(query,disk_conn)
    df.to_sql(file, conn, if_exists='append', index =False)
    
sql=open('./SQLite_TPC-E/scripts/4_create_index.sql', mode='r', encoding='utf-8-sig').read()
cur.executescript(sql)
sql=open('./SQLite_TPC-E/scripts/4_create_fk_index.sql', mode='r', encoding='utf-8-sig').read()
cur.executescript(sql)

print('indexes created ')

def frame1():
    numfound = 0
    cur.execute("Select ca_id from customer_account")

    customers= cur.fetchall()
    c_len=len(customers)
    customer = customers[random.randint(0,c_len-1)][0]

    query = '''Select t_id, t_dts, st_name, tt_name, t_s_symb, t_qty, t_exec_name, t_chrg, s_name, ex_name from trade,
    status_type, trade_type, security, exchange where t_ca_id = {} and st_id = t_st_id and tt_id = t_tt_id and s_symb =
    t_s_symb and ex_id = s_ex_id order by t_dts desc limit 50'''.format(customer)
    cur.execute(query)
    records = cur.fetchall()
    #print(records)

    trade_id = []
    trade_dts = []
    trade_qty = []
    exec_name = []
    status_name = []
    symbol = []
    charge = []
    s_name = []
    ex_name = []
    type_name = []

    numfound = len(records)
    for row in records:
        t_id, t_dts, st_name, tt_name, t_s_symb, t_oty, t_exec_name, t_chrg, S_name, Ex_name = row
        trade_id.append(t_id)
        trade_dts.append(t_dts)
        trade_qty.append(t_oty)
        exec_name.append(t_exec_name)
        status_name.append(st_name)
        symbol.append(t_s_symb)
        charge.append(t_chrg)
        s_name.append(S_name)
        ex_name.append(Ex_name)


    query = '''Select c_l_name, c_f_name, b_name from CUSTOMER_ACCOUNT, CUSTOMER, BROKER
    where CA_ID = {} and C_ID = CA_C_ID and B_ID = CA_B_ID'''.format(customer)

    cur.execute(query)
    try:
        ca_id, c_id, b_id = cur.fetchone()
        #print(ca_id, c_id, b_id)
    except:
        pass

df_data={'number_of_transactions':[],'time_required':[]}
succesfull_transactions=0
total_time=0

for op in range(20):
    #print('here')
    start_time=time.time()

    while time.time()<start_time+60:
        try:
            cur.execute('begin')
            #print('habshd')
            frame1()
            cur.execute('commit')
            succesfull_transactions+=1
            #print('frame1 commited')
        except:
            #print('haksdb')
            cur.execute('rollback')
        end_time=time.time()
    total_time+=end_time-start_time
    df_data['time_required'].append(total_time)
    df_data['number_of_transactions'].append(succesfull_transactions)


with open("trade_status_inmemory.json", "w") as fp:
    json.dump(df_data, fp)
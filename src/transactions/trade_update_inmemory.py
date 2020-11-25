"""
Program that implements TPC-E benchmark method in-memory: Trade Update

Authors:
1. Ravikiran Jois Yedur Prabhakar
2. Karanjit Singh
3. Suhas Vijayakumar
"""


import sqlite3
import random
import time
import pandas as pd
import matplotlib.pyplot as plt
import json

conn= sqlite3.connect(':memory;')
cur =conn.cursor()
#create tables
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

'''Frame Number 1'''
def frame1():
    i = 0
    ex_name = ''

    num_found = 0
    num_updated = 0

    max_trades, max_updated = 20, 20

    query = '''select t_id from trade'''
    cur.execute(query)
    trade_ids=cur.fetchall()
    trade_ids=[t[0] for t in trade_ids]
    trade_id = []
    for i in range(20):
        trade_id.append(trade_ids[random.randrange(len(trade_ids))])

    for i in range(0, max_trades):
        if num_updated < max_updated:
            query = '''Select t_exec_name from trade where t_id = {}'''.format(trade_id[i])
            #print(query)
            cur.execute(query)
            names = cur.fetchall()
            names=[n[0] for n in names]
            num_found += len(names)
            for n in names:
                if 'x' in n:
                    n = n.replace('x', ' ')
                else:
                    n = n.replace(' ', 'x')

                query = '''Update Trade 
                Set t_exec_name = "{}" where t_id = {}'''.format(n, trade_id[i])
                num_updated += 1


    for i in trade_id:
        query = '''Select t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, t_trade_price from trade, trade_type
        where t_id = {} and t_tt_id = tt_id'''.format(i)
        cur.execute(query)
        bid_price, exec_name, is_cash, is_market, trade_price = cur.fetchone()
        #print("For Trade ID ", i, ": Bid Price = ", bid_price, ", Executive Name = ", exec_name, ", Is Cash?: ", is_cash,
            #", Is Market?: ", is_market, ", Trade Price = ", trade_price)

        query = '''Select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE from settlement where SE_T_ID = {}'''.format(i)
        cur.execute(query)
        settlement_amount, settlement_cash_due_date, settlement_cash_type = cur.fetchone()
        #print("Settlement Amount = ", settlement_amount, ", Settlement Cash Due Date = ", settlement_cash_due_date,
            #", Settlement Cash Type = ", settlement_cash_type)
        if is_cash:
            query = '''Select CT_AMT, CT_DTS, CT_NAME from CASH_TRANSACTION where CT_T_ID = {}'''.format(i)
            cur.execute(query)
            cash_transaction_amt, cash_transaction_dts, cash_transaction_name = cur.fetchone()
            #print("Trade ", i, " is a cash transaction with Cash transaction amount = ", cash_transaction_amt,
                #", Cash transaction dts = ", cash_transaction_dts, " and Cash transaction name = ", cash_transaction_name)

        query = '''Select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = {} order by TH_DTS limit 3'''.format(i)
        cur.execute(query)
        answer = cur.fetchall()
        for row in answer:
            trade_history_dts, trade_history_status_id = row
            #print("Trade History DTS = ", trade_history_dts, ", Trade History Status ID = ", trade_history_status_id)

def frame2():
    i = 0
    max_trades = 20
    max_updates = 20

    cur.execute("Select min(t_ca_id), max(t_ca_id) from trade")
    min_id, max_id = cur.fetchone()
    customer = random.randrange(min_id, max_id)


    query = '''select T_DTS from trade'''
    cur.execute(query)
    times = cur.fetchall()
    times=[t[0] for t in times]


    start_pos = random.randint(0,len(times)-10)
    start = times[start_pos]
    end = times[random.randint(start_pos+1,start_pos+10)]

    query = '''Select t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price from trade
        where t_ca_id = {} and T_DTS >= '{}' order by T_DTS asc '''.format(customer, start)
    #print(query)
    cur.execute(query)
    trades = cur.fetchall()
    num_found = len(trades)
    num_updated = 0

    for i in range(num_found):
        if num_updated < max_updates:
            query = '''select SE_CASH_TYPE from settlement where SE_T_ID = {}'''.format(trades[i][3])
            cur.execute(query)
            cash_type = cur.fetchone()
            if trades[i][2]:
                if cash_type == "Cash Account":
                    cash_type = "Cash"
                else:
                    cash_type = "Cash Account"
                if cash_type == "Margin Account":
                    cash_type = "Margin"
                else:
                    cash_type = "Margin Account"

            query = '''Update Settlement set SE_CASH_TYPE = "{}" where SE_T_ID = {}'''.format(cash_type, trades[i][3])
            num_updated += 1

            query = '''Select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE from SETTLEMENT where SE_T_ID = {}'''.format(trades[i][3])
            cur.execute(query)
            #print("For Trade id: ", trades[i][3])
            #for n in cur.fetchone():
                #print(n)

            query = '''Select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = {} order by TH_DTS limit 3'''\
                .format(trades[i][3])
            #print(query)
            cur.execute(query)
            #print("For Trade id: ", trades[i][3])
            op = cur.fetchall()
            for row in op:
                #print(row)
                pass

def frame3():
    max_trades = 20
    max_updates = 20

    query = '''select TH_DTS from TRADE_HISTORY'''
    cur.execute(query)
    times = cur.fetchall()
    times=[t[0] for t in times]

    start_pos = random.randrange(len(times))
    start = times[start_pos]
    end = times[random.randrange(start_pos, len(times))]

    query = '''Select T_S_SYMB from trade'''
    cur.execute(query)
    security = cur.fetchall()
    security=[s[0] for s in security]
    symbol = security[random.randrange(len(security))]

    query = '''Select T_CA_ID, T_EXEC_NAME, T_IS_CASH, T_TRADE_PRICE, T_QTY, S_NAME, T_DTS, T_ID, T_TT_ID, TT_NAME
    from TRADE, TRADE_TYPE, SECURITY where T_S_SYMB = '{}' and T_DTS >= '{}' and T_DTS <= '{}' and TT_ID = T_TT_ID and 
    S_SYMB = T_S_SYMB order by T_DTS asc limit 20'''.format(symbol, start, end)
    cur.execute(query)
    trades = cur.fetchall()

    num_found = len(trades)
    num_updated = 0

    for i in range(num_found):
        query = '''Select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE from SETTLEMENT where SE_T_ID = {}  '''.format(trades[i][7])
        cur.execute(query)
        for n in cur.fetchall():
            #print(n)
            pass
        if trades[i][2]:
            if num_updated < max_updates:
                query = ''' Select CT_NAME from CASH_TRANSACTION where CT_T_ID = {}'''.format(trades[i][7])
                cur.execute(query)
                ct_name = cur.fetchone()
                if ' shares of ' in ct_name:
                    ct_name = str(trades[i][9]) + ' ' + str(trades[i][4]) + ' '+ 'Shares of' + ' ' + str(trades[i][5])
                else:
                    ct_name = str(trades[i][9]) + ' ' + str(trades[i][4]) + ' '+ 'shares of' + ' ' + str(trades[i][5])

                query = ''' Update CASH_TRABSACTION set CT_NAME = "{}" where CT_T_ID = {}'''.format(ct_name,trades[i][7])

                num_updated += 1

            query = ''' Select CT_AMT, CT_DTS, CT_NAME from CASH_TRANSACTION where CT_T_ID = {}'''.format(trades[i][7])
            cur.execute(query)
            for row in cur.fetchall():
               #print(row)
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

        try:
            cur.execute('begin')
            frame2()
            cur.execute('commit')
            succesfull_transactions+=1
            #print('frame2 commited')
        except:
            cur.execute('rollback')

    #print((op+1)*1000,'trasactions attempted')
    end_time=time.time()
    total_time+=end_time-start_time
    df_data['time_required'].append((op+1)*60)
    df_data['number_of_transactions'].append(succesfull_transactions)
    print('succesful transactions ', succesfull_transactions)


with open("trade_update_inmemory.json", "w") as fp:
    json.dump(df_data, fp)

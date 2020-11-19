import sqlite3
import random
import time
import pandas as pd
import matplotlib.pyplot as plt

conn= sqlite3.connect(':memory;')
cur =conn.cursor()



#frame_1
def frame1():
    cur.execute("SELECT min(t_id),max(t_id) from trade order by t_id")
    trade_min,trade_max=cur.fetchone()
    num_found=0
    bid_price={}
    exec_name={}
    is_cash={}
    is_market={}
    trade_price={}
    settlement_amount={}
    settlement_cash_due_date={}
    settlement_cash_type={}
    cash_transaction_amount={}
    cash_transaction_dts={}
    cash_transaction_name={}
    trade_history_dts={}
    trade_history_status_id={}

    trade_count=0
    settlement_count=0
    cash_count=0
    trade_history_count=0

    for i in range(trade_min,trade_max):
        #get_trade_information
        query='''
        SELECT t_bid_price,t_exec_name,t_is_cash,tt_is_mrkt,t_trade_price from trade,trade_type where t_id={} and t_tt_id=tt_id
        '''.format(i)  
        cur.execute(query)
        try:
            bid_price[i],exec_name[i],is_cash[i],is_market[i],trade_price[i]=cur.fetchone()
            num_found+=1
            trade_count+=1
        except:
            pass
            

        #get_settlement_information
        query='''
        SELECT se_amt,se_cash_due_date,se_cash_type from settlement where se_t_id ={} 
        '''.format(i)
        cur.execute(query)
        try:
            settlement_amount[i],settlement_cash_due_date[i],settlement_cash_type[i]=cur.fetchone()
            settlement_count+=1
        except:
            pass


        #get_cash_information
        if i in is_cash:
            query='''
            SELECT ct_amt,ct_dts,ct_name from cash_transaction where ct_t_id={}
            '''.format(i)
        
            cur.execute(query)
            try:
                cash_transaction_amount[i],cash_transaction_dts[i],cash_transaction_name[i]=cur.fetchone()
                cash_count+=1
            except:
                pass

        #trade_history
        query='''
        SELECT th_dts,th_st_id from trade_history where th_t_id={} order by th_dts limit 3
        '''.format(i)
        cur.execute(query)
        resp=cur.fetchall()
        for r in resp:
            try:
                trade_history_dts[i].append(r[0]) 
                trade_history_status_id[i].append(r[1])
            except:
                trade_history_dts[i]=[r[0]]
                trade_history_status_id[i]=[r[1]]
                trade_history_count+=1
        


    #print('trade    count',trade_count)
    #print('settlement count',settlement_count)
    #print('cash count',cash_count)
    #print('trade history count',trade_history_count)






#frame2
def frame2():
    query='''SELECT t_ca_id from trade;'''
    cur.execute(query)
    accounts=cur.fetchall()
    accounts=[a[0] for a in accounts]
    a_len=len(accounts)
    query='''SELECT th_dts from trade_history order by th_dts;'''
    cur.execute(query)
    timestamps=cur.fetchall()
    timestamps=[t[0] for t in timestamps]
    t_len=len(timestamps)

    account=accounts[random.randint(0,a_len-1)]
    start=random.randint(0,t_len-1)
    start_trade_dts=timestamps[start]
    end_trade_dts=timestamps[start+random.randint(0,t_len-1-start)]

    query='''
    SELECT t_bid_price,t_exec_name,t_is_cash,t_id,t_trade_price from trade
    where t_ca_id ={} and
    t_dts>='{}' and
    t_dts<='{}'
    order by t_dts;
    '''.format(account,start_trade_dts,end_trade_dts)
    cur.execute(query)
    resp=cur.fetchall()
    if resp:
        for r in resp:
            #get extra information for each trade
            query='''
            SELECT se_amt,se_cash_due_date,se_cash_type from settlement
            where
            se_t_id={}
            '''.format(r[3])
            cur.execute(query)
            cur.fetchall()

            #get_cash_information_for_the_trade
            query='''
            SELECT ct_amt,ct_dts,ct_name from cash_transaction
            where ct_t_id ={}
            '''.format(r[3])
            cur.execute(query)
            cur.fetchall()

            #trade_history for trades
            query='''
            SELECT th_dts,th_st_id
            from
            trade_history
            where th_t_id={}
            order by th_dts limit 3;
            '''.format(r[3])
            cur.execute(query)
            cur.fetchall()


#frame3
def frame3():
    query='''SELECT th_dts from trade_history order by th_dts;'''
    cur.execute(query)
    timestamps=cur.fetchall()
    timestamps=[t[0] for t in timestamps]
    t_len=len(timestamps)
    start=random.randint(0,t_len-1)
    start_trade_dts=timestamps[start]
    end_trade_dts=timestamps[start+random.randint(0,t_len-1-start)]
    cur.execute("SELECT min(ca_id),max(ca_id) from customer_account;")
    ca_min,ca_max=cur.fetchone()
    cur.execute('''SELECT t_s_symb from trade;''')
    symbols=cur.fetchall()
    symbols=[sym[0] for sym in symbols]
    sy_len=len(symbols)
    symbol=symbols[random.randint(0,sy_len)]

    query='''
    SELECT t_ca_id,t_exec_name,t_is_cash,t_trade_price,t_qty,t_dts,t_id,t_tt_id
    from trade where
    t_s_symb = '{}' and
    t_dts>='{}' and
    t_dts<='{}'
    order by t_dts
    limit 20
    '''.format(symbol,start_trade_dts,end_trade_dts)

    cur.execute(query)
    resp=cur.fetchall()
    if resp:
        for r in resp:
            query='''
            select se_amt,se_cash_due_date,se_cash_type
            from settlement
            where se_t_id ={};
            '''.format(r[6])
            cur.execute(query)
            cur.fetchall()

            #get_cash_information_for_the_trade
            query='''
            SELECT ct_amt,ct_dts,ct_name from cash_transaction
            where ct_t_id ={}
            '''.format(r[6])
            cur.execute(query)
            cur.fetchall()
            

            #trade_history for trades
            query='''
            SELECT th_dts,th_st_id
            from
            trade_history
            where th_t_id={}
            order by th_dts limit 3;
            '''.format(r[6])
            cur.execute(query)
            cur.fetchall()
        


#frame4
def frame4():
    query='''SELECT t_ca_id from trade;'''
    cur.execute(query)
    accounts=cur.fetchall()
    accounts=[a[0] for a in accounts]
    a_len=len(accounts)
    account=accounts[random.randint(0,a_len-1)]


    query='''SELECT t_dts from trade order by t_dts;'''
    cur.execute(query)
    timestamps=cur.fetchall()
    timestamps=[t[0] for t in timestamps]
    t_len=len(timestamps)
    start_dts=timestamps[random.randint(0,t_len-1)]

    query='''
    select t_id from trade
    where
    t_ca_id= {} and
    t_dts>='{}'
    order by t_dts
    limit 1
    '''.format(account,start_dts)
    cur.execute(query)
    trade=cur.fetchone()[0]

    #The trade_id is used in the subquery to find the original trade_id
    query='''
    select
    hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty
    from holding_history
    where hh_h_t_id in
    (
        select hh_h_t_id
        from holding_history
        where
        hh_t_id = {}
    ) 
    limit 20
    '''.format(trade)
    cur.execute(query)
    cur.fetchone()


df_data={'number_of_transactions':[],'time_required':[]}
succesfull_transactions=0
total_time=0

for op in range(5):
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

        try:
            cur.execute('begin')
            frame3()
            cur.execute('commit')
            succesfull_transactions+=1
            #print('frame3 commited')
        except:
            cur.execute('rollback')

        try:
            cur.execute('begin')
            frame4()
            cur.execute('commit')
            succesfull_transactions+=1
            #print('frame4 commited')
        except:
            cur.execute('rollback')
    #print((op+1)*1000,'trasactions attempted')
    end_time=time.time()
    total_time+=end_time-start_time
    df_data['time_required'].append(total_time)
    df_data['number_of_transactions'].append(succesfull_transactions)

df=pd.DataFrame.from_dict(df_data)
df.plot(kind='line',x='time_required',y='number_of_transactions')
plt.show()


















    
    



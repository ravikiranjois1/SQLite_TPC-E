import sqlite3
import random
conn= sqlite3.connect('tpce')
cur =conn.cursor()


"""
#frame_1
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
    


#print('trade count',trade_count)
#print('settlement count',settlement_count)
#print('cash count',cash_count)
#print('trade history count',trade_history_count)
"""





#frame2
bid_price={}
exec_name={}
is_cash={}
is_market={}
trade_price={}
settlement_amount={}
settlement_cash_due_date={}
settlement_cash_type={}

query='''SELECT ca_id from customer_account;'''
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
print(account,start_trade_dts,end_trade_dts)
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
        query='''
        SELCET se_amt,se_cash_due_date,se_cash_type from settlement
        where
        se_t_id= trade_list[i]
        '''
    










    
    


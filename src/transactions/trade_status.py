import sqlite3
import random

conn = sqlite3.connect("tpce")
cur = conn.cursor()
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
print(records)

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

print(numfound)
print("For custormer ", customer, " The trade info is:")
print(trade_id, trade_dts, trade_qty, exec_name, status_name, symbol, charge, s_name, ex_name, type_name)
print()

query = '''Select c_l_name, c_f_name, b_name from CUSTOMER_ACCOUNT, CUSTOMER, BROKER
where CA_ID = {} and C_ID = CA_C_ID and B_ID = CA_B_ID'''.format(customer)

cur.execute(query)
try:
    ca_id, c_id, b_id = cur.fetchone()
    print(ca_id, c_id, b_id)
except:
    pass






import sqlite3
from random import randrange
from datetime import datetime
import time

conn = sqlite3.connect('tpce')
cur = conn.cursor()

"""Frame 1"""

cur.execute("select ca_id from CUSTOMER_ACCOUNT")
ca_id_fetch = cur.fetchall()
list_of_ca_ids = ca_id_fetch
random_index = randrange(len(ca_id_fetch))
acct_id = list_of_ca_ids[random_index][0]

# Get account, customer, and broker information

account_info = dict()
get_account_1 = cur.execute(
    "select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = " + str(acct_id))
get_account_list = list(get_account_1)
account_info["acct_name"] = get_account_list[0][0]
broker_id = get_account_list[0][1]
cust_id = get_account_list[0][2]
tax_status = get_account_list[0][3]

customer_info = dict()
get_customer = cur.execute(
    "select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = '{}'".format(str(cust_id)))
get_customer_list = list(get_customer)
customer_info["cust_f_name"] = get_customer_list[0][0]
customer_info["cust_l_name"] = get_customer_list[0][1]
cust_tier = get_customer_list[0][2]
customer_info["tax_id"] = get_customer_list[0][3]

broker_info = dict()
get_broker = cur.execute("select B_NAME from BROKER where B_ID = '{}'".format(str(broker_id)))
get_broker_list = list(get_broker)
if len(get_broker_list) > 0:
    broker_info["broker_name"] = get_broker_list[0][0]

"""Frame 2"""

cur.execute("select c_f_name, c_l_name, c_tax_id from CUSTOMER")
customer_dict = dict()
customer_name_info = cur.fetchall()
list_customer_name_info = list(customer_name_info)
random_index = randrange(len(list_customer_name_info))
customer_entry = list_customer_name_info[random_index]
exec_f_name = customer_entry[0]
exec_l_name = customer_entry[1]
customer_dict["exec_tax_id"] = customer_entry[2]

if exec_f_name != customer_info["cust_f_name"] or exec_l_name != customer_info["cust_l_name"]\
        or customer_dict["exec_tax_id"] != customer_info["tax_id"]:
    account_perm_info = dict()
    get_permission = cur.execute("select AP_ACL from ACCOUNT_PERMISSION "
                                 "where AP_CA_ID = '{}' and AP_F_NAME = '{}' and AP_L_NAME = '{}' and AP_TAX_ID = '{}'".format(
        str(acct_id), str(exec_f_name), str(exec_l_name), str(customer_dict["exec_tax_id"])))
    get_permission_list = list(get_permission)
    if len(get_permission_list) > 0:
        ap_acl = get_permission_list[0][0]

"""Frame 3"""

symbol_info = cur.execute("select t_id, t_tt_id, t_s_symb, t_lifo, t_qty, t_is_cash, t_st_id from TRADE")
trade_dict = dict()
trade_info = cur.fetchall()
list_trade_info = list(trade_info)
random_index = randrange(len(list_trade_info))
trade_entry = list_trade_info[random_index]
# trade_id = trade_entry[0]
trade_type_id = trade_entry[1]
symbol = trade_entry[2]
is_lifo = trade_entry[3]
trade_quantity = trade_entry[4]
type_is_margin = trade_entry[5]


company_info = cur.execute("select CO_NAME from COMPANY")
company_dict = dict()
company_info = cur.fetchall()
list_company_info = list(company_info)
random_index = randrange(len(list_company_info))
company_entry = list_company_info[random_index]
company_dict["co_name"] = company_entry[0]

security_dict = dict()
if symbol == "":
    cur.execute("select CO_ID from COMPANY where CO_NAME = '{}'".format(company_dict["co_name"]))
    co_info = cur.fetchall()
    co_id = list(co_info)[0][0]

    cur.execute("select S_EX_ID, S_NAME, S_SYMB, S_ISSUE from SECURITY where S_CO_ID = '{}'".format(co_id))
    security_list = list(cur.fetchall())[0]
    exch_id = security_list[0]
    security_dict["s_name"] = security_list[1]
    symbol = security_list[2]
    security_dict["issue"] = security_list[3]
else:
    cur.execute("select S_CO_ID, S_EX_ID, S_NAME from SECURITY where S_SYMB = '{}'".format(symbol))
    security_list = list(cur.fetchall())[0]
    security_dict["co_id"] = security_list[0]
    exch_id = security_list[1]
    security_dict["s_name"] = security_list[2]

    cur.execute("select CO_NAME from COMPANY where CO_ID = '{}'".format(security_dict["co_id"]))
    list_company_name = list(cur.fetchall())
    if len(list_company_name) > 0:
        co_name = list_company_name[0][0]

# To get the current pricing information for the security
cur.execute("select LT_PRICE from LAST_TRADE where LT_S_SYMB = '{}'".format(symbol))
market_price_list = list(cur.fetchall())
market_price = market_price_list[0][0]

# Set trade characteristics based on the type of trade
cur.execute("select TT_IS_MRKT, TT_IS_SELL from TRADE_TYPE where TT_ID = '{}'".format(trade_type_id))
trade_type_list = list(cur.fetchall())
type_is_market = trade_type_list[0][0]
type_is_sell = trade_type_list[0][1]

# TODO """Change required""" - Have to give requested price as input
# if type_is_market: - """Change required"""
requested_price = market_price

buy_value = 0
sell_value = 0
needed_qty = trade_quantity

cur.execute("select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = '{}' and HS_S_SYMB = '{}'".format(acct_id, symbol))
list_holding_summary = list(cur.fetchall())
if len(list_holding_summary):
    hs_qty = list_holding_summary[0][0]

list_holding = []
hs_qty = 0
if type_is_sell:
    if hs_qty > 0:
        if is_lifo:
            cur.execute(
                "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS desc".format(
                    acct_id, symbol))
            list_holding = list(cur.fetchall())
        else:
            cur.execute(
                "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS asc".format(
                    acct_id, symbol))
            list_holding = list(cur.fetchall())

        h_qty = list_holding[0][0]
        h_price = list_holding[0][1]
        index = 0
        while index < len(list_holding[0]) and needed_qty != 0:
            hold = list_holding[0][index]
            hold_qty = int(hold[0])
            hold_price = hold[1]

            if hold_qty > needed_qty:
                buy_value += needed_qty * hold_price
                sell_value += needed_qty * requested_price
                needed_qty = 0
            else:
                buy_value += hold_qty * hold_price
                sell_value += hold_qty * requested_price
                needed_qty = needed_qty - hold_qty
else:
    if hs_qty < 0:
        if is_lifo:
            cur.execute(
                "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS desc".format(
                    acct_id, symbol))
            list_holding = list(cur.fetchall())
        else:
            cur.execute(
                "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS asc".format(
                    acct_id, symbol))
            list_holding = list(cur.fetchall())

        h_qty = list_holding[0][0]
        h_price = list_holding[0][1]
        index = 0

        while index < len(list_holding[0]) and needed_qty != 0:
            hold = list_holding[0][index]
            hold_qty = int(hold[0])
            hold_price = hold[1]

            if hold_qty + needed_qty < 0:
                sell_value += needed_qty * hold_price
                buy_value += needed_qty * requested_price
                needed_qty = 0
            else:
                hold_qty = -hold_qty
                sell_value += hold_qty * hold_price
                buy_value += hold_qty * requested_price
                needed_qty = needed_qty - hold_qty

tax_amount = 0
if sell_value > buy_value and (tax_status == 1 or tax_status == 2):
    cur.execute(
        "select sum(TX_RATE) from TAXRATE where TX_ID in (select CX_TX_ID from CUSTOMER_TAXRATE where CX_C_ID = '{}')".format(
            cust_id))
    tax_rates_list = list(cur.fetchall())
    tax_rates = tax_rates_list[0][0]
    tax_amount = (sell_value - buy_value) * tax_rates

comm_rate = 0
cur.execute(
    "select CR_RATE from COMMISSION_RATE where CR_C_TIER = '{}' and CR_TT_ID = '{}' and CR_EX_ID = '{}' and CR_FROM_QTY <= '{}' and CR_TO_QTY >= '{}'".format(
        cust_tier, trade_type_id, exch_id, trade_quantity, trade_quantity))
comm_rate_list = list(cur.fetchall())
if len(comm_rate_list) > 0:
    comm_rate = comm_rate_list[0][0]

cur.execute("select CH_CHRG from CHARGE where CH_C_TIER = '{}' and CH_TT_ID = '{}'".format(cust_tier, trade_type_id))
charge_list = list(cur.fetchall())
charge_amount = charge_list[0][0]

# Compute assets on margin trades

acct_assets = 0

if type_is_margin:
    cur.execute("select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = '{}'".format(acct_id))
    customer_acc_list = list(cur.fetchall())
    acct_bal = customer_acc_list[0][0]

    # Should return 0 or 1 row

    cur.execute("select sum(HS_QTY * LT_PRICE) from HOLDING_SUMMARY, LAST_TRADE where HS_CA_ID = '{}' and LT_S_SYMB = HS_S_SYMB".format(
            acct_id))
    list_hold_assets = list(cur.fetchall())
    hold_assets = list_hold_assets[0][0]

    if hold_assets is None:  # Account has no holdings
        acct_assets = acct_bal
    else:
        acct_assets = hold_assets + acct_bal

# Set the status for this trade
if type_is_market:
    status_id = "CMPT"
else:
    status_id = "PNDG"

"""Frame-4: Insert the trade"""
comm_amount = (comm_rate / 100) * trade_quantity * requested_price
exec_name = exec_f_name + " " + exec_l_name
if type_is_margin:
    is_cash = 0
else:
    is_cash = 1

now = datetime.now()
now_dts = now.strftime("%Y-%m-%d %H:%M:%S")
cur.execute("select TH_T_ID from TRADE_HISTORY order by TH_T_ID desc limit 1")
new_t_id = int(list(cur.fetchall())[0][0])+1

cur.execute("insert into TRADE (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH, T_S_SYMB, T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, T_LIFO) "
            "values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(new_t_id, now_dts, status_id, trade_type_id, is_cash, symbol, trade_quantity, requested_price, acct_id, exec_name, None, charge_amount, comm_amount, 0, is_lifo))
if not type_is_market:
    cur.execute("insert into TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB, TR_QTY, TR_BID_PRICE, TR_B_ID) "
                "values ('{}', '{}', '{}', '{}', '{}', '{}')".format(new_t_id, trade_type_id, symbol, trade_quantity, requested_price, broker_id))
cur.execute("insert into TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID) values ('{}', '{}', '{}')".format(new_t_id, now_dts, status_id))

"""Frame 5"""
# TODO Give this as input to the method
# Intentional rollback of transaction caused by driver (CE).
# if roll_it_back = 1:
#     cur.execute("ROLLBACK")


"""Frame 6"""
cur.execute("COMMIT")
if type_is_market:
    eAction = 0
else:
    eAction = 1

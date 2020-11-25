"""
Program that implements TPC-E benchmark method in-memory: Trade Order

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
from random import randrange
from datetime import datetime
import json

conn = sqlite3.connect(':memory;')
cur = conn.cursor()
# create tables
print('creating tables')
sql = open('./SQLite_TPC-E/scripts/1_create_table.sql', mode='r', encoding='utf-8-sig').read()
cur.executescript(sql)
print('tables created')

disk_conn = sqlite3.connect('tpce')
disk_cur = disk_conn.cursor()
disk_cur.execute("select name from sqlite_master where type='table';")
tables = disk_cur.fetchall()
tables = [t[0] for t in tables]
for file in tables:
    print(file)
    query = 'select * from {};'.format(file)
    df = pd.read_sql_query(query, disk_conn)
    df.to_sql(file, conn, if_exists='append', index=False)

sql = open('./SQLite_TPC-E/scripts/4_create_index.sql', mode='r', encoding='utf-8-sig').read()
cur.executescript(sql)
sql = open('./SQLite_TPC-E/scripts/4_create_fk_index.sql', mode='r', encoding='utf-8-sig').read()
cur.executescript(sql)

print('indexes created ')

cur.execute('select * from trade limit 10;')
print(cur.fetchall())


def frame_1(connection, cursor):
    """Frame 1"""
    cursor.execute("select ca_id from CUSTOMER_ACCOUNT")
    ca_id_fetch = cursor.fetchall()
    list_of_ca_ids = ca_id_fetch
    random_index = randrange(len(ca_id_fetch))
    acct_id = list_of_ca_ids[random_index][0]

    # Get account, customer, and broker information
    get_account_1 = cursor.execute(
        "select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = " + str(acct_id))
    get_account_list = list(get_account_1)
    acct_name = get_account_list[0][0]
    broker_id = get_account_list[0][1]
    cust_id = get_account_list[0][2]
    tax_status = get_account_list[0][3]

    get_customer = cursor.execute(
        "select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = '{}'".format(str(cust_id)))
    get_customer_list = list(get_customer)
    cust_f_name = get_customer_list[0][0]
    cust_l_name = get_customer_list[0][1]
    cust_tier = get_customer_list[0][2]
    tax_id = get_customer_list[0][3]

    get_broker = cursor.execute("select B_NAME from BROKER where B_ID = '{}'".format(str(broker_id)))
    get_broker_list = list(get_broker)
    if len(get_broker_list) > 0:
        broker_name = get_broker_list[0][0]
    return acct_id, cust_id, cust_f_name, cust_l_name, cust_tier, tax_id, tax_status, broker_id


def frame_2(connection, cursor, cust_id, acct_id, cust_f_name, cust_l_name, cust_tier, tax_id):
    """Frame 2"""
    cursor.execute("select c_f_name, c_l_name, c_tax_id from CUSTOMER")
    customer_dict = dict()
    customer_name_info = cursor.fetchall()
    list_customer_name_info = list(customer_name_info)
    random_index = randrange(len(list_customer_name_info))
    customer_entry = list_customer_name_info[random_index]
    exec_f_name = customer_entry[0]
    exec_l_name = customer_entry[1]
    exec_tax_id = customer_entry[2]

    if exec_f_name != cust_f_name or exec_l_name != cust_l_name or exec_tax_id != tax_id:
        get_permission = cursor.execute("select AP_ACL from ACCOUNT_PERMISSION "
                                        "where AP_CA_ID = '{}' and AP_F_NAME = '{}' and AP_L_NAME = '{}' and AP_TAX_ID = '{}'".format(
            str(acct_id), str(exec_f_name), str(exec_l_name), str(exec_tax_id)))
        get_permission_list = list(get_permission)
        if len(get_permission_list) > 0:
            ap_acl = get_permission_list[0][0]
    return exec_f_name, exec_l_name, exec_tax_id


def frame_3(connection, cursor, cust_id, acct_id, tax_status, cust_tier):
    """Frame 3"""
    symbol_info = cursor.execute("select t_id, t_tt_id, t_s_symb, t_lifo, t_qty, t_is_cash, t_st_id from TRADE")
    trade_info = cursor.fetchall()
    list_trade_info = list(trade_info)
    random_index = randrange(len(list_trade_info))
    trade_entry = list_trade_info[random_index]
    trade_type_id = trade_entry[1]
    symbol = trade_entry[2]
    is_lifo = trade_entry[3]
    trade_quantity = trade_entry[4]
    type_is_margin = trade_entry[5]

    company_info = cursor.execute("select CO_NAME from COMPANY")
    company_info = cursor.fetchall()
    list_company_info = list(company_info)
    random_index = randrange(len(list_company_info))
    company_entry = list_company_info[random_index]
    co_name = company_entry[0]

    security_dict = dict()
    if symbol == "":
        cursor.execute("select CO_ID from COMPANY where CO_NAME = '{}'".format(co_name))
        co_info = cursor.fetchall()
        co_id = list(co_info)[0][0]

        cursor.execute("select S_EX_ID, S_NAME, S_SYMB, S_ISSUE from SECURITY where S_CO_ID = '{}'".format(co_id))
        security_list = list(cursor.fetchall())[0]
        exch_id = security_list[0]
        s_name = security_list[1]
        symbol = security_list[2]
        security_dict["issue"] = security_list[3]
    else:
        cursor.execute("select S_CO_ID, S_EX_ID, S_NAME from SECURITY where S_SYMB = '{}'".format(symbol))
        security_list = list(cursor.fetchall())[0]
        security_dict["co_id"] = security_list[0]
        exch_id = security_list[1]
        s_name = security_list[2]

        cursor.execute("select CO_NAME from COMPANY where CO_ID = '{}'".format(security_dict["co_id"]))
        list_company_name = list(cursor.fetchall())
        if len(list_company_name) > 0:
            co_name = list_company_name[0][0]

    # To get the current pricing information for the security
    cursor.execute("select LT_PRICE from LAST_TRADE where LT_S_SYMB = '{}'".format(symbol))
    market_price_list = list(cursor.fetchall())
    market_price = market_price_list[0][0]

    # Set trade characteristics based on the type of trade
    cursor.execute("select TT_IS_MRKT, TT_IS_SELL from TRADE_TYPE where TT_ID = '{}'".format(trade_type_id))
    trade_type_list = list(cursor.fetchall())
    type_is_market = trade_type_list[0][0]
    type_is_sell = trade_type_list[0][1]

    if type_is_market:
        requested_price = market_price
    else:
        cursor.execute("select AVG(LT_PRICE) from LAST_TRADE where LT_S_SYMB = '{}'".format(symbol))
        avg_market_price_list = list(cursor.fetchall())
        requested_price = avg_market_price_list[0][0]

    buy_value = 0
    sell_value = 0
    needed_qty = trade_quantity

    cursor.execute(
        "select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = '{}' and HS_S_SYMB = '{}'".format(acct_id, symbol))
    list_holding_summary = list(cursor.fetchall())
    if len(list_holding_summary):
        hs_qty = list_holding_summary[0][0]

    list_holding = []
    hs_qty = 0
    if type_is_sell:
        if hs_qty > 0:
            if is_lifo:
                cursor.execute(
                    "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS desc".format(
                        acct_id, symbol))
                list_holding = list(cursor.fetchall())
            else:
                cursor.execute(
                    "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS asc".format(
                        acct_id, symbol))
                list_holding = list(cursor.fetchall())

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
                cursor.execute(
                    "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS desc".format(
                        acct_id, symbol))
                list_holding = list(cursor.fetchall())
            else:
                cursor.execute(
                    "select H_QTY, H_PRICE from HOLDING where H_CA_ID = '{}' and H_S_SYMB = '{}' order by H_DTS asc".format(
                        acct_id, symbol))
                list_holding = list(cursor.fetchall())

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
        cursor.execute(
            "select sum(TX_RATE) from TAXRATE where TX_ID in (select CX_TX_ID from CUSTOMER_TAXRATE where CX_C_ID = '{}')".format(
                cust_id))
        tax_rates_list = list(cursor.fetchall())
        tax_rates = tax_rates_list[0][0]
        tax_amount = (sell_value - buy_value) * tax_rates

    comm_rate = 0
    cursor.execute(
        "select CR_RATE from COMMISSION_RATE where CR_C_TIER = '{}' and CR_TT_ID = '{}' and CR_EX_ID = '{}' and CR_FROM_QTY <= '{}' and CR_TO_QTY >= '{}'".format(
            cust_tier, trade_type_id, exch_id, trade_quantity, trade_quantity))
    comm_rate_list = list(cursor.fetchall())
    if len(comm_rate_list) > 0:
        comm_rate = comm_rate_list[0][0]

    cursor.execute(
        "select CH_CHRG from CHARGE where CH_C_TIER = '{}' and CH_TT_ID = '{}'".format(cust_tier, trade_type_id))
    charge_list = list(cursor.fetchall())
    charge_amount = charge_list[0][0]

    # Compute assets on margin trades

    acct_assets = 0

    if type_is_margin:
        cursor.execute("select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = '{}'".format(acct_id))
        customer_acc_list = list(cursor.fetchall())
        acct_bal = customer_acc_list[0][0]

        # Should return 0 or 1 row

        cursor.execute(
            "select sum(HS_QTY * LT_PRICE) from HOLDING_SUMMARY, LAST_TRADE where HS_CA_ID = '{}' and LT_S_SYMB = HS_S_SYMB".format(
                acct_id))
        list_hold_assets = list(cursor.fetchall())
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
    return comm_rate, trade_quantity, requested_price, type_is_margin, status_id, trade_type_id, symbol, type_is_market, charge_amount, is_lifo, acct_assets


def frame_4(connection, cursor, acct_id, comm_rate, trade_quantity, requested_price, type_is_margin, status_id, exec_f_name,
            exec_l_name, trade_type_id, symbol, type_is_market, charge_amount, broker_id, is_lifo):
    """Frame-4: Insert the trade"""
    comm_amount = (comm_rate / 100) * trade_quantity * requested_price
    exec_name = exec_f_name + " " + exec_l_name
    if type_is_margin:
        is_cash = 0
    else:
        is_cash = 1

    now = datetime.now()
    now_dts = now.strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("select TH_T_ID from TRADE_HISTORY order by TH_T_ID desc limit 1")
    new_t_id = int(list(cursor.fetchall())[0][0]) + 1

    cursor.execute(
        "insert into TRADE (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH, T_S_SYMB, T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, T_LIFO) "
        "values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(
            new_t_id, now_dts, status_id, trade_type_id, is_cash, symbol, trade_quantity, requested_price, acct_id,
            exec_name, None, charge_amount, comm_amount, 0, is_lifo))
    if not type_is_market:
        cursor.execute("insert into TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB, TR_QTY, TR_BID_PRICE, TR_B_ID) "
                       "values ('{}', '{}', '{}', '{}', '{}', '{}')".format(new_t_id, trade_type_id, symbol,
                                                                            trade_quantity, requested_price, broker_id))
    cursor.execute(
        "insert into TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID) values ('{}', '{}', '{}')".format(new_t_id, now_dts,
                                                                                                 status_id))


def frame_5(connection, cursor, roll_it_back=0):
    """Frame 5"""
    # Intentional rollback of transaction caused by driver (CE).
    if roll_it_back == 1:
        cursor.execute("ROLLBACK")


def frame_6(connection, cursor, type_is_market):
    """Frame 6"""
    cursor.execute("COMMIT")
    if type_is_market:
        eAction = 0
    else:
        eAction = 1
    return eAction


def executor(connection, cursor):
    df_data = {'number_of_transactions': [], 'time_required': []}
    total_time = 0

    cur.execute("PRAGMA journal_mode=WAL;")
    successful_transactions = 0
    for op in range(20):
        start_time = time.time()

        while time.time() < start_time + 60:
            try:
                acct_id, cust_id, cust_f_name, cust_l_name, cust_tier, tax_id, tax_status, broker_id = frame_1(
                    connection, cursor)
                exec_f_name, exec_l_name, exec_tax_id = frame_2(connection, cursor, cust_id, acct_id, cust_f_name, cust_l_name,
                                                                cust_tier, tax_id)
                comm_rate, trade_quantity, requested_price, type_is_margin, status_id, trade_type_id, symbol, type_is_market, charge_amount, is_lifo, acct_assets = frame_3(
                    connection, cursor, cust_id, acct_id, tax_status, cust_tier)
                frame_4(connection, cursor, acct_id, comm_rate, trade_quantity, requested_price, type_is_margin, status_id,
                        exec_f_name, exec_l_name, trade_type_id, symbol, type_is_market, charge_amount, broker_id, is_lifo)
                frame_5(connection, cursor)
                eAction = frame_6(connection, cursor, type_is_market)
                successful_transactions += 1
            except:
                cursor.execute('rollback')

        end_time = time.time()
        total_time = total_time + end_time - start_time
        df_data['time_required'].append((op+1)*60)
        df_data['number_of_transactions'].append(successful_transactions)

    with open("trade_order_inmemory.json", "w") as fp:
        json.dump(df_data, fp)


if __name__ == "__main__":
    executor(conn, cur)
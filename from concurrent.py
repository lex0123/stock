from concurrent.futures import ThreadPoolExecutor
import efinance as ef
import mysql.connector
import numpy as np
import requests
import pandas as pd
from sqlalchemy import create_engine

def get_all_code():
    all_code_url = "http://44.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12&_=1579615221139"
    r = requests.get(all_code_url, timeout=5).json()
    return [data['f12'] for data in r['data']['diff']]

def save_to_mysql(stock_code, start_date, end_date):
    try:
        db = mysql.connector.connect(
            user="gjd",
            password="214791",
            database="gp_data"
        )
        cursor = db.cursor()
        df = ef.stock.get_quote_history(stock_code, beg=start_date, end=end_date)
        for index, row in df.iterrows():
            date = row['日期']
            open = row['开盘']
            close = row['收盘']
            up = row['涨跌额']
            upr = row['涨跌幅']
            low = row['最低']
            high = row['最高']
            vol = row['成交量']
            vola = row['成交额']
            tr = row['换手率']
            sql = """
            INSERT INTO gp_base_info(code, dt, open, close, up, upr, low, high, vol, vola, tr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            close = VALUES(close),
            up = VALUES(up),
            upr = VALUES(upr),
            low = VALUES(low),
            high = VALUES(high),
            vol = VALUES(vol),
            vola = VALUES(vola),
            tr = VALUES(tr)
            """
            values = (stock_code, date, open, close, up, upr, low, high, vol, vola, tr)
            cursor.execute(sql, values)
        db.commit()
    except Exception as e:
        print(f"Error processing stock {stock_code}: {e}")

def save_stock_to_mysql(stock_codes):
    conn = mysql.connector.connect(
        user="gjd",
        password="214791",
        database="gp_data"
    )
    cursor = conn.cursor()
    arr = np.array(stock_codes)
    arrays = np.array_split(arr, 30)
    for codes in arrays:
        df = ef.stock.get_base_info(stock_codes=codes)
        batch_values = []
        for index, row in df.iterrows():
            code = row['股票代码']
            name = row['股票名称']
            retained_profits = row['净利润']
            total_value = row['总市值']
            market_value = row['流通市值']
            industry = row['所处行业']
            dynamic_pe = row['市盈率(动)']
            pb = row['市净率']
            roe = row['ROE']
            margin_rate = row['毛利率']
            net_profit_rate = row['净利率']
            if retained_profits == '-':
                retained_profits = None
            if total_value == '-':
                total_value = None
            if market_value == '-':
                market_value = None
            if industry == '-':
                industry = None
            if dynamic_pe == '-':
                dynamic_pe = None
            if pb == '-':
                pb = None
            if roe == '-':
                roe = None
            if margin_rate == '-':
                margin_rate = None
            if net_profit_rate == '-':
                net_profit_rate = None
            if retained_profits is None and total_value is None and market_value is None and industry is None:
                continue
            batch_values.append((code, name, retained_profits, total_value, market_value, industry, dynamic_pe, pb, roe, margin_rate, net_profit_rate))
            sql = """
                INSERT INTO stock_info(code, name, retained_profits, total_value, market_value, industry, dynamic_pe, pb, roe, margin_rate, net_profit_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                retained_profits = VALUES(retained_profits),
                total_value = VALUES(total_value),
                market_value = VALUES(market_value),
                industry = VALUES(industry),
                dynamic_pe = VALUES(dynamic_pe),
                pb = VALUES(pb),
                roe = VALUES(roe),
                margin_rate = VALUES(margin_rate),
                net_profit_rate = VALUES(net_profit_rate)
                """
        if len(batch_values) > 0:
            cursor.executemany(sql, batch_values)
            conn.commit()
    cursor.close()

def create_stock_info_table():
    conn = mysql.connector.connect(
        user="gjd",
        password="214791",
        database="gp_data"
    )
    cursor = conn.cursor()
    sql = """
    CREATE TABLE IF NOT EXISTS `stock_info` (
        `code` varchar(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '编码',
        `name` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '名称',
        `retained_profits` decimal(18,2) DEFAULT NULL COMMENT '净利润',
        `total_value` decimal(18,2) DEFAULT NULL COMMENT '总市值',
        `market_value` decimal(18,2) DEFAULT NULL COMMENT '流通市值',
        `industry` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '所处行业',
        `dynamic_pe` decimal(15,2) DEFAULT NULL COMMENT '市盈率(动态)',
        `pb` decimal(15,2) DEFAULT NULL COMMENT '市净率',
        `roe` decimal(15,2) DEFAULT NULL COMMENT '净资产收益率，它是衡量公司盈利能力的一个重要财务指标，表示公司利用股东权益（净资产）创造利润的能力，最好在同行业内进行比较',
        `margin_rate` decimal(15,2) DEFAULT NULL COMMENT '毛利率',
        `net_profit_rate` decimal(15,2) DEFAULT NULL COMMENT '净利率',
        PRIMARY KEY (`code`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def create_gp_base_info_table():
    conn = mysql.connector.connect(
        user="gjd",
        password="214791",
        database="gp_data"
    )
    cursor = conn.cursor()
    sql = """
    CREATE TABLE IF NOT EXISTS `gp_base_info` (
        `code` varchar(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '编码',
        `dt` date NOT NULL COMMENT '日期',
        `open` decimal(15,2) DEFAULT NULL COMMENT '开盘价',
        `close` decimal(15,2) DEFAULT NULL COMMENT '收盘价',
        `up` decimal(15,2) DEFAULT NULL COMMENT '涨跌',
        `upr` decimal(15,2) DEFAULT NULL COMMENT '涨幅百分比',
        `low` decimal(15,2) DEFAULT NULL COMMENT '最低价',
        `high` decimal(15,2) DEFAULT NULL COMMENT '最高价',
        `vol` bigint DEFAULT NULL COMMENT '成交量',
        `vola` decimal(15,2) DEFAULT NULL COMMENT '成交额',
        `tr` decimal(15,2) DEFAULT NULL COMMENT '换手率，百分比',
        PRIMARY KEY (`code`, `dt`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def export_gp_base_info_to_excel(file_path):
    engine = create_engine('mysql+mysqlconnector://gjd:214791@localhost/gp_data')
    query = "SELECT * FROM gp_base_info"
    df = pd.read_sql(query, engine)
    df.to_excel(file_path, index=False)

def export_stock_info_to_excel(file_path):
    engine = create_engine('mysql+mysqlconnector://gjd:214791@localhost/gp_data')
    query = "SELECT * FROM stock_info"
    df = pd.read_sql(query, engine)
    df.to_excel(file_path, index=False)

# 创建表
create_stock_info_table()
create_gp_base_info_table()

# # 获取所有股票代码
# stock_codes = get_all_code()
# # 保存股票基本信息
# save_stock_to_mysql(stock_codes)
# start_date = '20240220'
# end_date = '20250221'
# # 使用多线程处理每个股票代码
# with ThreadPoolExecutor(max_workers=12) as executor:
#     executor.map(lambda code: save_to_mysql(code, start_date, end_date), stock_codes)

# export_gp_base_info_to_excel('/home/lizheng/桌面/stock analyse/gp_base_info.xlsx')
# export_stock_info_to_excel('/home/lizheng/桌面/stock analyse/stock_info.xlsx')

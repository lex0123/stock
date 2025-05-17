import re
import sqlite3
import mysql.connector
import requests
import json
import pandas as pd
from sqlalchemy import create_engine

def export_gp_base_info_to_excel(file_path):
    conn = sqlite3.connect('gp_data.db')
    query = "SELECT * FROM stock_data"
    df = pd.read_sql(query, conn)
    print(df)   
    df.to_excel(file_path, index=False)

def get_stock():
    count = 0
    db = sqlite3.connect('gp_data.db')
    cursor = db.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data(
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_name VARCHAR(255) NOT NULL,
            stock_code VARCHAR(255) NOT NULL UNIQUE,
            stock_price_endtoday FLOAT NOT NULL,
            stock_ration_change FLOAT NOT NULL,
            the_price_change FLOAT NOT NULL,
            the_trading_volume INT NOT NULL,
            the_trading_amount FLOAT NOT NULL,
            stock_price_max FLOAT NOT NULL,
            stock_price_min FLOAT NOT NULL,
            stock_price_begintoday FLOAT NOT NULL,
            stock_price_endyestoday FLOAT NOT NULL,
            stock_Volume_Ratio FLOAT NOT NULL, 
            stock_Turnover_Rate FLOAT NOT NULL,
            stock_pe FLOAT NOT NULL,
            stock_pb FLOAT NOT NULL
        )
    ''')
    for i in range(1, 287):
        url = f'https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&cb=jQuery37109183077648297867_1742228027785&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f7,f15,f18,f16,f17,f10,f8,f9,f23&fid=f3&pn={i}&pz=20&po=1&dect=1&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=|0|0|0|'
        response = requests.get(url, timeout=5).text
        json_str = re.search(r'\((.*)\)', response).group(1)
        try:
            data = json.loads(json_str)
            for stock in data['data']['diff']:
                # 检查并处理无效的值
                stock_name = stock['f14']
                stock_code = stock['f12']
                stock_price_endtoday = round(0.01 * float(stock['f2']), 2) if stock['f2'] != '-' else 0
                stock_ration_change = round(0.01 * float(stock['f3']), 2) if stock['f3'] != '-' else 0
                the_price_change = round(0.01 * float(stock['f4']), 2) if stock['f4'] != '-' else 0
                the_trading_volume = int(stock['f5']) if stock['f5'] != '-' else 0
                the_trading_amount = round(0.01 * float(stock['f6']), 2) if stock['f6'] != '-' else 0.0
                stock_price_max = round(0.01 * float(stock['f15']), 2) if stock['f15'] != '-' else 0
                stock_price_min = round(0.01 * float(stock['f16']), 2) if stock['f16'] != '-' else 0
                stock_price_begintoday = round(0.01 * float(stock['f17']), 2) if stock['f17'] != '-' else 0
                stock_price_endyestoday = int(stock['f18']) if stock['f18'] != '-' else 0
                stock_Volume_Ratio = round(0.01 * float(stock['f10']), 2) if stock['f10'] != '-' else 0
                stock_Turnover_Rate = round(0.01 * float(stock['f8']), 2) if stock['f8'] != '-' else 0
                stock_pe = round(0.01 * float(stock['f9']), 2) if stock['f9'] != '-' else 0
                stock_pb = round(0.01 * float(stock['f23']), 2) if stock['f23'] != '-' else 0
                print(stock_name, stock_code, stock_price_endtoday, stock_ration_change, the_price_change, the_trading_volume, the_trading_amount, stock_price_max, stock_price_min, stock_price_begintoday, stock_price_endyestoday, stock_Volume_Ratio, stock_Turnover_Rate, stock_pe, stock_pb)
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_data(
                        stock_name,
                        stock_code,
                        stock_price_endtoday,
                        stock_ration_change,
                        the_price_change,
                        the_trading_volume,
                        the_trading_amount,
                        stock_price_max,
                        stock_price_min,
                        stock_price_begintoday,
                        stock_price_endyestoday,
                        stock_Volume_Ratio,
                        stock_Turnover_Rate,
                        stock_pe,
                        stock_pb
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    stock_name,
                    stock_code,
                    stock_price_endtoday,
                    stock_ration_change,
                    the_price_change,
                    the_trading_volume,
                    the_trading_amount,
                    stock_price_max,
                    stock_price_min,
                    stock_price_begintoday,
                    stock_price_endyestoday,
                    stock_Volume_Ratio,
                    stock_Turnover_Rate,
                    stock_pe,
                    stock_pb
                ))
        except json.JSONDecodeError as e:
            print(f"JSON解码错误: {e}")
            print("响应内容不是有效的JSON格式")
    cursor.close()
    db.commit()
    db.close()

if __name__ == "__main__":
    get_stock()
    export_gp_base_info_to_excel('./stock_data.xlsx')

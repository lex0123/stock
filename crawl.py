import os
import tushare as ts
import pandas as pd
import pymysql
from sqlalchemy import create_engine

# 推荐用环境变量保存token，避免明文写入代码
# 设置环境变量方法（仅需一次）：set TUSHARE_TOKEN=你的token
token = os.getenv('TUSHARE_TOKEN')
if not token:
    raise ValueError("请先设置环境变量 TUSHARE_TOKEN")

# 初始化pro接口
pro = ts.pro_api(token)
df = pro.daily(ts_code='000001.SZ,600000.SH', start_date='20180701', end_date='20180718')

# # MySQL数据库配置
# user = 'root'
# password = '你的mysql密码'
# host = 'localhost'
# port = 3306
# database = 'stockdb'

# # 创建数据库连接
# engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")

# # 保存数据到MySQL表（表名为daily，不存在会自动创建）
# df.to_sql('daily', engine, if_exists='replace', index=False)

print("数据已保存到 MySQL 的 daily 表中")
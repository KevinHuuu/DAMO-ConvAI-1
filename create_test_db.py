#!/usr/bin/env python3
import os
import sqlite3

# 创建金融数据库示例
financial_db_path = '/Users/changranh/Desktop/bird2/DAMO-ConvAI/bird/data/dev_databases/financial/financial.sqlite'

# 确保父目录存在
os.makedirs(os.path.dirname(financial_db_path), exist_ok=True)

# 创建数据库连接
conn = sqlite3.connect(financial_db_path)
cursor = conn.cursor()

# 创建交易表
cursor.execute('''
CREATE TABLE Transactions (
    transaction_id INTEGER PRIMARY KEY,
    date TEXT,
    amount REAL,
    description TEXT,
    category TEXT
)
''')

# 插入一些示例数据
transactions = [
    (1, '2023-01-01', 100.50, 'Grocery shopping', 'Food'),
    (2, '2023-01-02', 50.25, 'Gas station', 'Transportation'),
    (3, '2023-01-03', 200.00, 'Rent payment', 'Housing'),
    (4, '2023-01-04', 75.00, 'Restaurant', 'Food'),
    (5, '2023-01-05', 120.75, 'Utilities', 'Housing')
]

cursor.executemany('INSERT INTO Transactions VALUES (?, ?, ?, ?, ?)', transactions)

# 提交更改并关闭连接
conn.commit()
conn.close()

print(f"创建了示例数据库: {financial_db_path}")

# 创建标准答案SQL文件
gold_sql_dir = '/Users/changranh/Desktop/bird2/DAMO-ConvAI/bird/data'
os.makedirs(gold_sql_dir, exist_ok=True)

with open(os.path.join(gold_sql_dir, 'dev_gold.sql'), 'w') as f:
    f.write("SELECT COUNT(*) FROM Transactions\tfinancial\n")
    f.write("SELECT SUM(amount) FROM Transactions\tfinancial\n")

print(f"创建了标准答案SQL文件: {os.path.join(gold_sql_dir, 'dev_gold.sql')}")

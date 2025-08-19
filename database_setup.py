# -*- coding: utf-8 -*-
import sqlite3

# --- 数据库设置 ---
DATABASE_NAME = "csgo_market_data.db"

def create_connection():
    """ 创建一个到SQLite数据库的连接 """
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        print(f"✅ 成功连接到数据库 '{DATABASE_NAME}'")
        return conn
    except sqlite3.Error as e:
        print(f"❌ 数据库连接失败: {e}")
        return None

def create_table(conn):
    """ 在数据库中创建价格历史记录表 """
    # 新的表结构：不再有items表，直接存储market_hash_name
    # 增加了一个索引(index)在 market_hash_name 和 timestamp 上，能极大提高后续按饰品和时间查询的效率
    sql_create_price_history_table = """
    CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_hash_name TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        platform TEXT NOT NULL,
        sell_price REAL,
        sell_count INTEGER,
        bidding_price REAL,
        bidding_count INTEGER
    );
    """
    
    sql_create_index = """
    CREATE INDEX IF NOT EXISTS idx_name_time ON price_history (market_hash_name, timestamp);
    """
    
    try:
        cursor = conn.cursor()
        print("--- 正在创建 'price_history' 表 ---")
        cursor.execute(sql_create_price_history_table)
        print("✔️ 'price_history' 表已创建。")
        
        print("\n--- 正在为查询优化创建索引 ---")
        cursor.execute(sql_create_index)
        print("✔️ 性能索引已创建。")

    except sqlite3.Error as e:
        print(f"❌ 数据库操作失败: {e}")

def main():
    """ 主函数，用于创建数据库和表 """
    conn = create_connection()
    if conn:
        create_table(conn)
        conn.close()
        print("\n✅ 数据库初始化完成，连接已关闭。")

if __name__ == '__main__':
    main()

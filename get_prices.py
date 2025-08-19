# -*- coding: utf-8 -*-
import requests
import json
import os
import sqlite3
from datetime import datetime
try:
    from config import API_KEY
except ImportError:
    API_KEY = ""

# 导入成交量获取功能
from get_sales import get_multiple_items_sales_volume

# --- 全局设置 ---
DATABASE_NAME = "csgo_market_data.db"
BASE_URL = "https://open.steamdt.com"
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
WATCHLIST_FILE = "watchlist.txt"

# read_watchlist, get_prices_batch, filter_price_data 函数与上一版完全相同，此处省略以保持简洁
# 您可以直接复用上一版中的这三个函数，无需修改

def read_watchlist(filepath: str) -> list[str]:
    """从指定的文本文件中读取待查询的 marketHashName 列表。"""
    if not os.path.exists(filepath):
        print(f"❌ 错误：找不到关注列表文件 '{filepath}'。")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines()]
        item_names = [line for line in lines if line and not line.startswith('#')]
    return item_names

def get_prices_batch(market_hash_names: list[str]):
    """通过 'marketHashName' 批量查询饰品价格。"""
    if not market_hash_names: return None
    print(f"ℹ️  准备为 {len(market_hash_names)} 个饰品批量查询价格...")
    endpoint = "/open/cs2/v1/price/batch"
    payload = {"marketHashNames": market_hash_names}
    try:
        response = requests.post(BASE_URL + endpoint, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            print("✅ API价格查询成功。")
            return data.get("data", [])
        else:
            print(f"❌ API返回错误：{data.get('errorMsg')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求API时发生网络错误：{e}")
        return None

def filter_price_data(price_data: list) -> list:
    """根据指定规则筛选价格数据：使用YOUPIN的sell_price和BUFF的bidding_price。"""
    if not price_data: return []
    print("ℹ️  正在根据规则筛选平台数据（使用YOUPIN的sell_price和BUFF的bidding_price）...")
    filtered_list = []
    for item_data in price_data:
        market_hash_name = item_data.get("marketHashName")
        data_list = item_data.get("dataList", [])
        youpin_data, buff_data = None, None
        
        # 提取YOUPIN和BUFF平台数据
        for platform_data in data_list:
            if platform_data.get("platform") == "YOUPIN": 
                youpin_data = platform_data
            elif platform_data.get("platform") == "BUFF": 
                buff_data = platform_data
        
        # 检查是否同时有YOUPIN和BUFF数据
        if youpin_data and buff_data:
            # 创建混合数据：使用YOUPIN的sell_price和相关信息，BUFF的bidding_price，YOUPIN的订单量
            mixed_data = {
                "platform": "MIXED",
                "platformItemId": youpin_data.get("platformItemId", ""),
                "sellPrice": youpin_data.get("sellPrice", 0),
                "sellCount": youpin_data.get("sellCount", 0),
                "biddingPrice": buff_data.get("biddingPrice", 0),
                "biddingCount": youpin_data.get("biddingCount", 0),  # 使用YOUPIN的求购量
                "updateTime": max(youpin_data.get("updateTime", 0), buff_data.get("updateTime", 0))
            }
            
            # 验证数据有效性并调整价格
            sell_price = mixed_data.get("sellPrice", 0)
            bidding_price = mixed_data.get("biddingPrice", 0)
            
            if sell_price > 0 and bidding_price > 0:
                # 如果求购价高于或等于在售价，设置为在售价-1
                if bidding_price >= sell_price:
                    bidding_price = sell_price - 1
                    print(f"⚠️  {market_hash_name}: 求购价调整为{bidding_price}")
                
                new_item_data = {
                    "marketHashName": market_hash_name, 
                    "dataList": [mixed_data]
                }
                filtered_list.append(new_item_data)
                print(f"✅ {market_hash_name}: YOUPIN卖价{sell_price} + BUFF买价{bidding_price}")
            else:
                print(f"⚠️  {market_hash_name}: 数据无效（卖价{sell_price}, 买价{bidding_price}）")
        else:
            print(f"❌ {market_hash_name}: 缺少YOUPIN或BUFF数据")
    
    print(f"✅ 数据筛选完成，有效数据：{len(filtered_list)}条")
    return filtered_list

def save_data_to_db(filtered_data: list, sales_volume_data: dict = None):
    """
    将筛选后的数据保存到 SQLite 数据库的 'price_history' 表，包含成交量信息。
    """
    if not filtered_data:
        print("ℹ️  没有数据可以保存到数据库。")
        return

    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # 检查是否存在sales_volume列，如果不存在则添加
        cursor.execute("PRAGMA table_info(price_history)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'sales_volume' not in columns:
            cursor.execute("ALTER TABLE price_history ADD COLUMN sales_volume TEXT")
            conn.commit()
            print("✅ 已添加sales_volume列到数据库表")
        
        print(f"ℹ️  正在将 {len(filtered_data)} 条筛选后的饰品数据写入数据库...")
        
        current_timestamp = int(datetime.now().timestamp())
        
        records_to_insert = []
        for item in filtered_data:
            market_hash_name = item['marketHashName']
            # 获取该饰品的成交量数据
            sales_volume = sales_volume_data.get(market_hash_name, "未能获取") if sales_volume_data else "未能获取"
            
            for platform_data in item['dataList']:
                # 准备一条要插入的记录（包含成交量）
                record = (
                    market_hash_name,
                    current_timestamp,
                    platform_data.get('platform'),
                    platform_data.get('sellPrice'),
                    platform_data.get('sellCount'),
                    platform_data.get('biddingPrice'),
                    platform_data.get('biddingCount'),
                    sales_volume,
                )
                records_to_insert.append(record)
        
        # 使用 executemany 批量插入，效率更高
        if records_to_insert:
            sql = """
            INSERT INTO price_history (market_hash_name, timestamp, platform, sell_price, sell_count, bidding_price, bidding_count, sales_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(sql, records_to_insert)
            conn.commit()
            print(f"✅ 成功将 {len(records_to_insert)} 条价格记录（含成交量）写入数据库。")

    except sqlite3.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if conn:
            conn.close()

# --- 主程序执行区 ---
if __name__ == "__main__":
    print("\n" + "="*22 + " 任务：采集、筛选并存储价格数据（含成交量） " + "="*22)
    if not API_KEY:
        print("🛑 错误：请先在 config.py 文件中填写您的 API_KEY。")
    else:
        target_items = read_watchlist(WATCHLIST_FILE)
        if target_items:
            # 获取价格数据
            raw_data = get_prices_batch(target_items)
            if raw_data:
                filtered_data = filter_price_data(raw_data)
                
                # 获取成交量数据
                print("\n" + "="*22 + " 开始获取饰品成交量数据 " + "="*22)
                sales_volume_data = get_multiple_items_sales_volume(target_items)
                
                # 保存所有数据到数据库
                save_data_to_db(filtered_data, sales_volume_data)
                
                # 显示成交量获取结果
                print(f"\n{'='*22} 成交量获取结果汇总 {'='*22}")
                for item, volume in sales_volume_data.items():
                    print(f"{item}: {volume}")
    
    print("\n🎉  任务执行完毕。")

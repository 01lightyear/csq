import requests
import json
import sqlite3
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

# 数据库设置
DATABASE_NAME = "kline.db"
WATCHLIST_FILE = "watchlist.txt"
ALL_ITEMS_CACHE_FILE = "all_items_cache.json"

# API设置
API_URL = 'https://api.steamdt.com/user/steam/category/v1/kline'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://steamdt.com/'
}

def load_all_items_cache() -> Dict[str, str]:
    """加载all_items_cache.json并建立market_hash_name到C5平台typeVal的映射"""
    if not os.path.exists(ALL_ITEMS_CACHE_FILE):
        print(f"❌ 错误：找不到all_items_cache文件 '{ALL_ITEMS_CACHE_FILE}'")
        return {}
    
    mapping = {}
    try:
        with open(ALL_ITEMS_CACHE_FILE, 'r', encoding='utf-8') as f:
            items_data = json.load(f)
            
        for item in items_data:
            market_hash_name = item.get('marketHashName')
            platform_list = item.get('platformList', [])
            
            if market_hash_name:
                # 在platformList中查找name为C5的平台
                for platform in platform_list:
                    if platform.get('name') == 'C5':
                        type_val = platform.get('itemId')
                        if type_val:
                            mapping[market_hash_name] = type_val
                            break
        
        print(f"✅ 已加载 {len(mapping)} 个物品的C5平台typeVal映射")
        return mapping
        
    except json.JSONDecodeError as e:
        print(f"❌ 解析all_items_cache文件失败: {e}")
        return {}
    except Exception as e:
        print(f"❌ 加载all_items_cache文件时发生错误: {e}")
        return {}

def load_watchlist() -> List[str]:
    """加载watchlist中的物品名称"""
    if not os.path.exists(WATCHLIST_FILE):
        print(f"❌ 错误：找不到watchlist文件 '{WATCHLIST_FILE}'")
        return []
    
    with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
        items = [line.strip() for line in f.readlines() if line.strip() and not line.startswith('#')]
    
    print(f"✅ 已加载 {len(items)} 个待查询物品")
    return items

def create_database():
    """创建kline数据库和表"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # 创建kline_data表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS kline_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_hash_name TEXT NOT NULL,
            type_val TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open_price REAL NOT NULL,
            close_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            volume REAL NOT NULL,
            turnover REAL NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 创建索引以提高查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_market_timestamp ON kline_data(market_hash_name, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_type_val ON kline_data(type_val)')
        
        conn.commit()
        print("✅ 数据库初始化成功")
        
    except sqlite3.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if conn:
            conn.close()

def get_latest_timestamp(market_hash_name: str) -> Optional[int]:
    """获取指定物品的最新时间戳"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT MAX(timestamp) FROM kline_data 
        WHERE market_hash_name = ?
        ''', (market_hash_name,))
        
        result = cursor.fetchone()
        return result[0] if result[0] is not None else None
        
    except sqlite3.Error as e:
        print(f"❌ 查询最新时间戳失败: {e}")
        return None
    finally:
        if conn:
            conn.close()

def is_database_empty() -> bool:
    """检查数据库是否为空"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM kline_data')
        count = cursor.fetchone()[0]
        return count == 0
        
    except sqlite3.Error as e:
        print(f"❌ 检查数据库状态失败: {e}")
        return True
    finally:
        if conn:
            conn.close()

def adjust_to_beijing_midnight(timestamp_ms: int) -> int:
    """
    将时间戳调整为最近的过去北京时间24点（午夜0点）
    北京时间 = UTC+8
    """
    # 将毫秒时间戳转换为秒
    timestamp_sec = timestamp_ms // 1000
    
    # 转换为datetime对象（UTC时间）
    dt_utc = datetime.fromtimestamp(timestamp_sec, timezone.utc)
    
    # 转换为北京时间（UTC+8）
    dt_beijing = dt_utc + timedelta(hours=8)
    
    # 调整到北京时间的24点（午夜0点）- 总是过去的24点
    dt_beijing_midnight = dt_beijing.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 转换回UTC时间戳
    dt_utc_midnight = dt_beijing_midnight - timedelta(hours=8)
    
    # 返回毫秒时间戳
    return int(dt_utc_midnight.timestamp() * 1000)

def get_kline_data(type_val: str, max_time: Optional[int] = None) -> Optional[List]:
    """获取K线数据"""
    # 直接将查询时间戳设置为过去最近的北京时间24点
    current_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    query_timestamp = adjust_to_beijing_midnight(current_timestamp)
    
    query_params = {
        'timestamp': str(query_timestamp),
        'type': '2',
        'platform': 'ALL',
        'specialStyle': ''
    }
    
    # 根据是否首次创建设置maxTime
    if max_time:
        query_params['maxTime'] = str(max_time)
    
    query_params['typeVal'] = type_val
    
    try:
        print(f"正在请求typeVal: {type_val} 的K线数据...")
        response = requests.get(API_URL, headers=HEADERS, params=query_params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if data.get('success'):
            kline_list = data.get('data', [])
            print(f"✅ 成功获取 {len(kline_list)} 条K线数据")
            return kline_list
        else:
            print(f"❌ API返回错误: {data.get('errorMsg')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求API时发生网络错误: {e}")
        return None
    except Exception as e:
        print(f"❌ 处理数据时发生未知错误: {e}")
        return None

def save_kline_data(market_hash_name: str, type_val: str, kline_data: List) -> int:
    """保存K线数据到数据库"""
    if not kline_data:
        return 0
    
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        total_saved = 0
        
        # 过滤掉最后一个实时数据（非每日数据），只保存完整日K线数据
        if len(kline_data) > 1:
            # 保存除最后一个外的所有历史日K线数据
            historical_data = kline_data[:-1]
        else:
            # 如果只有一个数据，可能是历史数据，直接使用
            historical_data = kline_data
        
        # 按时间戳排序，从旧到新保存
        sorted_data = sorted(historical_data, key=lambda x: int(x[0]))
        
        for daily_data in sorted_data:
            # 解析K线数据
            timestamp_raw, open_price, close_price, high_price, low_price, volume, turnover = daily_data
            
            # 转换时间戳为整数
            timestamp_int = int(timestamp_raw)
            
            # 自动检测时间戳格式：如果数值小于2000000000，认为是秒级时间戳，否则是毫秒级
            if timestamp_int < 3000000000:
                # 秒级时间戳，需要转换为毫秒级
                timestamp_ms = timestamp_int * 1000
            else:
                # 毫秒级时间戳
                timestamp_ms = timestamp_int
            
            # 调整到北京时间24点
            timestamp_sec = adjust_to_beijing_midnight(timestamp_ms) // 1000
            
            # 处理可能为None或空值的情况
            volume = volume if volume is not None and volume != '' else 0.0
            turnover = turnover if turnover is not None and turnover != '' else 0.0
            
            # 检查是否已存在该时间戳的数据
            cursor.execute('''
            SELECT COUNT(*) FROM kline_data 
            WHERE market_hash_name = ? AND timestamp = ?
            ''', (market_hash_name, timestamp_sec))
            
            if cursor.fetchone()[0] > 0:
                continue  # 跳过已存在的数据
            
            # 插入新数据
            cursor.execute('''
            INSERT INTO kline_data 
            (market_hash_name, type_val, timestamp, open_price, close_price, high_price, low_price, volume, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (market_hash_name, type_val, timestamp_sec, open_price, close_price, high_price, low_price, volume, turnover))
            
            total_saved += 1
        
        conn.commit()
        if total_saved > 0:
            print(f"✅ 已保存 {market_hash_name} 的 {total_saved} 条K线数据")
        else:
            print(f"⚠️  {market_hash_name} 无新数据需要保存")
        return total_saved
        
    except sqlite3.Error as e:
        print(f"❌ 保存K线数据失败: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def process_all_items():
    """处理所有物品的K线数据"""
    print("开始处理K线数据采集...")
    
    # 加载必要的数据
    typeval_mapping = load_all_items_cache()
    watchlist = load_watchlist()
    
    if not typeval_mapping or not watchlist:
        print("❌ 无法加载必要数据，退出")
        return
    
    # 检查数据库状态
    is_empty = is_database_empty()
    max_time = 1735488000 if is_empty else None  # 2025.1.1的时间戳
    
    if is_empty:
        print("📊 首次创建数据库，将获取从2025.1.1至今的历史数据")
    else:
        print("📊 数据库已存在，将获取最新的增量数据")
    
    total_saved = 0
    
    for item_name in watchlist:
        print(f"\n{'='*60}")
        print(f"正在处理: {item_name}")
        print('='*60)
        
        if item_name not in typeval_mapping:
            print(f"❌ 找不到 {item_name} 的C5平台typeVal映射，跳过")
            continue
        
        type_val = typeval_mapping[item_name]
        
        # 获取K线数据
        kline_data = get_kline_data(type_val, max_time)
        if not kline_data:
            print(f"❌ 无法获取 {item_name} 的K线数据")
            continue
        
        # 保存数据
        saved_count = save_kline_data(item_name, type_val, kline_data)
        total_saved += saved_count
        
        if saved_count > 0:
            print(f"✅ {item_name} 处理完成")
        else:
            print(f"⚠️  {item_name} 无新数据需要保存")
        
        # 添加延迟以避免API频率限制
        import time
        time.sleep(3)
    
    print(f"\n{'='*60}")
    print(f"处理完成！总共保存了 {total_saved} 条K线数据")
    print('='*60)

def main():
    """主函数"""
    print("K线数据采集系统")
    print("="*60)
    
    # 创建数据库
    create_database()
    
    # 处理所有物品
    process_all_items()
    
    print("\n🎉 K线数据采集完成")

if __name__ == "__main__":
    main()
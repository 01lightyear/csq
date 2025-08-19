import requests
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

# 数据库设置
DATABASE_NAME = "market_index.db"

# API设置
API_URL = 'https://api.steamdt.com/user/statistics/v2/chart'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://steamdt.com/'
}

def create_database():
    """创建market_index数据库和表"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # 创建market_index表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            index_value REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 创建索引以提高查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON market_index(timestamp)')
        
        conn.commit()
        print("✅ 数据库初始化成功")
        
    except sqlite3.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if conn:
            conn.close()

def save_index_to_db(index_value, timestamp):
    """将大盘指数保存到数据库"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # 检查是否已存在该时间戳的数据
        cursor.execute('''
        SELECT COUNT(*) FROM market_index WHERE timestamp = ?
        ''', (timestamp,))
        
        if cursor.fetchone()[0] > 0:
            print(f"⚠️  时间戳 {timestamp} 的数据已存在，跳过")
            return False
        
        cursor.execute('''
        INSERT INTO market_index (index_value, timestamp)
        VALUES (?, ?)
        ''', (index_value, timestamp))
        
        conn.commit()
        print(f"✅ 大盘指数 {index_value} (时间戳: {timestamp}) 已保存到数据库")
        return True
        
    except sqlite3.Error as e:
        print(f"❌ 保存数据失败: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_latest_timestamp():
    """获取数据库中最新的时间戳"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT MAX(timestamp) FROM market_index')
        result = cursor.fetchone()
        return result[0] if result[0] is not None else None
        
    except sqlite3.Error as e:
        print(f"❌ 查询最新时间戳失败: {e}")
        return None
    finally:
        if conn:
            conn.close()

def is_database_empty():
    """检查数据库是否为空"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM market_index')
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

def get_market_index_data() -> Optional[List]:
    """获取大盘指数数据"""
    # 直接将查询时间戳设置为过去最近的北京时间24点
    current_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    query_timestamp = adjust_to_beijing_midnight(current_timestamp)
    
    query_params = {
        'timestamp': str(query_timestamp),
        'type': '2',
        'dateType': '4'
    }
    
    try:
        print(f"正在请求大盘指数数据...")
        response = requests.get(API_URL, headers=HEADERS, params=query_params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if data.get('success'):
            index_list = data.get('data', [])
            print(f"✅ 成功获取 {len(index_list)} 条大盘指数数据")
            return index_list
        else:
            print(f"❌ API返回错误: {data.get('errorMsg')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求API时发生网络错误: {e}")
        return None
    except Exception as e:
        print(f"❌ 处理数据时发生未知错误: {e}")
        return None

def adjust_existing_timestamps():
    """调整现有数据库中的时间戳到最接近的北京24点"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # 获取所有现有数据
        cursor.execute('SELECT id, index_value, timestamp FROM market_index ORDER BY timestamp')
        existing_data = cursor.fetchall()
        
        updated_count = 0
        
        for record_id, index_value, old_timestamp in existing_data:
            # 调整时间戳
            adjusted_timestamp = adjust_to_beijing_midnight(old_timestamp * 1000) // 1000
            
            # 如果时间戳有变化，则更新
            if adjusted_timestamp != old_timestamp:
                cursor.execute('''
                UPDATE market_index 
                SET timestamp = ? 
                WHERE id = ?
                ''', (adjusted_timestamp, record_id))
                updated_count += 1
        
        conn.commit()
        print(f"✅ 已调整 {updated_count} 条记录的时间戳到最接近的北京24点")
        
    except sqlite3.Error as e:
        print(f"❌ 调整时间戳失败: {e}")
    finally:
        if conn:
            conn.close()

def save_market_index_data(index_data: List) -> int:
    """保存大盘指数数据到数据库"""
    if not index_data:
        return 0
    
    total_saved = 0
    
    # 按时间戳排序，从旧到新保存
    sorted_data = sorted(index_data, key=lambda x: int(x[0]))
    
    for timestamp_raw, index_value in sorted_data:
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
        
        # 直接保存，跳过已存在的
        if save_index_to_db(index_value, timestamp_sec):
            total_saved += 1
    
    return total_saved

def main():
    """主函数"""
    print("大盘指数数据采集系统")
    print("="*60)
    
    # 创建数据库
    create_database()
    
    # 调整现有数据库中的时间戳到最接近的北京24点
    print("🔄 调整现有数据时间戳...")
    adjust_existing_timestamps()
    
    # 检查数据库状态
    is_empty = is_database_empty()
    
    if is_empty:
        print("📊 首次创建数据库，将获取当前大盘指数数据")
    else:
        print("📊 数据库已存在，将获取最新的大盘指数数据")
    
    # 获取大盘指数数据
    index_data = get_market_index_data()
    if not index_data:
        print("❌ 无法获取大盘指数数据")
        return
    
    # 保存数据
    total_saved = save_market_index_data(index_data)
    
    print(f"\n{'='*60}")
    print(f"处理完成！总共保存了 {total_saved} 条大盘指数数据")
    print('='*60)

if __name__ == "__main__":
    main()
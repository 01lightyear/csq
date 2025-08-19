#!/usr/bin/env python3
import sqlite3
from datetime import datetime, timedelta

# 连接数据库
conn = sqlite3.connect('kline.db')
cursor = conn.cursor()

# 选择一个物品进行详细分析
item_name = 'M4A1-S | Printstream (Factory New)'

print(f'详细分析物品: {item_name}')
print('=' * 60)

# 获取所有时间戳并排序
cursor.execute('''
SELECT timestamp, open_price, close_price 
FROM kline_data 
WHERE market_hash_name = ? 
ORDER BY timestamp
''', (item_name,))

records = cursor.fetchall()
timestamps = [record[0] for record in records]

if timestamps:
    print(f'总记录数: {len(timestamps)}')
    print(f'最早时间: {datetime.fromtimestamp(timestamps[0]).strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'最新时间: {datetime.fromtimestamp(timestamps[-1]).strftime("%Y-%m-%d %H:%M:%S")}')
    
    # 计算时间跨度
    time_span_days = (timestamps[-1] - timestamps[0]) / (24 * 3600)
    print(f'时间跨度: {time_span_days:.1f} 天')
    print(f'应有记录数: {int(time_span_days) + 1}')
    print(f'实际记录数: {len(timestamps)}')
    print(f'缺失记录数: {int(time_span_days) + 1 - len(timestamps)}')
    
    print('\n检查数据连续性:')
    print('-' * 40)
    
    # 检查连续性
    expected_timestamp = timestamps[0]
    missing_periods = []
    
    for i, ts in enumerate(timestamps):
        if ts != expected_timestamp:
            # 发现缺失
            missing_start = expected_timestamp
            missing_end = ts - 86400  # 前一天
            
            if missing_end >= missing_start:
                missing_periods.append((missing_start, missing_end))
            
            print(f'位置 {i+1}: 期望 {datetime.fromtimestamp(expected_timestamp).strftime("%Y-%m-%d")}, 实际 {datetime.fromtimestamp(ts).strftime("%Y-%m-%d")}')
        
        expected_timestamp = ts + 86400  # 下一天
    
    if missing_periods:
        print(f'\n发现 {len(missing_periods)} 个缺失期间:')
        for start, end in missing_periods:
            start_date = datetime.fromtimestamp(start).strftime("%Y-%m-%d")
            end_date = datetime.fromtimestamp(end).strftime("%Y-%m-%d")
            days_missing = (end - start) / 86400 + 1
            print(f'  {start_date} 至 {end_date} (缺失 {days_missing:.0f} 天)')
    else:
        print('✅ 数据连续，无缺失')
    
    # 显示前10条和后10条记录
    print(f'\n前10条记录:')
    for i, ts in enumerate(timestamps[:10]):
        date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        print(f'  {i+1:2d}. {date}')
    
    print(f'\n后10条记录:')
    for i, ts in enumerate(timestamps[-10:], len(timestamps)-9):
        date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        print(f'  {i:2d}. {date}')

else:
    print('❌ 未找到数据')

conn.close()
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import quote

def encode_market_hash_name(market_hash_name):
    """将market_hash_name编码为URL格式"""
    # 替换特殊字符
    encoded = market_hash_name.replace(' ', '%20').replace('|', '%7C')
    return quote(encoded, safe='%')

def get_item_sales_volume(market_hash_name):
    """获取指定饰品的成交量"""
    # 编码饰品名称
    encoded_name = encode_market_hash_name(market_hash_name)
    url = f'https://steamdt.com/cs2/{encoded_name}'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        print(f"正在请求页面: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        print("请求成功，正在解析页面...")

        soup = BeautifulSoup(response.text, 'html.parser')

        # 定位并提取成交量数据
        volume_label_element = soup.find(string=re.compile("今日成交"))
        
        volume = "未能找到成交量信息"  # 设置一个默认值

        if volume_label_element:
            print("已成功定位到'今日成交'标签。")
            volume_element = volume_label_element.find_next_sibling('span')
            
            if volume_element:
                volume = volume_element.get_text(strip=True)
                print("已成功提取成交量数值。")
            else:
                print("找到了'今日成交'标签，但未能找到其对应的数值元素。")
        else:
            print("未能在页面中定位到'今日成交'标签，可能是网站结构已更新。")

        print(f"\n--- 数据获取成功 ---")
        print(f"饰品: {market_hash_name}")
        print(f"今日成交量: {volume}")
        
        return volume
        
    except requests.exceptions.RequestException as e:
        print(f"\n请求网页时发生网络错误: {e}")
        return None
    except Exception as e:
        print(f"\n处理数据时发生未知错误: {e}")
        return None

def get_multiple_items_sales_volume(market_hash_names):
    """批量获取多个饰品的成交量"""
    results = {}
    
    for item_name in market_hash_names:
        print(f"\n{'='*50}")
        print(f"正在处理饰品: {item_name}")
        print('='*50)
        
        volume = get_item_sales_volume(item_name)
        if volume:
            results[item_name] = volume
    
    return results

# 示例使用
if __name__ == "__main__":
    # 示例饰品列表
    test_items = [
        "AK-47 | Hydroponic (Factory New)",
        "M4A1-S | Printstream (Factory New)"
    ]
    
    print("开始批量获取饰品成交量...")
    results = get_multiple_items_sales_volume(test_items)
    
    print(f"\n{'='*50}")
    print("批量获取完成，结果汇总:")
    print('='*50)
    for item, volume in results.items():
        print(f"{item}: {volume}")
# -*- coding: utf-8 -*-
import requests
import json
import os
from datetime import datetime
from config import API_KEY  # 从配置文件导入您的 API Key

# --- 全局设置 ---
BASE_URL = "https://open.steamdt.com"


# ---------------------------------------------
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}
# 所有物品信息的缓存文件名
ALL_ITEMS_CACHE_FILE = "all_items_cache.json"
# 仅包含 marketHashName 的文本文件名
MARKET_HASH_NAME_FILE = "market_hash_names.txt"


def fetch_and_cache_all_items():
    """
    获取所有 CS2 饰品的基础信息。
    如果今天已有缓存，则从本地加载；否则，从 API 获取并创建缓存。
    """
    # 检查今天是否已经有缓存文件
    if os.path.exists(ALL_ITEMS_CACHE_FILE):
        try:
            # 获取文件的最后修改日期
            file_mod_date = datetime.fromtimestamp(os.path.getmtime(ALL_ITEMS_CACHE_FILE)).date()
            if file_mod_date == datetime.today().date():
                print(f"✔️  侦测到今日缓存，程序将不会调用API。所有物品信息已在 '{ALL_ITEMS_CACHE_FILE}' 中。")
                return True
        except Exception as e:
            print(f"⚠️  读取缓存文件时出错: {e}。将尝试重新从 API 获取。")

    print("ℹ️  本地无今日缓存，正在从 API 获取所有物品列表 (每日仅限一次)...")
    # API 端点完全符合文档： GET /open/cs2/v1/base
    endpoint = "/open/cs2/v1/base"
    try:
        # 发送 GET 请求
        response = requests.get(BASE_URL + endpoint, headers=HEADERS, timeout=60)
        response.raise_for_status()  # 如果请求失败 (状态码非 2xx)，则抛出异常
        
        data = response.json()
        if data.get("success"):
            all_items = data.get("data", [])
            print(f"✅ API 调用成功，获取到 {len(all_items)} 条物品信息。")

            # 1. 缓存完整的 JSON 结果到本地文件
            with open(ALL_ITEMS_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_items, f, ensure_ascii=False, indent=4)
            print(f"✔️  完整的物品信息已保存到 '{ALL_ITEMS_CACHE_FILE}'。")

            # 2. 提取所有的 marketHashName 并保存到单独的文本文件
            market_hash_names = [item['marketHashName'] for item in all_items if 'marketHashName' in item]
            with open(MARKET_HASH_NAME_FILE, 'w', encoding='utf-8') as f:
                for name in market_hash_names:
                    f.write(name + '\n')
            print(f"✔️  所有 Market Hash Name 已提取并保存到 '{MARKET_HASH_NAME_FILE}'。")
            
            return True
        else:
            print(f"❌ API 返回错误： {data.get('errorMsg', '未知错误')} (错误码: {data.get('errorCode')})")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ 请求 API 时发生网络错误：{e}")
        return False

# --- 主程序执行区 ---
if __name__ == "__main__":
    print("\n" + "="*25 + " 任务：获取所有物品列表 " + "="*25)
    if not API_KEY:
        print("🛑 错误：请先在 config.py 文件中填写您的 API_KEY。")
    else:
        fetch_and_cache_all_items()
    print("\n🎉  任务执行完毕。")


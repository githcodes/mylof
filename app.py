# -*- coding: utf-8 -*-
"""
LOF基金数据服务 Flask 应用（PostgreSQL版本）
功能：
- 实时/历史 LOF 基金行情抓取（东方财富、AKShare、天天基金、集思录）
- 净值、溢价率、估算净值、估算溢价率计算
- 持仓数据、资产配置、费率抓取与更新
- 指数数据管理（沪深300、恒生、纳斯达克100、恒生科技等）
- 基金分类（被动指数型、主动混合型、QDII-FOF）
- 估值(K)、基估(K)、动估(K)等衍生指标计算
- 多线程更新任务、定时任务调度
- RESTful API 供前端调用
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import sqlite3
import akshare as ak
from datetime import datetime, timedelta
import time
import threading
import concurrent.futures
import random
import requests
import re
import json
import pandas as pd
import os
from bs4 import BeautifulSoup
import html
import logging
from typing import Optional, Dict, Any, List, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
from sklearn.linear_model import LinearRegression
import bisect
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

app = Flask(__name__)
CORS(app)                         # 允许跨域请求
DATABASE = 'lof.db'               # 不再使用，但保留变量以免报错（实际用DATABASE_URL）
HISTORY_DATA_DIR = r"D:\jisilvlof"  # 历史 CSV 文件存放目录（本地路径，部署时需注意）
os.makedirs(HISTORY_DATA_DIR, exist_ok=True)


# ---------- 股票行情缓存 ----------
stock_quote_cache = {}            # 缓存 {股票代码: (时间戳, 行情数据)}
CACHE_TTL = 30                    # 缓存有效期30秒

# 用于回归预测基估(K)的指数列表（普通主动/被动型基金使用）
REGRESSION_INDICES = ['CSI300', 'HSI', 'CSI500', 'CYB']

# 集思录无数据的基金（需要从快照表回填份额）
MISSING_JISILU_FUNDS = ['160513','160617','160621','160622','160641','161010','161019','161133','161119', '161115', '161216', '161505','161626', '161713', '161722', '161820', '162215', '162712', '162715',  '163003','163005','163907','164509', '164606','164509','164703','164814','164902','165311','165509',   '165517','166016','166105','167501','167506','501038','501053','501062','501065', '501088', '501093', '501065', '501210']

# ---------- 基金分类和 FOF 配置 ----------
# 被动指数型 LOF 基金代码集合（人工整理）
PASSIVE_INDEX_FUNDS = {
    '160119', '160135', '160218', '160219', '160221', '160222', '160223', '160225',
    '160615', '160616', '160620', '160625', '160626', '160628', '160629', '160630',
    '160631', '160632', '160633', '160635', '160637', '160638', '160639', '160643',
    '160706', '160716', '160806', '160807', '160925', '161017', '161024', '161025',
    '161026', '161027', '161028', '161029', '161030', '161031', '161032', '161033',
    '161035', '161036', '161037', '161039', '161118', '161121', '161122', '161123',
    '161217', '161226', '161227', '161607', '161631', '161715', '161716', '161720',
    '161724', '161725', '161726', '161811', '161812', '161816', '162216', '162307',
    '162412', '162509', '162711', '163109', '163111', '163113', '163114', '163115',
    '163116', '163118', '163407', '163821', '164206', '164508', '165309', '165511',
    '165515', '165519', '165520', '165521', '165522', '165525', '167301', '167302',
    '168203', '168204', '168701', '501005', '501007', '501008', '501009', '501010',
    '501011', '501012', '501016', '501019', '501029', '501030', '501031', '501036',
    '501037', '501043', '501045', '501047', '501048', '501050', '501057', '501058',
    '501059', '501060', '501061', '501089', '501090', '501227', '502000', '502003',
    '502006', '502010', '502013', '502023', '502048', '502053', '502056', '161831',
    '501306', '161124', '501307', '160924', '501025', '501302', '501301', '501311',
    '160717', '501303', '160322', '501310', '164705', '501305', '501021', '164906',
    '162415', '161127', '160140', '161126', '161128', '161130', '161125', '501300'
}
# QDII-FOF 基金代码集合（需要特殊处理底层ETF组合）
FOF_FUNDS = {
    '501312',  # 华宝海外科技股票
    # 可添加其他类似基金
}

# T+1 公告净值的基金代码列表（可动态增删）
T1_FUNDS = {
    '161133', 
    '501210',  # 优势回报FOF-LOF（示例）
    # 添加更多基金代码
}

# ETF 名称映射，用于前端显示
ETF_NAMES = {
    'ARKK': 'ARK Innovation ETF',
    'ARKG': 'ARK Genomic Revolution ETF',
    'ARKQ': 'ARK Autonomous Technology & Robotics ETF',
    'SOXX': 'iShares Semiconductor ETF',
    'AIQ': 'Global X Artificial Intelligence & Technology ETF',
    'QQQ': 'Invesco QQQ Trust',
    'BOTZ': 'Global X Robotics & Artificial Intelligence ETF',
    'XLK': 'Technology Select Sector SPDR ETF',
    'SMH': 'VanEck Semiconductor ETF',
    'FINX': 'Global X FinTech ETF',
}

# FOF 基金的底层配置：持仓 ETF 及权重、业绩比较基准指数权重
FOF_HOLDINGS = {
    '501312': {
        'underlying': [  # 底层标的代码（需映射到可获取行情的符号）及权重
            ('ARKK', 0.1874),
            ('ARKG', 0.1535),
            ('ARKQ', 0.1159),
            ('SOXX', 0.0951),
            ('AIQ', 0.0785),
            ('QQQ', 0.0745),
            ('BOTZ', 0.0744),
            ('XLK', 0.0644),
            ('SMH', 0.0429),
            ('FINX', 0.0120),
        ],
        'benchmark': {   # 业绩比较基准指数权重（用于基估K）
            'NDX': 0.80,    # 纳斯达克100指数
            'HSTECH': 0.10, # 恒生科技指数
            'BOND': 0.10,   # 中证综合债指数（暂忽略）
        }
    }
}


def init_missing_funds():
    """强制确保初始缺失基金存在（不管表是否为空）"""
    default_codes = MISSING_JISILU_FUNDS
    conn = get_db()
    cursor = conn.cursor()
    for code in default_codes:
        # PostgreSQL: INSERT ... ON CONFLICT DO NOTHING
        cursor.execute("INSERT INTO missing_funds (fund_code) VALUES (%s) ON CONFLICT (fund_code) DO NOTHING", (code,))
    conn.commit()
    conn.close()
    print(f"已确保缺失基金列表包含: {default_codes}")   

# 交易日历缓存
TRADING_DAYS_CACHE_FILE = 'trading_days_cache.json'
TRADING_DAYS_SET = None      # 用于快速判断（集合）
TRADING_DAYS_LIST = []       # 用于顺序查找（排序列表）

def load_trading_days():
    """
    加载交易日历，优先从缓存读取，若无则从 AKShare 获取并缓存。
    返回交易日集合，同时更新 TRADING_DAYS_LIST 和 TRADING_DAYS_SET。
    """
    global TRADING_DAYS_SET, TRADING_DAYS_LIST
    if TRADING_DAYS_SET is not None:
        return TRADING_DAYS_SET

    days = None
    # 1. 尝试从本地缓存加载
    if os.path.exists(TRADING_DAYS_CACHE_FILE):
        try:
            with open(TRADING_DAYS_CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                cache_time = datetime.fromisoformat(data.get('cache_time', '2000-01-01'))
                if (datetime.now() - cache_time).days < 30:
                    days = data.get('days', [])
                    print(f"从缓存加载交易日历，共 {len(days)} 个交易日")
        except Exception as e:
            print(f"读取交易日历缓存失败: {e}")

    # 2. 若无缓存或缓存过期，从 AKShare 获取
    if days is None:
        try:
            df = ak.tool_trade_date_hist_sina()
            if not df.empty:
                days = df['trade_date'].astype(str).tolist()
                # 保存到缓存
                with open(TRADING_DAYS_CACHE_FILE, 'w', encoding='utf-8') as f:
                    json.dump({
                        'cache_time': datetime.now().isoformat(),
                        'days': days
                    }, f, ensure_ascii=False, indent=2)
                print(f"从 AKShare 获取交易日历，共 {len(days)} 个交易日")
        except Exception as e:
            print(f"AKShare 获取交易日历失败: {e}")

    if days:
        # 排序并存储为列表和集合
        days_sorted = sorted(days)
        TRADING_DAYS_LIST = days_sorted
        TRADING_DAYS_SET = set(days_sorted)
    else:
        TRADING_DAYS_SET = None
        TRADING_DAYS_LIST = []

    return TRADING_DAYS_SET

def is_trading_day(date_str: str) -> bool:
    """判断给定日期是否为交易日"""
    global TRADING_DAYS_SET
    if TRADING_DAYS_SET is None:
        load_trading_days()
    if TRADING_DAYS_SET is not None:
        return date_str in TRADING_DAYS_SET
    # 回退：仅判断周末
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return dt.weekday() < 5

def get_previous_trading_day(date_str: str) -> str:
    """
    返回 date_str 之前最近的一个交易日（若 date_str 本身是交易日，则返回前一个交易日）。
    若无更早交易日，返回 None。
    """
    global TRADING_DAYS_LIST
    if not TRADING_DAYS_LIST:
        load_trading_days()
    if TRADING_DAYS_LIST:
        idx = bisect.bisect_left(TRADING_DAYS_LIST, date_str)
        if idx > 0:
            return TRADING_DAYS_LIST[idx - 1]
        return None
    # 回退：逐日递减并跳过周末
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    dt -= timedelta(days=1)
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt.strftime('%Y-%m-%d')


def backfill_nav_from_nav_table():
    """
    将 fund_nav 表中的净值回填到 lof_history 表中 nav 为 NULL 的记录。
    匹配条件：fund_code 和 date 相同。
    """
    print(f"{datetime.now()}: 开始回填 lof_history 缺失净值...")
    conn = get_db()
    cursor = conn.cursor()
    # 查询所有 nav 为 NULL 的历史记录
    cursor.execute("""
        SELECT id, fund_code, date FROM lof_history
        WHERE nav IS NULL
    """)
    rows = cursor.fetchall()
    total = len(rows)
    if total == 0:
        print("没有需要回填的记录")
        conn.close()
        return
    
    updated = 0
    for row in rows:
        fund_code = row['fund_code']
        date_str = row['date']
        # 从 fund_nav 表查询该基金该日期的净值
        cursor.execute(
            "SELECT nav FROM fund_nav WHERE fund_code = %s AND nav_date = %s",
            (fund_code, date_str)
        )
        nav_row = cursor.fetchone()
        if nav_row and nav_row['nav'] is not None:
            cursor.execute(
                "UPDATE lof_history SET nav = %s WHERE id = %s",
                (nav_row['nav'], row['id'])
            )
            updated += 1
        if updated % 1000 == 0:
            print(f"已回填 {updated}/{total} 条")
    conn.commit()
    conn.close()
    print(f"净值回填完成，共更新 {updated} 条记录")

def get_available_nav(fund_code: str, trade_date: str) -> tuple:
    """
    获取基金在交易日 trade_date 可用的净值。
    对于 T+1 基金，取上一个交易日的净值（若存在）；
    对于普通基金，取当日的净值（若已公布）。
    返回 (nav, nav_date)
    """
    conn = get_db()
    cursor = conn.cursor()
    if fund_code in T1_FUNDS:
        # T+1 基金：取上一个交易日的净值
        prev_trading_day = get_previous_trading_day(trade_date)
        if prev_trading_day is None:
            conn.close()
            return None, None
        cursor.execute("""
            SELECT nav, nav_date FROM fund_nav
            WHERE fund_code = %s AND nav_date = %s AND nav IS NOT NULL
        """, (fund_code, prev_trading_day))
    else:
        # 普通基金：取当日的净值（若已公布）
        cursor.execute("""
            SELECT nav, nav_date FROM fund_nav
            WHERE fund_code = %s AND nav_date = %s AND nav IS NOT NULL
        """, (fund_code, trade_date))
    row = cursor.fetchone()
    conn.close()
    if row:
        return row['nav'], row['nav_date']
    return None, None

def process_missing_funds_advanced():
    """
    自动处理 missing_funds 表中的所有基金：
    - 先同步净值数据到 fund_nav 表（可选，注释掉以节省时间）
    - 从 lof_funds 表获取已有信息。
    - 通过 get_cached_batch_stock_quote 获取实时行情（价格、涨跌幅、成交量、成交额、总市值、交易日期）。
    - 从 fund_nav 表获取净值（按交易日匹配），若该日无净值则设为 None。
    - 通过 fetch_merged_shares 获取份额。
    - 更新 lof_funds（含行情和净值），更新 lof_funds_snapshot。
    - 仅在成功获取到行情日期时，写入 lof_history（否则跳过）。
    - 成功处理的基金从 missing_funds 中移除。
    """
    init_missing_funds()
    
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code FROM missing_funds")
    missing_codes = [row[0] for row in cursor.fetchall()]
    if not missing_codes:
        print("missing_funds 表为空，无需处理")
        conn.close()
        return

    print(f"开始处理 {len(missing_codes)} 只缺失基金: {missing_codes}")

    # 1. 批量获取所有缺失基金的实时行情
    print("正在批量获取实时行情...")
    quotes = get_cached_batch_stock_quote(missing_codes)

    # 2. 获取份额数据
    print("正在获取份额数据...")
    shares_df = fetch_merged_shares()

    success_codes = []
    fail_codes = []
    today = datetime.now().strftime('%Y-%m-%d')
    current_year = datetime.now().strftime('%Y')

    for code in missing_codes:
        print(f"\n处理基金 {code} ...")
        hist_date = None  # 初始为 None，仅当行情有效时赋值

        # ----- 先从 lof_funds 读取已有数据 -----
        conn_db = get_db()
        cur_db = conn_db.cursor()
        cur_db.execute("""
            SELECT fund_name, current_price, change_percent, volume_hands, volume_amount, 
                   total_market_value, nav, nav_date, day_low
            FROM lof_funds WHERE fund_code = %s
        """, (code,))
        db_row = cur_db.fetchone()
        conn_db.close()

        # 初始化变量（优先使用数据库中的值）
        fund_name = db_row['fund_name'] if db_row and db_row['fund_name'] else code
        current_price = db_row['current_price'] if db_row else None
        change_percent = db_row['change_percent'] if db_row else None
        volume_hands = db_row['volume_hands'] if db_row else None
        volume_amount = db_row['volume_amount'] if db_row else None
        total_market_value = db_row['total_market_value'] if db_row else None
        day_low = db_row['day_low'] if db_row else None

        # ----- 获取实时行情 -----
        quote = quotes.get(code)
        print(f"DEBUG quote: {quote}")
        if quote:
            if quote.get('name'):
                fund_name = quote.get('name')
            current_price = quote.get('current_price')
            change_percent = quote.get('change_percent')
            volume_hands = quote.get('volume')              # 手
            volume_amount = quote.get('turnover')           # 万元
            total_market_value = quote.get('total_market_value')  # 元
            hist_date = quote.get('trade_date')  # 可能为 None
            day_low = quote.get('low')
            if hist_date:
                print(f"  ✅ 行情获取成功: 名称={fund_name}, 价格={current_price}, 最低价={day_low},涨跌幅={change_percent}%, "
                      f"成交量={volume_hands}手, 成交额={volume_amount}万元, 总市值={total_market_value}元, 日期={hist_date}")
            else:
                print(f"  ⚠️ 行情数据无交易日期，跳过历史记录写入")
        else:
            print(f"  ⚠️ 未获取到实时行情，跳过历史记录写入")

        # ---------- 净值获取：从 fund_nav 表查询，按交易日匹配 ----------
        nav = None
        nav_date = None
        if hist_date:
            nav, nav_date = get_available_nav(code, hist_date)
            if nav is not None:
                print(f"  ✅ 获取净值: {nav} (日期 {nav_date})")
            else:
                print(f"  ⚠️ 未找到可用净值")

        # ----- 份额数据 -----
        shares_row = shares_df.loc[code] if code in shares_df.index else None
        if shares_row is not None:
            fund_shares = shares_row.get('fund_shares')
            shares_add = shares_row.get('shares_add')
            shares_change = shares_row.get('shares_change')
            snapshot_date = shares_row.get('snapshot_date', today)
            print(f"  ✅ 份额获取成功: {fund_shares} 万份 (日期 {snapshot_date})")
        else:
            print(f"  ⚠️ 未获取到份额数据")
            fund_shares = None
            shares_add = None
            shares_change = None
            snapshot_date = None

        # ----- 更新 lof_funds（无论是否有行情都更新基础信息） -----
        conn2 = get_db()
        cur2 = conn2.cursor()
        cur2.execute("SELECT fund_code FROM lof_funds WHERE fund_code = %s", (code,))
        exists = cur2.fetchone()
        if exists:
            update_fields = []
            params = []
            if fund_name:
                update_fields.append("fund_name = %s")
                params.append(fund_name)
            if current_price is not None:
                update_fields.append("current_price = %s")
                params.append(current_price)
            if change_percent is not None:
                update_fields.append("change_percent = %s")
                params.append(change_percent)
            if day_low is not None:
                update_fields.append("day_low = %s")
                params.append(day_low)
            if volume_hands is not None:
                update_fields.append("volume_hands = %s")
                params.append(volume_hands)
            if volume_amount is not None:
                update_fields.append("volume_amount = %s")
                params.append(volume_amount)
            if total_market_value is not None:
                update_fields.append("total_market_value = %s")
                params.append(total_market_value)
            if nav is not None:
                update_fields.append("nav = %s")
                params.append(nav)
            if nav_date:
                update_fields.append("nav_date = %s")
                params.append(nav_date)
            if update_fields:
                sql = f"UPDATE lof_funds SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP WHERE fund_code = %s"
                params.append(code)
                cur2.execute(sql, params)
        else:
            cur2.execute("""
                INSERT INTO lof_funds 
                (fund_code, fund_name, current_price, change_percent, volume_hands, volume_amount, 
                 total_market_value, nav, nav_date, day_low, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (code, fund_name, current_price, change_percent, volume_hands, volume_amount, 
                  total_market_value, nav, nav_date, day_low))
        conn2.commit()
        conn2.close()
        print(f"  ✅ lof_funds 更新完成 (成交量={volume_hands}手, 成交额={volume_amount}万元, 总市值={total_market_value}亿元)")

        # ----- 写入 lof_funds_snapshot -----
        if fund_shares is not None:
            conn2 = get_db()
            cur2 = conn2.cursor()
            cur2.execute("""
                INSERT INTO lof_funds_snapshot 
                (fund_code, fund_shares, shares_add, shares_change, snapshot_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fund_code, snapshot_date) DO UPDATE SET
                    fund_shares = EXCLUDED.fund_shares,
                    shares_add = EXCLUDED.shares_add,
                    shares_change = EXCLUDED.shares_change
            """, (code, fund_shares, shares_add, shares_change, snapshot_date))
            conn2.commit()
            conn2.close()
            print(f"  ✅ 份额快照写入完成")

        # ========== 计算溢价率 ==========
        premium_rate = None
        if current_price is not None and nav is not None and nav != 0:
            premium_rate = (current_price / nav - 1) * 100
            print(f"  ✅ 计算溢价率: {premium_rate:.2f}%")
        else:
            print(f"  ⚠️ 无法计算溢价率（价格或净值缺失）")

        # ----- 写入 lof_history 仅在 hist_date 非空时执行 -----
        if hist_date and (current_price is not None or nav is not None):
            success = insert_or_replace_lof_history(
                fund_code=code,
                date=hist_date,
                close=current_price if current_price is not None else 0.0,
                nav=nav,
                nav_date=nav_date,
                index_change=None,
                heavy_change=None,
                premium_rate=premium_rate,
                volume_amount=volume_amount,   # 已经是万元
                fund_shares=fund_shares,
                shares_add=shares_add,
                shares_change=shares_change,
                low=day_low   # 传入当日最低价
            )
            if success:
                print(f"  ✅ 历史记录写入成功 (交易日 {hist_date})")
            else:
                print(f"  ❌ 历史记录写入失败")
        else:
            print(f"  ℹ️ 跳过历史记录写入（无有效行情日期或价格/净值）")

        # 判断是否处理成功（至少更新了 lof_funds 就算部分成功？这里沿用原逻辑：有价格或净值）
        if current_price is not None or nav is not None:
            success_codes.append(code)
            # 如果有历史记录且成功，可重算动态字段
            if hist_date:
                calculate_dynamic_fields_for_fund(code)
        else:
            fail_codes.append(code)

    # 从 missing_funds 删除成功处理的
    if success_codes:
        conn = get_db()
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(success_codes))
        cursor.execute(f"DELETE FROM missing_funds WHERE fund_code IN ({placeholders})", success_codes)
        conn.commit()
        conn.close()
        print(f"\n✅ 成功处理 {len(success_codes)} 只基金，已从 missing_funds 移除: {success_codes}")
    if fail_codes:
        print(f"\n⚠️ {len(fail_codes)} 只基金处理失败，仍保留在 missing_funds 表中: {fail_codes}")

    print("missing_funds 高级自动处理任务完成")



def update_jicha_all():
    """
    遍历 lof_history 表中所有记录，重新计算并更新基差(K) (jicha)。
    基差(K) = ((jigu - nav) / nav) * 100
    """
    print(f"{datetime.now()}: 开始全量更新基差(K)...")
    conn = get_db()
    cursor = conn.cursor()
    
    # 获取所有有 jigu 和 nav 的记录
    cursor.execute("""
        SELECT id, fund_code, date, jigu, nav
        FROM lof_history
        WHERE jigu IS NOT NULL AND nav IS NOT NULL AND nav != 0
    """)
    rows = cursor.fetchall()
    total = len(rows)
    if total == 0:
        print("没有可更新的记录（缺少 jigu 或 nav）")
        conn.close()
        return
    
    updated = 0
    for row in rows:
        jigu = row['jigu']
        nav = row['nav']
        if nav == 0:
            continue
        jicha = ((jigu - nav) / nav) * 100
        # 更新该条记录
        cursor.execute(
            "UPDATE lof_history SET jicha = %s WHERE id = %s",
            (jicha, row['id'])
        )
        updated += 1
        if updated % 1000 == 0:
            print(f"已更新 {updated}/{total} 条记录")
    
    conn.commit()
    conn.close()
    print(f"基差(K)更新完成，共更新 {updated} 条记录")


def classify_fund(fund_code, fund_name):
    """根据基金代码和名称判断基金类型：被动指数型 / QDII-FOF / 主动混合型"""
    if fund_code in PASSIVE_INDEX_FUNDS:
        return '被动指数型'
    elif fund_code in FOF_FUNDS:
        return 'QDII-FOF'
    else:
        return '主动混合型'

# ---------- 股票行情缓存（保留原有函数） ----------
def get_market_prefix(stock_code):
    """根据股票代码判断所属市场前缀（新浪/腾讯格式）"""
    # 先处理常见的指数代码
    if stock_code in ('HSI', 'HSTECH'):
        return ('hk', 'hk')   # 恒生指数系列
    if stock_code == 'NDX':
        return ('gb', 'us')   # 纳斯达克100
    if len(stock_code) == 6 and stock_code.isdigit():
        if stock_code.startswith(('6', '5', '9', '688')):
            return ('sh', 'sh')   # 上海 A 股
        else:
            return ('sz', 'sz')   # 深圳 A 股
    elif len(stock_code) == 5 and stock_code.isdigit() and stock_code.startswith('0'):
        return ('hk', 'hk')       # 港股
    elif stock_code.isalpha():
        return ('gb', 'us')       # 美股（字母代码）
    else:
        return ('sz', 'sz')


def get_stock_realtime(stock_code):
    """单只股票实时行情（新浪接口，备用）"""
    sina_prefix, _ = get_market_prefix(stock_code)
    if sina_prefix == 'gb':
        prefix = 'gb_'
    elif sina_prefix == 'hk':
        prefix = 'hk'
    else:
        prefix = sina_prefix
    url = f"https://hq.sinajs.cn/list={prefix}{stock_code}"
    headers = {"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=3)
        resp.encoding = 'gbk'
        text = resp.text
        if '="' not in text:
            return None
        data_str = text.split('="')[1].split('"')[0]
        parts = data_str.split(',')
        if len(parts) < 10:
            return None
        if prefix == 'gb_':
            current_price = float(parts[1]) if parts[1] and parts[1] != '-' else None
            change_amount = float(parts[2]) if parts[2] and parts[2] != '-' else None
            yesterday_close = current_price - change_amount if current_price is not None and change_amount is not None else None
            change_percent = float(parts[3]) if parts[3] and parts[3] != '-' else None
        else:
            current_price = float(parts[3]) if parts[3] and parts[3] != '-' else None
            yesterday_close = float(parts[2]) if parts[2] and parts[2] != '-' else None
            change_percent = None
            if current_price is not None and yesterday_close is not None and yesterday_close != 0:
                change_percent = (current_price - yesterday_close) / yesterday_close * 100
                if abs(change_percent) > 30:
                    change_percent = None
        return {"current_price": current_price, "change_percent": change_percent}
    except Exception as e:
        logging.warning(f"获取股票 {stock_code} 行情失败: {e}")
        return None

def get_tencent_batch_realtime(stock_codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """腾讯接口批量获取股票实时行情，返回单位：成交量（手）、成交额（万元），并提取总市值（亿元）"""
    if not stock_codes:
        return {}
    unique_codes = list(set(stock_codes))
    tencent_params = []
    for code in unique_codes:
        _, tencent_prefix = get_market_prefix(code)
        if tencent_prefix == 'sh':
            tencent_params.append(f'sh{code}')
        elif tencent_prefix == 'sz':
            tencent_params.append(f'sz{code}')
        elif tencent_prefix == 'hk':
            tencent_params.append(f'hk{code}')
        else:
            tencent_params.append(f'us{code}')
    query_str = ','.join(tencent_params)
    url = f"http://qt.gtimg.cn/q={query_str}"
    headers = {"User-Agent": "Mozilla/5.0"}
    result = {}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        resp.encoding = 'gbk'
        text = resp.text
        lines = text.strip().split('\n')
        for line in lines:
            if '="' not in line:
                continue
            code_part = line.split('=')[0].replace('v_', '')
            if code_part.startswith(('sh', 'sz', 'hk', 'us')):
                code = code_part[2:]
            else:
                continue
            data_str = line.split('"')[1]
            parts = data_str.split('~')
            if len(parts) < 58:   # 确保有足够的字段
                continue
            try:
                name = parts[1] if len(parts) > 1 else None
                current_price = float(parts[3]) if parts[3] and parts[3] != '-' else None
                # 最低价（索引34）
                low = float(parts[34]) if len(parts) > 34 and parts[34] and parts[34] != '-' else None
                yesterday_close = float(parts[4]) if parts[4] and parts[4] != '-' else None
                # 成交量（手）索引6
                volume = float(parts[6]) if len(parts) > 6 and parts[6] != '-' else None
                # 成交额（万元）索引57
                turnover = float(parts[37]) if len(parts) > 37 and parts[37] != '-' and parts[37] else None
                # 总市值
                total_market_value = float(parts[44]) if len(parts) > 44 and parts[44] != '-' and parts[44] else None
                # 修改为（转换为元）
                total_market_value_yuan = None
                if len(parts) > 44 and parts[44] and parts[44] != '-':
                    try:
                        total_market_value_yuan = float(parts[44]) * 100000000  # 亿元 → 元
                    except:
                        pass
                # 涨跌幅
                change_percent = None
                if current_price is not None and yesterday_close is not None and yesterday_close != 0:
                    change_percent = (current_price - yesterday_close) / yesterday_close * 100
                    if abs(change_percent) > 30:
                        change_percent = None
                # 交易日期（索引31，格式 YYYYMMDDHHMMSS）
                trade_date = None
                if len(parts) > 30 and parts[30]:
                    time_str = parts[30]
                    if len(time_str) >= 8:
                        trade_date = f"{time_str[:4]}-{time_str[4:6]}-{time_str[6:8]}"
               
                result[code] = {
                    "name": name,
                    "current_price": current_price,
                    "change_percent": change_percent,
                    "volume": volume,                # 手
                    "turnover": turnover,            # 万元
                    "trade_date": trade_date,
                    "total_market_value": total_market_value_yuan,
                    "low": low,   # 新增  
                }
            except Exception as e:
                logging.warning(f"腾讯解析股票 {code} 数据出错: {e}")
    except Exception as e:
        logging.error(f"腾讯批量获取股票行情失败: {e}")
    return result



def _fetch_from_sina(stock_codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """新浪接口批量获取股票实时行情，返回单位：成交量（手）、成交额（万元）"""
    if not stock_codes:
        return {}
    sina_params = []
    for code in stock_codes:
        sina_prefix, _ = get_market_prefix(code)
        if sina_prefix == 'sh':
            sina_params.append(f'sh{code}')
        elif sina_prefix == 'sz':
            sina_params.append(f'sz{code}')
        elif sina_prefix == 'hk':
            sina_params.append(f'hk{code}')
        else:
            sina_params.append(f'gb_{code}')
    query_str = ','.join(sina_params)
    url = f"https://hq.sinajs.cn/list={query_str}"
    headers = {"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"}
    result = {}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        resp.encoding = 'gbk'
        text = resp.text
        lines = text.strip().split('\n')
        for line in lines:
            if '="' not in line:
                continue
            code_part = line.split('=')[0].replace('var ', '')
            if code_part.startswith(('sh', 'sz', 'hk', 'gb_')):
                if code_part.startswith('gb_'):
                    code = code_part[3:]
                else:
                    code = code_part[2:]
            else:
                continue
            data_str = line.split('="')[1].split('"')[0]
            parts = data_str.split(',')
            if len(parts) < 10:
                continue
            try:
                name = parts[0] if parts[0] else None
                # 交易日期（索引30）
                trade_date = None
                if len(parts) > 30 and parts[30]:
                    trade_date = parts[30]  # 格式 YYYY-MM-DD
               
                
                if code_part.startswith('gb_'):
                    # 美股
                    current_price = float(parts[1]) if parts[1] and parts[1] != '-' else None
                    change_amount = float(parts[2]) if parts[2] and parts[2] != '-' else None
                    yesterday_close = current_price - change_amount if current_price is not None and change_amount is not None else None
                    change_percent = float(parts[3]) if parts[3] and parts[3] != '-' else None
                    # 最低价索引5
                    low = float(parts[5]) if len(parts) > 5 and parts[5] and parts[5] != '-' else None
                    volume = None
                    turnover = None
                    total_market_value = None
                else:
                    # A股、基金
                    current_price = float(parts[3]) if parts[3] and parts[3] != '-' else None
                    yesterday_close = float(parts[2]) if parts[2] and parts[2] != '-' else None
                    # 最低价索引5
                    low = float(parts[5]) if len(parts) > 5 and parts[5] and parts[5] != '-' else None
                    # 成交量（股）索引8 -> 转为手
                    volume_raw = float(parts[8]) if len(parts) > 8 and parts[8] != '-' and parts[8] else None
                    volume = volume_raw / 100 if volume_raw is not None else None
                    # 成交额（元）索引9 -> 转为万元
                    turnover_raw = float(parts[9]) if len(parts) > 9 and parts[9] != '-' and parts[9] else None
                    turnover = turnover_raw / 10000 if turnover_raw is not None else None
                    # 涨跌幅
                    change_percent = None
                    if current_price is not None and yesterday_close is not None and yesterday_close != 0:
                        change_percent = (current_price - yesterday_close) / yesterday_close * 100
                        if abs(change_percent) > 30:
                            change_percent = None
                    total_market_value = None   # 新浪接口无总市值
                result[code] = {
                    "name": name,
                    "current_price": current_price,
                    "change_percent": change_percent,
                    "volume": volume,           # 手
                    "turnover": turnover,       # 万元
                    "trade_date": trade_date,
                    "total_market_value": total_market_value,
                    "low": low   # 新增
                }
            except Exception as e:
                logging.warning(f"解析股票 {code} 数据出错: {e}")
    except Exception as e:
        logging.error(f"新浪批量获取股票行情失败: {e}")
    return result


def get_batch_stock_realtime(stock_codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """综合获取批量股票实时行情（优先新浪，失败降级腾讯，最后单只重试）"""
    if not stock_codes:
        return {}
    unique_codes = list(set(stock_codes))
    result = _fetch_from_sina(unique_codes)
    failed_codes = [code for code in unique_codes if code not in result]
    if failed_codes:
        logging.info(f"新浪接口未能获取 {len(failed_codes)} 只股票，尝试腾讯接口...")
        tencent_result = get_tencent_batch_realtime(failed_codes)
        result.update(tencent_result)
        still_failed = [code for code in failed_codes if code not in tencent_result]
        for code in still_failed:
            single = get_stock_realtime(code)
            if single:
                result[code] = single
                logging.info(f"单只重试成功: {code}")
            else:
                logging.warning(f"所有接口均未能获取股票 {code} 的数据")
    return result

def get_cached_batch_stock_quote(stock_codes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """带缓存的批量股票行情获取（减少请求频率）"""
    now = time.time()
    result = {}
    need_fetch = []
    for code in stock_codes:
        if code in stock_quote_cache:
            cached_time, data = stock_quote_cache[code]
            if now - cached_time < CACHE_TTL:
                result[code] = data
            else:
                need_fetch.append(code)
        else:
            need_fetch.append(code)
    if need_fetch:
        fresh = get_batch_stock_realtime(need_fetch)
        for code, data in fresh.items():
            stock_quote_cache[code] = (now, data)
            result[code] = data
    return result

# ---------- 数据库操作 ----------
def get_db(max_retries=3):
    """获取 PostgreSQL 数据库连接，支持重试"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        # 本地开发可设置 DATABASE_URL 环境变量，或 fallback 到 SQLite（但这里我们强制要求）
        raise Exception("DATABASE_URL environment variable not set")

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(database_url, sslmode='require')
            conn.cursor_factory = psycopg2.extras.RealDictCursor
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                raise

def init_db():
    """初始化 PostgreSQL 数据库所有表结构（如果不存在则创建）"""
    conn = get_db()
    cursor = conn.cursor()
    # lof_funds 表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lof_funds (
            id SERIAL PRIMARY KEY,
            fund_code TEXT UNIQUE NOT NULL,
            fund_name TEXT NOT NULL,
            current_price DOUBLE PRECISION,
            change_percent DOUBLE PRECISION,
            volume_amount DOUBLE PRECISION,
            volume_hands DOUBLE PRECISION,
            total_market_value DOUBLE PRECISION,
            nav DOUBLE PRECISION,
            nav_date TEXT,
            premium_rate DOUBLE PRECISION,
            purchase_status TEXT,
            redemption_status TEXT,
            daily_purchase_limit DOUBLE PRECISION,
            estimated_nav DOUBLE PRECISION,
            estimated_premium_rate DOUBLE PRECISION,
            purchase_fee_rate TEXT,
            redeem_fee_rate TEXT,
            day_low DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # lof_holdings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lof_holdings (
            id SERIAL PRIMARY KEY,
            fund_code TEXT NOT NULL,
            stock_code TEXT,
            stock_name TEXT,
            nav_ratio DOUBLE PRECISION,
            shares DOUBLE PRECISION,
            holding_rank INTEGER
        )
    ''')
    # lof_history
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lof_history (
            id SERIAL PRIMARY KEY,
            fund_code TEXT NOT NULL,
            date TEXT NOT NULL,
            close_price DOUBLE PRECISION,
            nav_date TEXT,
            nav DOUBLE PRECISION,
            jigu DOUBLE PRECISION,
            jiyi DOUBLE PRECISION,
            jicha DOUBLE PRECISION,
            donggu DOUBLE PRECISION,
            dongcha DOUBLE PRECISION,
            dongyi DOUBLE PRECISION,
            guzhi DOUBLE PRECISION,
            wucha DOUBLE PRECISION,
            premium_rate_k DOUBLE PRECISION,
            premium_rate DOUBLE PRECISION,
            volume_amount DOUBLE PRECISION,
            fund_shares DOUBLE PRECISION,
            shares_add DOUBLE PRECISION,
            shares_change DOUBLE PRECISION,
            index_change DOUBLE PRECISION,
            heavy_change DOUBLE PRECISION,
            nav_change_pct DOUBLE PRECISION,
            low DOUBLE PRECISION,
            UNIQUE(fund_code, date)
        )
    ''')
    # fund_pred_returns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_pred_returns (
            id SERIAL PRIMARY KEY,
            fund_code TEXT NOT NULL,
            date TEXT NOT NULL,
            pred_return DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code, date)
        )
    ''')
    # lof_asset_allocation
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lof_asset_allocation (
            id SERIAL PRIMARY KEY,
            fund_code TEXT NOT NULL,
            report_date TEXT NOT NULL,
            stock_ratio DOUBLE PRECISION,
            bond_ratio DOUBLE PRECISION,
            cash_ratio DOUBLE PRECISION,
            net_assets DOUBLE PRECISION,
            UNIQUE(fund_code, report_date)
        )
    ''')
    # index_daily
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_daily (
            id SERIAL PRIMARY KEY,
            index_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            change_pct DOUBLE PRECISION,
            UNIQUE(index_code, trade_date)
        )
    ''')
    # fund_classification
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_classification (
            id SERIAL PRIMARY KEY,
            fund_code TEXT UNIQUE NOT NULL,
            fund_type TEXT NOT NULL
        )
    ''')
    # fund_rates
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_rates (
            id SERIAL PRIMARY KEY,
            fund_code TEXT UNIQUE NOT NULL,
            purchase_fee TEXT,
            redeem_fee TEXT,
            management_fee DOUBLE PRECISION,
            custody_fee DOUBLE PRECISION,
            service_fee DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # lof_funds_snapshot
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lof_funds_snapshot (
            fund_code TEXT NOT NULL,
            fund_shares DOUBLE PRECISION,
            shares_add DOUBLE PRECISION,
            shares_change DOUBLE PRECISION,
            snapshot_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code, snapshot_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshot_code_date ON lof_funds_snapshot(fund_code, snapshot_date)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS missing_funds (
            fund_code TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # fund_nav
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_nav (
            id SERIAL PRIMARY KEY,
            fund_code TEXT NOT NULL,
            nav_date TEXT NOT NULL,
            nav DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code, nav_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_nav_code_date ON fund_nav(fund_code, nav_date)')

    conn.commit()
    conn.close()
    print("PostgreSQL 数据库表初始化/升级完成")



def get_missing_funds_list():
    """从 missing_funds 表获取所有待补全的基金代码"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code FROM missing_funds")
    funds = [row[0] for row in cursor.fetchall()]
    conn.close()
    return funds


# ---------- 东方财富实时行情 ----------

def fetch_realtime_data():
    """原有功能 + 返回 DataFrame"""
    print(f"{datetime.now()}: [东方财富] 开始获取LOF实时数据...")
    url = "https://push2delay.eastmoney.com/api/qt/clist/get"
    all_data = []
    page = 1
    page_size = 500
    while True:
        params = {
            "pn": str(page), "pz": str(page_size), "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
            "wbp2u": "|0|0|0|web", "fid": "f3",
            "fs": "b:MK0404,b:MK0405,b:MK0406,b:MK0407",
            "fields": "f1,f2,f3,f4,f5,f6,f12,f13,f14,f15,f16,f20",
        }
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            data = resp.json()
            if not data.get('data') or not data['data'].get('diff'):
                break
            stocks = data['data']['diff']
            all_data.extend(stocks)
            page += 1
            time.sleep(0.2)
        except Exception as e:
            print(f"获取第 {page} 页数据时出错: {e}")
            break

    records = []
    for item in all_data:
        code = item.get('f12', '')
        name = item.get('f14', '')
        price = item.get('f2')
        price = None if price == '-' else price
        change = item.get('f3')
        change = None if change == '-' else change
        volume_hands = item.get('f5')
        volume_hands = None if volume_hands == '-' else volume_hands
        volume_amount = item.get('f6')
        volume_amount = None if volume_amount == '-' else volume_amount
        total_mv = item.get('f20')
        total_mv = None if total_mv == '-' else total_mv
        low = item.get('f16')
        low = None if low == '-' else float(low) if low else None

        # 更新 lof_funds（使用 PostgreSQL ON CONFLICT）
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO lof_funds (fund_code, fund_name, current_price, change_percent, volume_hands, volume_amount, total_market_value, day_low, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (fund_code) DO UPDATE SET
                fund_name = EXCLUDED.fund_name,
                current_price = EXCLUDED.current_price,
                change_percent = EXCLUDED.change_percent,
                volume_hands = EXCLUDED.volume_hands,
                volume_amount = EXCLUDED.volume_amount,
                total_market_value = EXCLUDED.total_market_value,
                day_low = EXCLUDED.day_low,
                updated_at = CURRENT_TIMESTAMP
        """, (code, name, price, change, volume_hands, volume_amount, total_mv, low))
        conn.commit()
        conn.close()

        records.append({
            'fund_code': code,
            'close': float(price) if price is not None else None,
            'change_percent': float(change) if change is not None else None,
            'volume_amount': float(volume_amount) if volume_amount is not None else None,
        })
    df = pd.DataFrame(records)
    if not df.empty:
        df.set_index('fund_code', inplace=True)
    print(f"东方财富数据更新完成，共 {len(all_data)} 条")
    return df


def fetch_nav_data_raw():
    """从 AKShare 获取净值，返回 DataFrame，索引 fund_code，列 nav, nav_date"""
    try:
        df = ak.fund_purchase_em()
        if df.empty:
            return pd.DataFrame()
        df['fund_code'] = df['基金代码'].str.extract(r'(\d{6})')
        df.set_index('fund_code', inplace=True)
        df.rename(columns={
            '最新净值/万份收益': 'nav',
            '最新净值/万份收益-报告时间': 'nav_date'
        }, inplace=True)
        df = df[df['nav'].notna() & (df['nav'] != '-')]
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        return df[['nav', 'nav_date']]
    except Exception as e:
        print(f"AKShare 净值获取失败: {e}")
        return pd.DataFrame()
    




def fetch_sh_shares():
    """
    从上交所官方API获取LOF基金场内份额数据
    返回 DataFrame，包含列: fund_shares, date（索引为 fund_code）
    """
    url = 'https://query.sse.com.cn/commonQuery.do'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.sse.com.cn/',
        'Accept': 'application/json'
    }
    base_params = {
        'jsonCallBack': 'jsonpCallback',
        'isPagination': 'true',
        'sqlId': 'COMMON_SSE_SJ_JJSJ_JJGM_LOFGMTJ_L',
        'PRODUCT_TYPE': '11,14,15',
        'type': 'inParams',
        'pageHelp.pageSize': '100',
        'pageHelp.cacheSize': '1',
        '_': int(time.time() * 1000)
    }

    all_data = []
    snapshot_date = None
    page_no = 1
    while True:
        params = dict(base_params)
        params['pageHelp.pageNo'] = page_no
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            text = resp.text
            if text.startswith('jsonpCallback'):
                match = re.search(r'jsonpCallback\((.*)\)', text, re.DOTALL)
                if not match:
                    break
                data = json.loads(match.group(1))
            else:
                data = resp.json()

            rows = data.get('result', [])
            if not rows:
                break
            all_data.extend(rows)

            if snapshot_date is None and rows:
                trade_date_str = rows[0].get('TRADE_DATE', '')
                if trade_date_str:
                    snapshot_date = f"{trade_date_str[:4]}-{trade_date_str[4:6]}-{trade_date_str[6:8]}"

            page_help = data.get('pageHelp', {})
            total = page_help.get('total', 0)
            page_size = page_help.get('pageSize', 100)
            if page_no * page_size >= total:
                break
            page_no += 1
            time.sleep(0.2)
        except Exception as e:
            logging.warning(f"上交所API请求失败: {e}")
            break

    if not all_data:
        return pd.DataFrame()

    records = []
    for item in all_data:
        fund_code = item.get('FUND_CODE', '').strip()
        if not fund_code:
            continue
        share_str = item.get('INTERNAL_VOL', '0').replace(',', '')
        try:
            fund_shares = float(share_str)
        except:
            fund_shares = None
        if fund_shares is not None:
            records.append({
                'fund_code': fund_code,
                'fund_shares': fund_shares,
                'date': snapshot_date
            })
    df = pd.DataFrame(records)
    if not df.empty:
        df.set_index('fund_code', inplace=True)
        df = df[df['fund_shares'] > 0]
    logging.info(f"上交所份额抓取完成，共 {len(df)} 只基金，日期 {snapshot_date}")
    return df


def fetch_sz_shares(retries=3, timeout=15):
    """
    从深交所官方API获取LOF基金场内份额数据，返回最新交易日的份额。
    支持重试和超时设置。
    """
    base_url = 'https://www.szse.cn/api/report/ShowReport/data'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.szse.cn/',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    for attempt in range(retries):
        try:
            page_no = 1
            all_data = []
            latest_date = None
            while True:
                params = {
                    'SHOWTYPE': 'JSON',
                    'CATALOGID': 'scsj_fund_jjgm',
                    'jjlb': 'LOF',
                    'loading': 'first',
                    'PAGENO': page_no
                }
                resp = requests.get(base_url, headers=headers, params=params, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    rows = data[0].get('data', [])
                    if not rows:
                        break
                    for row in rows:
                        size_date = row.get('size_date')
                        if size_date:
                            if latest_date is None or size_date > latest_date:
                                latest_date = size_date
                    all_data.extend(rows)
                    if len(rows) < 20:
                        break
                    page_no += 1
                    time.sleep(0.2)
                else:
                    break
            
            if not all_data or not latest_date:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return pd.DataFrame()
            
            # 过滤出最新日期的记录
            filtered = [item for item in all_data if item.get('size_date') == latest_date]
            records = []
            for item in filtered:
                fund_code = item.get('fund_code', '').strip()
                if not fund_code:
                    continue
                share_str = item.get('current_size', '0').replace(',', '')
                try:
                    fund_shares = float(share_str)
                except:
                    fund_shares = None
                if fund_shares is not None:
                    records.append({
                        'fund_code': fund_code,
                        'fund_shares': fund_shares,
                        'date': latest_date
                    })
            df = pd.DataFrame(records)
            if not df.empty:
                df.set_index('fund_code', inplace=True)
                df = df[df['fund_shares'] > 0]
            logging.info(f"深交所份额抓取完成，共 {len(df)} 只基金，日期 {latest_date}")
            return df
        except Exception as e:
            logging.warning(f"深交所API请求失败 (尝试 {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(3)
            else:
                return pd.DataFrame()
    return pd.DataFrame()

def fetch_merged_shares():
    """
    合并上交所和深交所份额数据，存入快照表，并计算当日新增份额和份额涨幅。
    计算逻辑：对每只基金，查询其快照表中早于当日 snapshot_date 的最新一条记录，
    作为基准计算新增和涨幅（即使间隔多个交易日也能正确计算）。
    返回 DataFrame，索引 fund_code，包含 fund_shares, shares_add, shares_change, snapshot_date
    """
    sh_df = fetch_sh_shares()
    sz_df = fetch_sz_shares()

    # 取深交所日期（优先），若无则取上交所日期
    if not sz_df.empty:
        snapshot_date = sz_df['date'].iloc[0]
    elif not sh_df.empty:
        snapshot_date = sh_df['date'].iloc[0]
    else:
        print("无法获取份额数据日期，跳过快照")
        return pd.DataFrame()

    # 合并份额数据（去掉原 date 列，后面统一添加 snapshot_date）
    sh_data = sh_df.drop(columns=['date']) if 'date' in sh_df.columns else sh_df
    sz_data = sz_df.drop(columns=['date']) if 'date' in sz_df.columns else sz_df
    combined = pd.concat([sh_data, sz_data]).reset_index().drop_duplicates(subset='fund_code', keep='first')
    if combined.empty:
        return pd.DataFrame()
    combined.set_index('fund_code', inplace=True)

    # 添加快照日期列
    combined['snapshot_date'] = snapshot_date

    conn = get_db()
    cursor = conn.cursor()

    # 插入或替换当日份额（使用 ON CONFLICT）
    for fund_code, row in combined.iterrows():
        shares = row['fund_shares']
        if shares is None:
            continue
        cursor.execute('''
            INSERT INTO lof_funds_snapshot (fund_code, fund_shares, snapshot_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (fund_code, snapshot_date) DO UPDATE SET
                fund_shares = EXCLUDED.fund_shares
        ''', (fund_code, shares, snapshot_date))
    conn.commit()

    # ---------- 修改点：按基金单独查询最近历史记录，而非固定前一日 ----------
    # 初始化新增和涨幅列
    combined['shares_add'] = None
    combined['shares_change'] = None

    for fund_code in combined.index:
        cur = combined.loc[fund_code, 'fund_shares']
        if cur is None:
            continue

        # 查询该基金在 snapshot_date 之前的最新一条快照记录
        cursor.execute('''
            SELECT fund_shares FROM lof_funds_snapshot
            WHERE fund_code = %s AND snapshot_date < %s
            ORDER BY snapshot_date DESC LIMIT 1
        ''', (fund_code, snapshot_date))
        row = cursor.fetchone()
        if row:
            prev = row['fund_shares']
            if prev is not None and prev != 0:
                shares_add = round(cur - prev, 2)
                shares_change = round((shares_add / prev) * 100, 2)
            else:
                shares_add = None
                shares_change = None
        else:
            shares_add = None
            shares_change = None

        combined.loc[fund_code, 'shares_add'] = shares_add
        combined.loc[fund_code, 'shares_change'] = shares_change

    # 回写计算结果到快照表
    for fund_code in combined.index:
        shares_add = combined.loc[fund_code, 'shares_add']
        shares_change = combined.loc[fund_code, 'shares_change']
        cursor.execute('''
            UPDATE lof_funds_snapshot 
            SET shares_add = %s, shares_change = %s
            WHERE fund_code = %s AND snapshot_date = %s
        ''', (shares_add, shares_change, fund_code, snapshot_date))
    conn.commit()
    conn.close()

    print(f"份额快照完成，日期 {snapshot_date}，共 {len(combined)} 只基金")
    return combined

def patch_lof_history_from_sources(target_date=None):
    """
    仅针对集思录无数据的基金（MISSING_JISILU_FUNDS 列表），补全当日行情和净值数据。

    """
    if target_date is None:
        target_date = datetime.now().strftime('%Y-%m-%d')
    print(f"开始补全 {target_date} 的历史数据（仅针对集思录无数据基金）...")

    if not MISSING_JISILU_FUNDS:
        print("没有配置需要补全的基金列表，请设置 MISSING_JISILU_FUNDS")
        return

    # ---------- 新增：日期规范化辅助函数 ----------
    def normalize_date(date_str):
        """将 'MM-DD' 补全为 'YYYY-MM-DD'，若已是完整格式则原样返回，否则返回 None"""
        if not date_str:
            return None
        date_str = date_str.strip()
        # 若已是 YYYY-MM-DD，直接返回
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str
        # 若为 MM-DD，补当前年份
        if re.match(r'\d{2}-\d{2}', date_str):
            return datetime.now().strftime('%Y') + '-' + date_str
        return None

    # 规范化目标日期
    norm_target = normalize_date(target_date)

    realtime_df = fetch_realtime_data()
    nav_df = fetch_nav_data_raw()

    conn = get_db()
    cursor = conn.cursor()
    updated = 0

    for fund_code in MISSING_JISILU_FUNDS:
        close = realtime_df.loc[fund_code, 'close'] if fund_code in realtime_df.index else None
        volume_amount = realtime_df.loc[fund_code, 'volume_amount'] if fund_code in realtime_df.index else None
        # 单位转换：元 ->万元
        if volume_amount is not None:
            volume_amount = volume_amount / 10000

        nav = nav_df.loc[fund_code, 'nav'] if fund_code in nav_df.index else None
        nav_date = nav_df.loc[fund_code, 'nav_date'] if fund_code in nav_df.index else None

        # ---------- 修改：使用规范化日期比较 ----------
        if nav_date is not None:
            norm_nav = normalize_date(nav_date)
            if norm_nav is not None and norm_nav == norm_target:
                # 日期一致，使用完整格式的 nav_date
                nav_date = norm_nav
            else:
                print(f"  ⚠️ 基金 {fund_code} 净值日期 {nav_date} 与目标日期 {target_date} 不一致，跳过净值更新")
                nav = None
                nav_date = None

        cursor.execute("SELECT 1 FROM lof_history WHERE fund_code=%s AND date=%s", (fund_code, target_date))
        exists = cursor.fetchone() is not None

        if exists:
            updates = []
            params = []
            if close is not None:
                updates.append("close_price = %s")
                params.append(close)
            if volume_amount is not None:
                updates.append("volume_amount = %s")
                params.append(volume_amount)
            if nav is not None:
                updates.append("nav = %s")
                params.append(nav)
            if nav_date is not None:
                updates.append("nav_date = %s")
                params.append(nav_date)
            if updates:
                sql = f"UPDATE lof_history SET {', '.join(updates)} WHERE fund_code=%s AND date=%s"
                params.extend([fund_code, target_date])
                cursor.execute(sql, params)
                updated += 1
        else:
            cursor.execute('''
                INSERT INTO lof_history (
                    fund_code, date, close_price, nav_date, nav,
                    volume_amount, index_change, heavy_change
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (fund_code, target_date, close, nav_date, nav,
                  volume_amount, None, None))
            updated += 1

    conn.commit()
    conn.close()
    print(f"补全完成，共更新/插入 {updated} 条记录（日期 {target_date}）")



def backfill_shares_for_missing_funds():
    """
    为集思录无数据的基金回填份额数据。
    先调用 fetch_merged_shares() 确保快照表有最新数据，然后更新 lof_history。
    """
    # 1. 抓取最新份额数据并存入快照表
    print("正在从交易所抓取最新份额数据...")
    init_missing_funds()
    fetch_merged_shares()   # 该函数内部会更新 lof_funds_snapshot 表
    
    # 2. 检查是否有需要处理的基金列表
    if not MISSING_JISILU_FUNDS:
        print("没有需要处理的基金列表，请先配置 MISSING_JISILU_FUNDS")
        return
    
    conn = get_db()
    cursor = conn.cursor()
    placeholders = ','.join(['%s'] * len(MISSING_JISILU_FUNDS))
    cursor.execute(f"""
        SELECT fund_code, snapshot_date, fund_shares, shares_add, shares_change 
        FROM lof_funds_snapshot 
        WHERE fund_code IN ({placeholders})
    """, MISSING_JISILU_FUNDS)
    rows = cursor.fetchall()
    
    if not rows:
        print("快照表中没有指定基金的份额数据，可能抓取失败或基金不在列表中")
        conn.close()
        return
    
    updated = 0
    for row in rows:
        cursor.execute('''
            UPDATE lof_history 
            SET fund_shares = %s, shares_add = %s, shares_change = %s
            WHERE fund_code = %s AND date = %s
        ''', (row['fund_shares'], row['shares_add'], row['shares_change'], row['fund_code'], row['snapshot_date']))
        updated += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"份额回填完成，共更新 {updated} 条记录（基金: {', '.join(MISSING_JISILU_FUNDS)}）")


def backfill_shares_from_snapshot():
    """
    将 lof_funds_snapshot 表中的份额数据同步到 lof_history 表中对应日期的记录。
    匹配条件：lof_history.date == lof_funds_snapshot.snapshot_date
    """
    conn = get_db()
    cursor = conn.cursor()
    # 获取快照表中的所有记录
    cursor.execute("SELECT fund_code, snapshot_date, fund_shares, shares_add, shares_change FROM lof_funds_snapshot")
    rows = cursor.fetchall()
    updated = 0
    for row in rows:
        fund_code = row['fund_code']
        snapshot_date = row['snapshot_date']
        fund_shares = row['fund_shares']
        shares_add = row['shares_add']
        shares_change = row['shares_change']
        # 更新 lof_history 中相同日期的记录
        cursor.execute('''
            UPDATE lof_history 
            SET fund_shares = %s, shares_add = %s, shares_change = %s
            WHERE fund_code = %s AND date = %s
        ''', (fund_shares, shares_add, shares_change, fund_code, snapshot_date))
        updated += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"份额数据回填完成，共更新 {updated} 条记录")


def supplement_fund_details():
    """使用 AKShare 补充基金的净值、申购状态等，增加更长超时和重试机制"""
    import logging
    import time
    import traceback
    from concurrent.futures import ThreadPoolExecutor, TimeoutError

    logger = logging.getLogger(__name__)
    logger.info("开始补充净值/申购状态...")

    # 最大重试次数（包含首次调用）
    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"第 {attempt} 次尝试调用 ak.fund_purchase_em()...")
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ak.fund_purchase_em)
                try:
                    # 设置 90 秒超时
                    purchase_df = future.result(timeout=90)
                except TimeoutError:
                    logger.error(f"第 {attempt} 次请求超时（90秒）")
                    if attempt < max_attempts:
                        logger.info("等待 5 秒后重试...")
                        time.sleep(5)
                        continue  # 重试
                    else:
                        logger.error("所有重试均超时，放弃")
                        return
                except Exception as e:
                    logger.error(f"AKShare 调用异常: {e}", exc_info=True)
                    if attempt < max_attempts:
                        logger.info("等待 5 秒后重试...")
                        time.sleep(5)
                        continue
                    else:
                        raise

            elapsed = time.time() - start_time
            logger.info(f"ak.fund_purchase_em() 调用完成，耗时 {elapsed:.2f} 秒")
            break  # 成功则跳出重试循环

        except Exception as e:
            logger.error(f"第 {attempt} 次尝试失败: {e}", exc_info=True)
            if attempt < max_attempts:
                logger.info("等待 5 秒后重试...")
                time.sleep(5)
            else:
                logger.error("所有重试失败，退出")
                return

    # 后续处理（在成功获取 purchase_df 后）
    try:
        if purchase_df.empty:
            logger.warning("AKShare 获取申购状态数据为空")
            return

        logger.info(f"AKShare 返回 {len(purchase_df)} 条记录")
        logger.info(f"数据列名: {purchase_df.columns.tolist()}")
        logger.info(f"示例数据（前两行）:\n{purchase_df.head(2).to_string()}")

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT fund_code FROM lof_funds")
        db_codes = {row[0] for row in cursor.fetchall()}
        logger.info(f"数据库中有 {len(db_codes)} 只基金")

        # 提取基金代码
        try:
            if '基金代码' not in purchase_df.columns:
                raise KeyError(f"列名 '基金代码' 不存在，实际列名: {purchase_df.columns.tolist()}")
            extracted = purchase_df['基金代码'].str.extract(r'(\d{6})')[0]
            df_filtered = purchase_df[extracted.isin(db_codes)]
        except Exception as e:
            logger.error(f"提取基金代码时出错: {e}", exc_info=True)
            raise

        logger.info(f"过滤后匹配到 {len(df_filtered)} 条记录")
        if df_filtered.empty:
            logger.info("没有需要更新的基金")
            conn.close()
            return

        # 获取已存在的净值记录
        cursor.execute("SELECT fund_code, nav_date FROM fund_nav")
        existing_nav = {(row[0], row[1]) for row in cursor.fetchall()}

        batch_size = 300
        total = len(df_filtered)
        total_updated_funds = 0
        total_inserted_nav = 0

        for start in range(0, total, batch_size):
            batch = df_filtered.iloc[start:start + batch_size]
            logger.info(f"处理批次 {start // batch_size + 1}/{(total - 1) // batch_size + 1}，本批 {len(batch)} 条")

            fund_updates = []
            nav_inserts = []

            for _, row in batch.iterrows():
                raw_code = row.get('基金代码', '')
                if not raw_code:
                    continue
                match = re.search(r'(\d{6})', str(raw_code))
                if not match:
                    continue
                code = match.group(1)
                if code not in db_codes:
                    continue

                nav = row.get('最新净值/万份收益')
                if nav is not None and nav != '-':
                    try:
                        nav = float(nav)
                    except:
                        nav = None
                else:
                    nav = None

                if nav is None:
                    continue

                nav_date = row.get('最新净值/万份收益-报告时间')
                if nav_date and isinstance(nav_date, str):
                    nav_date = nav_date.strip()
                    if re.match(r'\d{2}-\d{2}', nav_date):
                        current_year = datetime.now().strftime('%Y')
                        nav_date = f"{current_year}-{nav_date}"

                purchase_status = row.get('申购状态')
                redemption_status = row.get('赎回状态')
                daily_limit = row.get('日累计限定金额')
                if daily_limit is not None and daily_limit != '-':
                    try:
                        daily_limit = float(daily_limit)
                    except:
                        daily_limit = None
                else:
                    daily_limit = None

                fund_updates.append((nav, nav_date, purchase_status, redemption_status, daily_limit, code))

                if nav_date and nav is not None:
                    key = (code, nav_date)
                    if key not in existing_nav:
                        nav_inserts.append((code, nav_date, nav))
                        existing_nav.add(key)

            if fund_updates:
                try:
                    cursor.executemany('''
                        UPDATE lof_funds 
                        SET nav=%s, nav_date=%s, purchase_status=%s, redemption_status=%s, daily_purchase_limit=%s
                        WHERE fund_code=%s
                    ''', fund_updates)
                    total_updated_funds += len(fund_updates)
                    logger.info(f"本批次更新了 {len(fund_updates)} 只基金的净值/状态")
                except Exception as e:
                    logger.error(f"更新 lof_funds 失败: {e}", exc_info=True)
                    conn.rollback()
                    raise

            if nav_inserts:
                try:
                    cursor.executemany(
                        "INSERT INTO fund_nav (fund_code, nav_date, nav) VALUES (%s, %s, %s) ON CONFLICT (fund_code, nav_date) DO NOTHING",
                        nav_inserts
                    )
                    total_inserted_nav += len(nav_inserts)
                    logger.info(f"本批次新增 {len(nav_inserts)} 条净值记录")
                except Exception as e:
                    logger.error(f"插入 fund_nav 失败: {e}", exc_info=True)
                    conn.rollback()
                    raise

            conn.commit()

        conn.close()
        logger.info(f"净值同步全部完成，共更新 {total_updated_funds} 只基金，新增 {total_inserted_nav} 条净值记录")

    except Exception as e:
        logger.error(f"处理数据时发生异常: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        raise

    logger.info("supplement_fund_details 执行完毕")



def fetch_estimated_nav_from_tiantian(fund_code: str, retry: int = 2) -> Optional[Dict[str, Any]]:
    """
    从天天基金接口获取单个基金的实时估算净值（支持重试）
    返回: {'estimated_nav': float, 'estimated_time': str, 'nav_date': str, 'gszzl': float}
    """
    url = f"https://fundgz.1234567.com.cn/js/{fund_code}.js"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://fund.eastmoney.com/'
    }
    for attempt in range(retry + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            resp.encoding = 'utf-8'
            text = resp.text
            # 去除 jsonpgz( 和 ) 包裹
            if text.startswith('jsonpgz('):
                json_str = text[8:-2]  # 去掉 'jsonpgz(' 和最后的 ');'
            else:
                json_str = text
            data = json.loads(json_str)
            if data.get('gsz') and data['gsz'] != '-':
                return {
                    'estimated_nav': float(data['gsz']),
                    'estimated_time': data.get('gztime', ''),
                    'nav_date': data.get('jzrq', ''),
                    'gszzl': float(data.get('gszzl', 0))
                }
            else:
                # 数据格式正确但无估算值，不重试
                logging.debug(f"天天基金 {fund_code} 无估算净值数据")
                return None
        except Exception as e:
            logging.warning(f"天天基金获取 {fund_code} 估算净值失败 (尝试 {attempt+1}/{retry+1}): {e}")
            if attempt < retry:
                time.sleep(0.5)
            else:
                return None
    return None


def update_estimated_nav():
    """
    更新全量基金的估算净值
    1. 优先使用 AKShare 批量接口
    2. 未获取到的基金使用天天基金单只接口多线程补充
    注意：名称中包含“定开”的基金将跳过估算净值更新
    """
    print(f"{datetime.now()}: [估算净值] 开始获取估算净值数据...")

    # ----- 获取需要跳过的定开基金代码 -----
    conn_skip = get_db()
    cursor_skip = conn_skip.cursor()
    cursor_skip.execute("SELECT fund_code FROM lof_funds WHERE fund_name LIKE '%%定开%%'  OR fund_code LIKE '506%%' or fund_code='501070'")
    skip_codes = {row['fund_code'] for row in cursor_skip.fetchall()}
    conn_skip.close()
    if skip_codes:
        print(f"跳过定开基金（不更新估算净值）: {skip_codes}")

    # ---------- 第一步：使用 akshare 批量获取 ----------
    success_codes = set()  # 记录成功更新的基金代码
    try:
        df = ak.fund_value_estimation_em()
        if df.empty:
            print("AKShare 估算净值数据为空")
        else:
            print(f"AKShare 估算净值接口返回 {len(df)} 条记录")
            # 查找估算净值所在的列名
            target_col = None
            for col in df.columns:
                if '估算数据' in col and '估算值' in col:
                    target_col = col
                    break
            if target_col is None:
                for col in df.columns:
                    if '估算值' in col:
                        target_col = col
                        break
            if target_col is None:
                print("错误：未找到估算净值列")
            else:
                print(f"使用列名: {target_col}")
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute("SELECT fund_code FROM lof_funds")
                db_codes = set(row['fund_code'] for row in cursor.fetchall())
                updates = []
                for _, row in df.iterrows():
                    raw_code = row.get('基金代码', '')
                    if not raw_code:
                        continue
                    match = re.search(r'(\d{6})', str(raw_code))
                    if not match:
                        continue
                    code = match.group(1)
                    if code not in db_codes:
                        continue
                    # 跳过定开基金
                    if code in skip_codes:
                        continue
                    est_val = row.get(target_col)
                    if est_val is not None and est_val != '-':
                        try:
                            if isinstance(est_val, str):
                                est_val = est_val.strip()
                            est_nav = float(est_val)
                            updates.append((est_nav, code))
                            success_codes.add(code)
                        except:
                            pass
                cursor.executemany("UPDATE lof_funds SET estimated_nav = %s WHERE fund_code = %s", updates)
                conn.commit()
                conn.close()
                print(f"AKShare 估算净值更新完成，更新了 {len(updates)} 只基金")
    except Exception as e:
        print(f"AKShare估算净值更新失败: {e}")

    # ---------- 第二步：多线程使用天天基金接口补充 ----------
    # 获取数据库中所有基金代码
    conn2 = get_db()
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT fund_code FROM lof_funds")
    all_codes = [row['fund_code'] for row in cursor2.fetchall()]
    conn2.close()

    # 未成功的基金 = 所有代码 - 成功更新代码 - 需要跳过的定开代码
    missing_codes = [code for code in all_codes if code not in success_codes and code not in skip_codes]
    if missing_codes:
        print(f"AKShare 未获取到 {len(missing_codes)} 只基金，开始使用天天基金接口多线程补充（并发数=5）...")

        def update_single_with_tt(code):
            """单只基金补充任务（线程安全）"""
            result = fetch_estimated_nav_from_tiantian(code, retry=2)
            if result:
                try:
                    conn = get_db()
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE lof_funds SET estimated_nav = %s WHERE fund_code = %s",
                        (result['estimated_nav'], code)
                    )
                    conn.commit()
                    conn.close()
                    print(f"  ✅ 天天基金补充成功: {code} -> {result['estimated_nav']}")
                    return True
                except Exception as e:
                    print(f"  ❌ 数据库更新失败 {code}: {e}")
                    return False
            else:
                print(f"  ⚠️ 天天基金未获取到数据: {code}")
                return False

        from concurrent.futures import ThreadPoolExecutor, as_completed
        updated_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(update_single_with_tt, code): code for code in missing_codes}
            for future in as_completed(futures):
                if future.result():
                    updated_count += 1
                time.sleep(0.1)

        print(f"天天基金接口多线程补充完成，共更新 {updated_count} 只基金")
    else:
        print("所有基金均已通过 AKShare 更新或属于定开基金跳过，无需天天基金补充")

    print(f"{datetime.now()}: 估算净值更新流程结束")


def update_estimated_premium_rate():
    """根据当前价格和估算净值计算估算溢价率 = (价格/估算净值 - 1)*100"""
    print(f"{datetime.now()}: 开始计算估算溢价率...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT fund_code, current_price, estimated_nav
        FROM lof_funds
        WHERE current_price IS NOT NULL AND estimated_nav IS NOT NULL AND estimated_nav > 0
    """)
    rows = cursor.fetchall()
    updated = 0
    for row in rows:
        price = row['current_price']
        est_nav = row['estimated_nav']
        if price and est_nav and est_nav > 0:
            est_premium = (price / est_nav - 1) * 100
            cursor.execute("UPDATE lof_funds SET estimated_premium_rate = %s WHERE fund_code = %s", (est_premium, row['fund_code']))
            updated += 1
    conn.commit()
    conn.close()
    print(f"估算溢价率计算完成，更新了 {updated} 只基金")

def update_premium_rate():
    """根据当前价格和官方净值计算溢价率 = (价格/净值 - 1)*100"""
    print(f"{datetime.now()}: 开始计算溢价率...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code, current_price, nav FROM lof_funds WHERE current_price IS NOT NULL AND nav IS NOT NULL AND nav > 0")
    rows = cursor.fetchall()
    updated = 0
    for row in rows:
        price = row['current_price']
        nav = row['nav']
        if price and nav and nav > 0:
            premium = (price / nav - 1) * 100
            cursor.execute("UPDATE lof_funds SET premium_rate = %s WHERE fund_code = %s", (premium, row['fund_code']))
            updated += 1
    conn.commit()
    conn.close()
    print(f"溢价率计算完成，更新了 {updated} 只基金")

# ---------- 持仓数据（天天基金）----------
def fetch_holdings_from_eastmoney(fund_code):
    """从天天基金网页抓取基金前十大持仓（股票代码、名称、占净值比例、持股数）"""
    url = f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=10&year=&month="
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": f"http://fundf10.eastmoney.com/ccmx_{fund_code}.html",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "utf-8"
        text = resp.text
        match = re.search(r'var\s+apidata\s*=\s*({.*?});', text, re.DOTALL)
        if not match: return []
        apidata_str = match.group(1)
        content_match = re.search(r'content\s*:\s*"((?:[^"\\]|\\.)*)"', apidata_str, re.DOTALL)
        if not content_match: return []
        html_content = content_match.group(1)
        html_content = html_content.encode().decode('unicode-escape')
        try:
            html_content = html_content.encode('latin-1').decode('utf-8')
        except: pass
        html_content = html.unescape(html_content)
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        if not table: return []
        rows = table.find_all('tr')
        if len(rows) < 2: return []
        header_row = rows[0]
        header_cells = header_row.find_all(['th', 'td'])
        header_texts = [cell.get_text(strip=True) for cell in header_cells]
        code_idx = name_idx = ratio_idx = shares_idx = None
        for i, txt in enumerate(header_texts):
            if '代码' in txt: code_idx = i
            elif '名称' in txt: name_idx = i
            elif '占净值比例' in txt: ratio_idx = i
            elif '持股数' in txt: shares_idx = i
        if code_idx is None and len(header_texts) > 1: code_idx = 1
        if name_idx is None and len(header_texts) > 2: name_idx = 2
        if ratio_idx is None and len(header_texts) > 3: ratio_idx = 3
        if shares_idx is None and len(header_texts) > 4: shares_idx = 4
        holdings = []
        rank = 1
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) <= max(filter(None, [code_idx, name_idx, ratio_idx, shares_idx])): continue
            stock_code = cells[code_idx].get_text(strip=True)
            stock_name = cells[name_idx].get_text(strip=True)
            ratio_str = cells[ratio_idx].get_text(strip=True).replace('%', '')
            shares_str = cells[shares_idx].get_text(strip=True).replace(',', '')
            if not stock_code or stock_code == '股票代码' or '合计' in stock_name: continue
            nav_ratio = float(ratio_str) if ratio_str else None
            shares = float(shares_str) if shares_str else None
            holdings.append({
                "stock_code": stock_code, "stock_name": stock_name,
                "nav_ratio": nav_ratio, "shares": shares, "holding_rank": rank,
            })
            rank += 1
            if rank > 10: break
        return holdings
    except Exception as e:
        print(f"获取持仓数据失败 {fund_code}: {e}")
        return []

def update_single_fund_holdings(fund_code):
    """更新单只基金的持仓数据（删除旧数据后插入新数据）"""
    try:
        print(f"开始更新 {fund_code} 持仓...")
        holdings = fetch_holdings_from_eastmoney(fund_code)
        if holdings:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM lof_holdings WHERE fund_code = %s", (fund_code,))
            for h in holdings:
                cursor.execute('''
                    INSERT INTO lof_holdings 
                    (fund_code, stock_code, stock_name, nav_ratio, shares, holding_rank)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (fund_code, h['stock_code'], h['stock_name'], h['nav_ratio'], h['shares'], h['holding_rank']))
            conn.commit()
            conn.close()
            print(f"✅ 更新 {fund_code} 持仓，共 {len(holdings)} 条")
        else:
            print(f"⚠️ {fund_code} 无持仓数据")
    except Exception as e:
        print(f"❌ {fund_code} 更新失败: {e}")
    time.sleep(random.uniform(0.5, 1.0))

def update_all_holdings_multithread(max_workers=3):
    """多线程并发更新所有基金的持仓数据"""
    print(f"{datetime.now()}: 开始多线程更新持仓（并发数={max_workers}）...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code FROM lof_funds")
    funds = cursor.fetchall()
    conn.close()
    fund_codes = [row['fund_code'] for row in funds]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_single_fund_holdings, code): code for code in fund_codes}
        for future in concurrent.futures.as_completed(futures):
            code = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{code} 线程异常: {e}")
    print("所有基金持仓多线程更新完成")

# ---------- 资产配置抓取 ----------
def fetch_asset_allocation_from_eastmoney(fund_code):
    """从天天基金抓取基金资产配置（股票、债券、现金比例及净资产）"""
    url = f"https://fundf10.eastmoney.com/zcpz_{fund_code}.html"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, 'html.parser')
        target_table = soup.find('table', class_='w782 comm tzxq')
        if not target_table:
            print(f"❌ 未找到资产配置明细表格: {fund_code}")
            return None
        rows = target_table.find_all('tr')
        asset_data_list = []
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) >= 5:
                report_date = cols[0].get_text(strip=True)
                stock_ratio_str = cols[1].get_text(strip=True).replace('%', '')
                bond_ratio_str = cols[2].get_text(strip=True).replace('%', '')
                cash_ratio_str = cols[3].get_text(strip=True).replace('%', '')
                net_assets_str = cols[4].get_text(strip=True)
                net_assets_match = re.search(r"([\d.]+)", net_assets_str)
                net_assets = float(net_assets_match.group(1)) if net_assets_match else None
                if stock_ratio_str and stock_ratio_str != '---':
                    asset_data_list.append({
                        "report_date": report_date,
                        "stock_ratio": float(stock_ratio_str),
                        "bond_ratio": float(bond_ratio_str) if bond_ratio_str != '---' else None,
                        "cash_ratio": float(cash_ratio_str) if cash_ratio_str != '---' else None,
                        "net_assets": net_assets,
                    })
        if asset_data_list:
            latest_asset = asset_data_list[0]
            print(f"✅ 资产配置解析成功 {fund_code}: 报告期: {latest_asset['report_date']}, 股票: {latest_asset['stock_ratio']}%, 净资产: {latest_asset['net_assets']}亿元")
            return latest_asset
        else:
            print(f"⚠️ 未找到有效的资产配置数据: {fund_code}")
            return None
    except Exception as e:
        print(f"❌ 资产配置抓取失败 {fund_code}: {e}")
        return None

def update_single_asset_allocation(fund_code):
    """更新单只基金的资产配置数据"""
    try:
        print(f"更新 {fund_code} 资产配置...")
        data = fetch_asset_allocation_from_eastmoney(fund_code)
        if data and data.get('report_date'):
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO lof_asset_allocation 
                (fund_code, report_date, stock_ratio, bond_ratio, cash_ratio, net_assets)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (fund_code, report_date) DO UPDATE SET
                    stock_ratio = EXCLUDED.stock_ratio,
                    bond_ratio = EXCLUDED.bond_ratio,
                    cash_ratio = EXCLUDED.cash_ratio,
                    net_assets = EXCLUDED.net_assets
            ''', (fund_code, data['report_date'], data['stock_ratio'], data['bond_ratio'], data['cash_ratio'], data['net_assets']))
            conn.commit()
            conn.close()
            print(f"✅ {fund_code} 资产配置更新成功")
        else:
            print(f"⚠️ {fund_code} 无资产配置数据")
    except Exception as e:
        print(f"❌ {fund_code} 更新失败: {e}")
    time.sleep(random.uniform(0.3, 0.8))

def update_all_asset_allocation_multithread(max_workers=5):
    """多线程并发更新所有基金的资产配置"""
    print(f"{datetime.now()}: 开始多线程更新资产配置（并发数={max_workers}）...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code FROM lof_funds")
    funds = cursor.fetchall()
    conn.close()
    fund_codes = [row['fund_code'] for row in funds]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_single_asset_allocation, code): code for code in fund_codes}
        for future in concurrent.futures.as_completed(futures):
            code = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{code} 线程异常: {e}")
    print("所有基金资产配置多线程更新完成")

# ---------- 指数数据更新（包括新增纳斯达克100和恒生科技）----------
# 指数列表（使用与股票行情系统兼容的代码）
INDEX_REALTIME_LIST = [
    '000300',   # 沪深300
    'HSI',      # 恒生指数
    '000905',   # 中证500
    '399006',   # 创业板指
    'NDX',      # 纳斯达克100
    'HSTECH',   # 恒生科技指数
]

# 映射到 index_daily 表中的 index_code
INDEX_CODE_MAP = {
    '000300': 'CSI300',
    'HSI': 'HSI',
    '000905': 'CSI500',
    '399006': 'CYB',
    'NDX': 'NDX',
    'HSTECH': 'HSTECH',
}

def update_index_realtime():
    """盘中实时更新指数涨跌幅（复用现有股票行情获取函数）"""
    now = datetime.now()
    # 交易日判断：周一至周五
    if now.weekday() >= 5:
        logging.info("非交易日，跳过指数实时更新")
        return
    # 交易时段 9:30-15:00
    if not (now.hour == 9 and now.minute >= 30) and not (10 <= now.hour < 15) and not (now.hour == 15 and now.minute == 0):
        logging.info("非交易时段，跳过指数实时更新")
        return
    
    today = now.strftime('%Y-%m-%d')
    logging.info(f"开始盘中实时更新指数涨跌幅（{today}）...")
    
    # 利用现有的批量获取股票行情函数（带缓存）
    quotes = get_cached_batch_stock_quote(INDEX_REALTIME_LIST)
    
    conn = get_db()
    cursor = conn.cursor()
    updated_count = 0
    for code in INDEX_REALTIME_LIST:
        internal_code = INDEX_CODE_MAP.get(code)
        if not internal_code:
            continue
        quote = quotes.get(code)
        if quote and quote.get('change_percent') is not None:
            change_pct = quote['change_percent']
            cursor.execute('''
                INSERT INTO index_daily (index_code, trade_date, change_pct)
                VALUES (%s, %s, %s)
                ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
            ''', (internal_code, today, change_pct))
            updated_count += 1
            logging.debug(f"更新 {internal_code} 实时涨跌幅: {change_pct:.2f}%")
        else:
            logging.warning(f"获取 {internal_code}({code}) 实时涨跌幅失败")
    
    conn.commit()
    conn.close()
    logging.info(f"指数实时更新完成，共更新 {updated_count} 个指数")


def update_nasdaq100_data():
    """使用 efinance 从新浪财经获取纳斯达克100指数历史日线数据"""
    try:
        import efinance as ef
        import pandas as pd

        print("  开始通过 efinance 获取纳斯达克100指数数据...")

        # 获取纳斯达克100指数历史数据
        df = ef.stock.get_quote_history('NDX')

        if df.empty:
            print("  efinance 返回数据为空")
            return

        # 标准化列名
        rename_dict = {}
        for col in df.columns:
            if '日期' in col:
                rename_dict[col] = 'date'
            elif '收盘' in col:
                rename_dict[col] = 'close'
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)

        if 'date' not in df.columns or 'close' not in df.columns:
            print(f"  无法识别日期或收盘价列，实际列名: {df.columns.tolist()}")
            return

        df['trade_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['change_pct'] = (df['close'] / df['close'].shift(1) - 1) * 100
        df = df.dropna(subset=['change_pct'])

        conn = get_db()
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute('''
                INSERT INTO index_daily (index_code, trade_date, change_pct)
                VALUES (%s, %s, %s)
                ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
            ''', ('NDX', row['trade_date'], row['change_pct']))
        conn.commit()
        conn.close()

        print(f"  ✅ 纳斯达克100指数数据更新成功（efinance），共 {len(df)} 条记录")

    except Exception as e:
        print(f"  ❌ efinance 获取失败: {e}")



def update_hstech_data():
    """获取恒生科技指数历史日线数据"""
    try:
        df = ak.stock_hk_index_daily_sina(symbol="HSTECH")
        if df is not None and not df.empty:
            df['trade_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['change_pct'] = (df['close'] / df['close'].shift(1) - 1) * 100
            conn = get_db()
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO index_daily (index_code, trade_date, change_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
                ''', ('HSTECH', row['trade_date'], row['change_pct']))
            conn.commit()
            conn.close()
            print("  恒生科技指数数据更新成功")
        else:
            print("  恒生科技指数数据获取失败")
    except Exception as e:
        print(f"  更新恒生科技指数失败: {e}")

def update_index_data():
    """更新所有监控指数的每日涨跌幅数据（沪深300、恒生、中证500、创业板、纳斯达克100、恒生科技）"""
    print(f"{datetime.now()}: 开始更新指数数据...")
    # 沪深300
    try:
        df_csi = ak.stock_zh_index_daily(symbol="sh000300")
        if df_csi is not None and not df_csi.empty:
            df_csi['trade_date'] = pd.to_datetime(df_csi['date']).dt.strftime('%Y-%m-%d')
            df_csi['change_pct'] = (df_csi['close'] / df_csi['close'].shift(1) - 1) * 100
            conn = get_db()
            cursor = conn.cursor()
            for _, row in df_csi.iterrows():
                cursor.execute('''
                    INSERT INTO index_daily (index_code, trade_date, change_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
                ''', ('CSI300', row['trade_date'], row['change_pct']))
            conn.commit()
            conn.close()
            print("  沪深300指数数据更新成功")
        else:
            print("  沪深300指数数据获取失败")
    except Exception as e:
        print(f"  更新沪深300指数失败: {e}")

    # 恒生指数
    try:
        df_hsi = ak.stock_hk_index_daily_sina(symbol="HSI")
        if df_hsi is not None and not df_hsi.empty:
            df_hsi['trade_date'] = pd.to_datetime(df_hsi['date']).dt.strftime('%Y-%m-%d')
            df_hsi['change_pct'] = (df_hsi['close'] / df_hsi['close'].shift(1) - 1) * 100
            conn = get_db()
            cursor = conn.cursor()
            for _, row in df_hsi.iterrows():
                cursor.execute('''
                    INSERT INTO index_daily (index_code, trade_date, change_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
                ''', ('HSI', row['trade_date'], row['change_pct']))
            conn.commit()
            conn.close()
            print("  恒生指数数据更新成功")
        else:
            print("  恒生指数数据获取失败（akshare 返回空）")
    except Exception as e:
        print(f"  更新恒生指数失败: {e}")

    # 中证500
    try:
        df_csi500 = ak.stock_zh_index_daily(symbol="sh000905")
        if df_csi500 is not None and not df_csi500.empty:
            df_csi500['trade_date'] = pd.to_datetime(df_csi500['date']).dt.strftime('%Y-%m-%d')
            df_csi500['change_pct'] = (df_csi500['close'] / df_csi500['close'].shift(1) - 1) * 100
            conn = get_db()
            cursor = conn.cursor()
            for _, row in df_csi500.iterrows():
                cursor.execute('''
                    INSERT INTO index_daily (index_code, trade_date, change_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
                ''', ('CSI500', row['trade_date'], row['change_pct']))
            conn.commit()
            conn.close()
            print("  中证500指数数据更新成功")
        else:
            print("  中证500指数数据获取失败")
    except Exception as e:
        print(f"  更新中证500指数失败: {e}")

    # 创业板指
    try:
        df_cyb = ak.stock_zh_index_daily(symbol="sz399006")
        if df_cyb is not None and not df_cyb.empty:
            df_cyb['trade_date'] = pd.to_datetime(df_cyb['date']).dt.strftime('%Y-%m-%d')
            df_cyb['change_pct'] = (df_cyb['close'] / df_cyb['close'].shift(1) - 1) * 100
            conn = get_db()
            cursor = conn.cursor()
            for _, row in df_cyb.iterrows():
                cursor.execute('''
                    INSERT INTO index_daily (index_code, trade_date, change_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (index_code, trade_date) DO UPDATE SET change_pct = EXCLUDED.change_pct
                ''', ('CYB', row['trade_date'], row['change_pct']))
            conn.commit()
            conn.close()
            print("  创业板指数据更新成功")
        else:
            print("  创业板指数据获取失败")
    except Exception as e:
        print(f"  更新创业板指失败: {e}")

    # 新增：纳斯达克100指数（用于FOF基金基估）
    update_nasdaq100_data()
    # 新增：恒生科技指数
    update_hstech_data()

    print("指数数据更新完成")

# ---------- 基金分类更新 ----------
def update_all_classifications():
    """根据预设规则更新所有基金的分类（被动指数型 / QDII-FOF / 主动混合型）"""
    print(f"{datetime.now()}: 开始更新基金分类...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code, fund_name FROM lof_funds")
    funds = cursor.fetchall()
    for row in funds:
        code = row['fund_code']
        name = row['fund_name']
        fund_type = classify_fund(code, name)
        cursor.execute('''
            INSERT INTO fund_classification (fund_code, fund_type)
            VALUES (%s, %s)
            ON CONFLICT (fund_code) DO UPDATE SET fund_type = EXCLUDED.fund_type
        ''', (code, fund_type))
    conn.commit()
    conn.close()
    print("基金分类更新完成")

# ---------- 申赎费率抓取 ----------
def fetch_fund_rates_from_eastmoney(fund_code):
    """从天天基金抓取申购费率、赎回费率、管理费、托管费、销售服务费"""
    url = f"https://fundf10.eastmoney.com/jjfl_{fund_code}.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, 'html.parser')
        purchase_fees = []
        for table in soup.find_all('table'):
            prev_h4 = table.find_previous_sibling('h4', class_='t')
            if prev_h4 and ('申购费率' in prev_h4.get_text() or '认购费率' in prev_h4.get_text()):
                rows = table.find_all('tr')
                header_row_idx = -1
                for i, row in enumerate(rows):
                    if '适用金额' in row.get_text():
                        header_row_idx = i
                        break
                if header_row_idx == -1:
                    continue
                for row in rows[header_row_idx + 1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        condition = cells[0].get_text(strip=True)
                        rate_text = cells[1].get_text(strip=True)
                        rate_match = re.search(r'([\d.]+)%', rate_text)
                        if rate_match:
                            rate = float(rate_match.group(1))
                        elif '每笔' in rate_text:
                            rate = None
                        else:
                            rate = None
                        purchase_fees.append({"condition": condition, "rate": rate})
                break
        redeem_fees = []
        for table in soup.find_all('table'):
            prev_h4 = table.find_previous_sibling('h4', class_='t')
            if prev_h4 and '赎回费率' in prev_h4.get_text():
                rows = table.find_all('tr')
                header_row_idx = -1
                for i, row in enumerate(rows):
                    if '适用期限' in row.get_text():
                        header_row_idx = i
                        break
                if header_row_idx == -1:
                    continue
                for row in rows[header_row_idx + 1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        holding = cells[0].get_text(strip=True)
                        rate_str = cells[1].get_text(strip=True).replace('%', '')
                        try:
                            rate = float(rate_str)
                        except:
                            rate = None
                        redeem_fees.append({"holding": holding, "rate": rate})
                break
        management_fee = custody_fee = service_fee = None
        for table in soup.find_all('table'):
            prev_h4 = table.find_previous_sibling('h4', class_='t')
            if prev_h4 and '运作费用' in prev_h4.get_text():
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value_str = cells[1].get_text(strip=True).replace('%', '')
                        try:
                            value = float(value_str)
                        except:
                            value = None
                        if '管理费' in label:
                            management_fee = value
                        elif '托管费' in label:
                            custody_fee = value
                        elif '销售服务费' in label:
                            service_fee = value
                break
        return {
            "purchase_fee": json.dumps(purchase_fees, ensure_ascii=False),
            "redeem_fee": json.dumps(redeem_fees, ensure_ascii=False),
            "management_fee": management_fee,
            "custody_fee": custody_fee,
            "service_fee": service_fee,
        }
    except Exception as e:
        print(f"抓取费率失败 {fund_code}: {e}")
        return None

def update_fund_rates(fund_code):
    """更新单只基金的费率数据"""
    data = fetch_fund_rates_from_eastmoney(fund_code)
    if data:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO fund_rates
            (fund_code, purchase_fee, redeem_fee, management_fee, custody_fee, service_fee, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (fund_code) DO UPDATE SET
                purchase_fee = EXCLUDED.purchase_fee,
                redeem_fee = EXCLUDED.redeem_fee,
                management_fee = EXCLUDED.management_fee,
                custody_fee = EXCLUDED.custody_fee,
                service_fee = EXCLUDED.service_fee,
                updated_at = CURRENT_TIMESTAMP
        ''', (fund_code, data['purchase_fee'], data['redeem_fee'], data['management_fee'], data['custody_fee'], data['service_fee']))
        conn.commit()
        conn.close()
        print(f"费率更新成功: {fund_code}")
    else:
        print(f"费率更新失败: {fund_code}")
    time.sleep(0.2)

def update_all_rates():
    """多线程并发更新所有基金的费率"""
    print(f"{datetime.now()}: 开始多线程更新费率...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code FROM lof_funds")
    funds = cursor.fetchall()
    conn.close()
    fund_codes = [row['fund_code'] for row in funds]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(update_fund_rates, code): code for code in fund_codes}
        for future in concurrent.futures.as_completed(futures):
            code = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{code} 线程异常: {e}")
    print("所有基金费率多线程更新完成")

# ---------- 辅助函数 ----------
def pct_to_float(val):
    """将百分比字符串转换为浮点数，如 '12.34%' -> 12.34"""
    if pd.isna(val): return None
    if isinstance(val, str):
        val = val.strip()
        if val == '' or val == '-':
            return None
        if val.endswith('%'):
            try:
                return float(val[:-1])
            except:
                return None
    try:
        return float(val)
    except:
        return None

def safe_float(val, decimals=3):
    """安全转换为浮点数并保留指定小数位数"""
    if pd.isna(val): return None
    try:
        v = float(val)
        return round(v, decimals)
    except:
        return None

def format_limit(value):
    """格式化日限购金额显示"""
    if pd.isna(value) or value is None: return "-"
    if value == 0: return "-"
    if value >= 1e8: return "不限"
    if value < 10000: return f"{value:.0f}元/日"
    return f"{value / 10000:.0f}万/日"

def format_amount(value):
    """格式化金额（万元/亿元）"""
    if pd.isna(value) or value is None: return "-"
    if value >= 1e8: return f"{value / 1e8:.2f}亿"
    if value >= 1e4: return f"{value / 1e4:.2f}万"
    return f"{value:.0f}"


# ---------- 滚动线性回归预测基估(K) ----------
def predict_jigu_by_regression(fund_code, current_date, prev_nav, lookback_days=30, indices=None, force_recalc=False):
    """
    返回: predicted_nav (float) 或 None
    """
    if indices is None:
        indices = REGRESSION_INDICES
    conn = get_db()
    cursor = conn.cursor()
    
    today = datetime.now().strftime('%Y-%m-%d')
    is_today = (current_date == today)
    need_recalc = force_recalc or is_today
    
    if not need_recalc:
        # 从独立表中读取已有的 pred_return
        cursor.execute("SELECT pred_return FROM fund_pred_returns WHERE fund_code = %s AND date = %s", (fund_code, current_date))
        row = cursor.fetchone()
        if row and row['pred_return'] is not None:
            pred_return = row['pred_return']
            predicted_nav = prev_nav * (1 + pred_return / 100)
            conn.close()
            return predicted_nav
    
    # 获取当前日期的各指数涨跌幅
    current_features = []
    for idx_code in indices:
        cursor.execute("SELECT change_pct FROM index_daily WHERE index_code = %s AND trade_date = %s", (idx_code, current_date))
        row = cursor.fetchone()
        if not row or row['change_pct'] is None:
            conn.close()
            return None
        current_features.append(row['change_pct'])
    
    cursor.execute('''
        SELECT date, nav
        FROM lof_history
        WHERE fund_code = %s AND nav IS NOT NULL AND date < %s
        ORDER BY date ASC
    ''', (fund_code, current_date))
    nav_records = cursor.fetchall()
    if len(nav_records) < 5:
        conn.close()
        return None
    
    X, y = [], []
    for i in range(1, len(nav_records)):
        prev_nav_val = nav_records[i-1]['nav']
        curr_date = nav_records[i]['date']
        curr_nav = nav_records[i]['nav']
        if prev_nav_val is None or curr_nav is None:
            continue
        features = []
        for idx_code in indices:
            cursor.execute("SELECT change_pct FROM index_daily WHERE index_code = %s AND trade_date = %s", (idx_code, curr_date))
            row = cursor.fetchone()
            if not row or row['change_pct'] is None:
                break
            features.append(row['change_pct'])
        if len(features) != len(indices):
            continue
        daily_return = (curr_nav / prev_nav_val - 1) * 100
        X.append(features)
        y.append(daily_return)
    
    if len(X) < 5:
        conn.close()
        return None
    X_recent = X[-lookback_days:]
    y_recent = y[-lookback_days:]
    model = LinearRegression()
    model.fit(X_recent, y_recent)
    pred_return = model.predict([current_features])[0]
    predicted_nav = prev_nav * (1 + pred_return / 100)
    
    # 保存到独立表中，增加重试机制
    for retry in range(3):
        try:
            cursor.execute('''
                INSERT INTO fund_pred_returns (fund_code, date, pred_return)
                VALUES (%s, %s, %s)
                ON CONFLICT (fund_code, date) DO UPDATE SET pred_return = EXCLUDED.pred_return, updated_at = CURRENT_TIMESTAMP
            ''', (fund_code, current_date, pred_return))
            conn.commit()
            break
        except Exception as e:
            if 'database is locked' in str(e) and retry < 2:
                time.sleep(0.5)
                continue
            else:
                logging.warning(f"保存 pred_return 失败 {fund_code} {current_date}: {e}")
                break
    
    conn.close()
    return predicted_nav




def calculate_jigu_fallback(prev_nav, csi300, hsi, stock_ratio=0.8, bond_ratio=0.1, cash_ratio=0.1):
    """
    基估(K)的备用计算方法：使用股票、债券、现金的加权收益估算净值
    其中股票部分用沪深300和恒生指数加权代替
    """
    if prev_nav is None:
        return None
    stock_index_change = (csi300 * 0.6 + hsi * 0.2) / stock_ratio if stock_ratio > 0 else 0
    total_return = stock_ratio * (1 + stock_index_change / 100) + bond_ratio + cash_ratio
    return prev_nav * total_return

def calculate_fof_benchmark_change(fund_code, date):
    """计算 FOF 基金的业绩基准组合涨跌幅（基于纳斯达克100和恒生科技）"""
    if fund_code not in FOF_HOLDINGS:
        return None
    benchmark = FOF_HOLDINGS[fund_code]['benchmark']
    total_weight = 0
    weighted_change = 0
    for idx_code, weight in benchmark.items():
        if idx_code == 'BOND':
            continue  # 债券暂忽略
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT change_pct FROM index_daily WHERE index_code = %s AND trade_date = %s", (idx_code, date))
        row = cursor.fetchone()
        conn.close()
        if row and row['change_pct'] is not None:
            weighted_change += weight * row['change_pct']
            total_weight += weight
    if total_weight > 0:
        return weighted_change / total_weight
    return None

def calculate_fof_etf_change(fund_code):
    """计算 FOF 基金底层 ETF 组合的实时加权涨跌幅（用于估值K）"""
    if fund_code not in FOF_HOLDINGS:
        return None
    underlying = FOF_HOLDINGS[fund_code]['underlying']
    if not underlying:
        return None
    # 提取所有 ETF 代码
    etf_codes = [etf[0] for etf in underlying]
    # 批量获取实时行情（使用缓存）
    quotes = get_cached_batch_stock_quote(etf_codes)
    total_weight = 0
    weighted_change = 0
    for etf, weight in underlying:
        if etf in quotes and quotes[etf]['change_percent'] is not None:
            weighted_change += weight * quotes[etf]['change_percent']
            total_weight += weight
    if total_weight > 0:
        return weighted_change / total_weight
    return None

# ================== 新增：统一估值(K)和基估(K)计算函数 ==================
def calculate_valuation_and_benchmark(
    fund_code: str,
    fund_type: str,
    date: str,
    prev_nav: Optional[float],
    close: Optional[float],
    nav: Optional[float],
    index_change: Optional[float] = None,
    heavy_change: Optional[float] = None,
    etf_change: Optional[float] = None,
    bench_change: Optional[float] = None,
    index_map: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Optional[float]]:
    """
    统一计算估值(K) 和 基估(K) 相关字段
    返回: {'guzhi', 'premium_rate_k', 'wucha', 'jigu', 'jiyi', 'jicha'}
    注意：基估(K) 只在 date 为当天时从 lof_funds.estimated_nav 获取；历史日期返回 None。
    """
    result = {
        'guzhi': None,
        'premium_rate_k': None,
        'wucha': None,
        'jigu': None,
        'jiyi': None,
        'jicha': None,
    }

    # ---------- 估值(K) ----------
    if fund_type == 'QDII-FOF':
        change_for_val = etf_change if etf_change is not None else calculate_fof_etf_change(fund_code)
        if change_for_val is None:
            change_for_val = heavy_change
    else:
        change_for_val = index_change

    if change_for_val is not None and prev_nav is not None:
        guzhi = prev_nav * (1 + change_for_val / 100)
        result['guzhi'] = guzhi
        if close is not None and guzhi is not None:
            result['premium_rate_k'] = (close / guzhi - 1) * 100
        if nav is not None and nav != 0 and guzhi is not None:
            result['wucha'] = ((guzhi - nav) / nav) * 100

    # ---------- 基估(K) ----------
    today = datetime.now().strftime('%Y-%m-%d')
    if date == today:
        # 当天：从 lof_funds 读取 estimated_nav
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT estimated_nav FROM lof_funds WHERE fund_code = %s", (fund_code,))
            row = cursor.fetchone()
            conn.close()
            if row and row['estimated_nav'] is not None:
                result['jigu'] = row['estimated_nav']
            else:
                result['jigu'] = None
        except Exception as e:
            logging.warning(f"获取 estimated_nav 失败 {fund_code}: {e}")
            result['jigu'] = None
    else:
        # 历史日期：不重新计算，保留原数据库中的值（由调用方处理）
        result['jigu'] = None

    # 计算 jiyi 和 jicha（基于 jigu）
    if result['jigu'] is not None and close is not None:
        result['jiyi'] = (close / result['jigu'] - 1) * 100 if result['jigu'] != 0 else None
    if result['jigu'] is not None and nav is not None and nav != 0:
        result['jicha'] = ((result['jigu'] - nav) / nav) * 100

    return result


# ---------- 历史数据导入（从CSV）----------
def import_history_from_local():
    global scheduler
    scheduler.pause()
    try:
        if not os.path.exists(HISTORY_DATA_DIR):
            print(f"❌ 历史数据目录不存在: {HISTORY_DATA_DIR}")
            return
        files = [f for f in os.listdir(HISTORY_DATA_DIR) if f.endswith('.csv')]
        if not files:
            print(f"❌ 目录中没有CSV文件: {HISTORY_DATA_DIR}")
            return
        print(f"✅ 找到 {len(files)} 个CSV文件，开始导入...")
        total_inserted = 0
        for filename in files:
            match = re.search(r'(\d{6})', filename)
            if not match:
                print(f"⚠️ 文件名 {filename} 无法提取6位基金代码，跳过")
                continue
            fund_code = match.group(1)
            filepath = os.path.join(HISTORY_DATA_DIR, filename)
            try:
                encodings = ['utf-8-sig', 'gbk', 'gb2312', 'gb18030', 'utf-8']
                df = None
                used_enc = None
                for enc in encodings:
                    try:
                        df = pd.read_csv(filepath, encoding=enc)
                        used_enc = enc
                        break
                    except:
                        continue
                if df is None:
                    print(f"❌ 无法读取文件 {filename}，已尝试所有编码")
                    continue
                print(f"  使用编码 {used_enc} 成功读取 {filename}")
                df.columns = [col.strip() for col in df.columns]

                # 列名映射（略，与原代码相同）
                if 'price_dt' in df.columns:
                    df.rename(columns={'price_dt': '日期'}, inplace=True)
                if 'net_value_dt' in df.columns:
                    df.rename(columns={'net_value_dt': '净值日期'}, inplace=True)
                if 'price' in df.columns:
                    df.rename(columns={'price': '收盘价'}, inplace=True)
                if 'net_value' in df.columns:
                    df.rename(columns={'net_value': '净值'}, inplace=True)
                if 'discount_rt' in df.columns:
                    df.rename(columns={'discount_rt': '溢价率'}, inplace=True)
                if 'volume' in df.columns:
                    df.rename(columns={'volume': '成交额(万元)'}, inplace=True)
                if 'amount' in df.columns:
                    df.rename(columns={'amount': '场内份额(万份)'}, inplace=True)
                if 'amount_incr' in df.columns:
                    df.rename(columns={'amount_incr': '场内新增(万份)'}, inplace=True)
                if 'amount_increase_rt' in df.columns:
                    df.rename(columns={'amount_increase_rt': '份额涨幅'}, inplace=True)
                if 'ref_increase_rt' in df.columns:
                    df.rename(columns={'ref_increase_rt': '指数涨幅'}, inplace=True)

                required_base = ['日期', '收盘价', '净值']
                missing = [c for c in required_base if c not in df.columns]
                if missing:
                    print(f"⚠️ 文件 {filename} 缺少基础列 {missing}，跳过")
                    continue

                # 获取基金类型（一次，用于整个文件）
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute("SELECT fund_type FROM fund_classification WHERE fund_code = %s", (fund_code,))
                fund_type_row = cursor.fetchone()
                fund_type = fund_type_row['fund_type'] if fund_type_row else '主动混合型'
                conn.close()

                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                df = df.dropna(subset=['日期'])
                df = df.sort_values('日期')
                inserted = 0

                for _, row in df.iterrows():
                    date_obj = row['日期']
                    date = date_obj.strftime('%Y-%m-%d')
                    close = row['收盘价']
                    if pd.isna(close): continue
                    close = float(close)
                    nav = row['净值']
                    nav = float(nav) if not pd.isna(nav) else None
                    index_change = pct_to_float(row.get('指数涨幅'))
                    heavy_change = pct_to_float(row.get('重仓涨幅'))
                    nav_date = row.get('净值日期')
                    nav_date = nav_date if not pd.isna(nav_date) else None
                    premium_rate = pct_to_float(row.get('溢价率'))
                    volume = safe_float(row.get('成交额(万元)'), 2)
                    shares = safe_float(row.get('场内份额(万份)'), 0)
                    shares_add = safe_float(row.get('场内新增(万份)'), 0)
                    shares_change = pct_to_float(row.get('份额涨幅'))

                    # 使用通用插入函数，传入基金类型以节省查询
                    success = insert_or_replace_lof_history(
                        fund_code=fund_code,
                        date=date,
                        close=close,
                        nav=nav,
                        nav_date=nav_date,
                        index_change=index_change,
                        heavy_change=heavy_change,
                        premium_rate=premium_rate,
                        volume_amount=volume,
                        fund_shares=shares,
                        shares_add=shares_add,
                        shares_change=shares_change,
                        fund_type=fund_type,   # 传入已查好的类型
                        # prev_nav 和 index_map 不传，让函数内部自动获取
                    )
                    if success:
                        inserted += 1
                        total_inserted += 1
                    else:
                        print(f"  ⚠️ 插入失败: {fund_code} {date}")

                    if inserted % 100 == 0:
                        # 通用函数内部已提交，这里无需提交
                        pass
                print(f"✅ 导入 {filename} 成功，共 {inserted} 条（基金代码：{fund_code}）")
            except Exception as e:
                print(f"❌ 导入 {filename} 失败: {e}")
        print(f"🎉 历史数据导入完成，共插入 {total_inserted} 条记录")

        # 重新计算动态加权字段
        print("开始计算所有基金的动态加权字段（动估/动差/动溢）...")
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT fund_code FROM lof_history")
        funds_with_history = cursor.fetchall()
        conn.close()
        for (code,) in funds_with_history:
            calculate_dynamic_fields_for_fund(code)
        print("动态加权字段计算完成")
    finally:
        scheduler.resume()

# ---------- 动态加权计算（动估/动差/动溢）---------
def calculate_dynamic_fields_for_fund(fund_code, lookback_days=25, decay=0.95, error_compensation_days=15):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT date, nav, guzhi, jigu, close_price, donggu
        FROM lof_history
        WHERE fund_code = %s
        ORDER BY date ASC
    ''', (fund_code,))
    rows = cursor.fetchall()
    if not rows:
        print(f"{fund_code} 无历史数据，跳过")
        conn.close()
        return

    # 确定“当前”日期（避开周末未来数据泄露）
    now = datetime.now()
    if now.weekday() >= 5:
        last_trading_day = now
        while last_trading_day.weekday() >= 5:
            last_trading_day -= timedelta(days=1)
        today = last_trading_day.strftime('%Y-%m-%d')
    else:
        today = now.strftime('%Y-%m-%d')

    # ---------- 训练阶段（仅使用历史数据，不含当天）----------
    history_rows = [r for r in rows if r['date'] < today and r['nav'] is not None and r['guzhi'] is not None and r['jigu'] is not None]
    valid_history = []
    for r in history_rows:
        valid_history.append({
            'date': r['date'],
            'nav': r['nav'],
            'est1': r['guzhi'],
            'est2': r['jigu'],
            'close': r['close_price']
        })

    if len(valid_history) >= 5:
        recent = valid_history[-lookback_days:] if len(valid_history) > lookback_days else valid_history
        best_w = 0.5
        min_loss = float('inf')
        step = 0.01
        n = len(recent)
        weights = [decay ** (n - 1 - i) for i in range(n)]
        for w in [round(i * step, 2) for i in range(20, 81)]:
            weighted_sse = 0
            for idx, d in enumerate(recent):
                combined = w * d['est1'] + (1 - w) * d['est2']
                err = combined - d['nav']
                weighted_sse += weights[idx] * (err * err)
            if weighted_sse < min_loss:
                min_loss = weighted_sse
                best_w = w
      #  print(f"{fund_code} 指数加权最优权重 w = {best_w:.3f} (估值K权重)")
    else:
        best_w = 0.5
        print(f"{fund_code} 有效历史数据不足5条，使用默认权重 w = 0.5")

    # 计算误差补偿系数
    temp_history = []
    for d in valid_history:
        donggu = best_w * d['est1'] + (1 - best_w) * d['est2']
        dongcha = (donggu - d['nav']) / d['nav'] * 100 if d['nav'] != 0 else None
        temp_history.append({'dongcha': dongcha})
    valid_dongcha = [t['dongcha'] for t in temp_history if t['dongcha'] is not None]
    if len(valid_dongcha) >= error_compensation_days:
        recent_dongcha = valid_dongcha[-error_compensation_days:]
        avg_error = sum(recent_dongcha) / len(recent_dongcha)
       # print(f"{fund_code} 最近{len(recent_dongcha)}天平均动差 = {avg_error:.2f}%")
    else:
        avg_error = 0
    compensation_factor = 1 / (1 + avg_error / 100) if avg_error != 0 else 1.0

    # ---------- 增量更新阶段：仅处理缺失值或当天记录 ----------
    updated = 0
    for row in rows:
        date = row['date']
        nav = row['nav']
        guzhi = row['guzhi']
        jigu = row['jigu']
        close = row['close_price']
        existing_donggu = row['donggu']  # 已有动估值

        # 只更新两种情况：1) 当前记录尚无动估；2) 当天数据（允许刷新）
        if guzhi is not None and jigu is not None and (existing_donggu is None or date == today):
            donggu_initial = best_w * guzhi + (1 - best_w) * jigu
            donggu_compensated = donggu_initial * compensation_factor
            dongcha = None
            if nav is not None and nav != 0:
                dongcha = (donggu_compensated - nav) / nav * 100
            dongyi = None
            if close is not None and close != 0 and donggu_compensated != 0:
                dongyi = (close / donggu_compensated - 1) * 100
            cursor.execute('''
                UPDATE lof_history
                SET donggu = %s, dongcha = %s, dongyi = %s
                WHERE fund_code = %s AND date = %s
            ''', (donggu_compensated, dongcha, dongyi, fund_code, date))
            updated += 1
           # if date == today:
           #    print(f"  更新当天 {date} 动估/动差/动溢")
           # else:
           #     print(f"  补充缺失 {date} 动估/动差/动溢")

    conn.commit()
    conn.close()
   # print(f"{fund_code} 动估/动差/动溢增量更新完成，共更新 {updated} 条记录")


# ---------- 集思录数据抓取（全量）----------
def fetch_jisilu_history(fund_code, days=365):
    """从集思录接口抓取指定基金的历史日线数据（最近 days 天）"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    base_url = f"https://www.jisilu.cn/data/lof/hist_list/{fund_code}"
    all_data = []
    page = 1
    page_size = 100
    while True:
        params = {'page': page, 'size': page_size, 'start': start_date, 'end': end_date}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.jisilu.cn/',
            'X-Requested-With': 'XMLHttpRequest'
        }
        try:
            resp = session.get(base_url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get('rows', [])
            if not rows:
                break
            all_data.extend(rows)
            if len(rows) < page_size:
                break
            page += 1
            time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            print(f"抓取 {fund_code} 失败: {e}")
            break
    session.close()
    if not all_data:
        return pd.DataFrame()
    records = [item['cell'] for item in all_data]
    df = pd.DataFrame(records)
    rename_map = {
        'price_dt': '日期', 'price': '收盘价', 'net_value': '净值', 'discount_rt': '溢价率',
        'volume': '成交额(万元)', 'amount': '场内份额(万份)', 'amount_incr': '场内新增(万份)',
        'amount_increase_rt': '份额涨幅', 'ref_increase_rt': '指数涨幅', 'net_value_dt': '净值日期'
    }
    df.rename(columns=rename_map, inplace=True)
    required_cols = ['日期', '收盘价', '净值']
    if not all(col in df.columns for col in required_cols):
        print(f"缺少必要列，实际列名: {df.columns.tolist()}")
        return pd.DataFrame()
    if '溢价率' in df.columns:
        df['溢价率'] = pd.to_numeric(df['溢价率'], errors='coerce')
        # 注意：集思录返回的 discount_rt 可能是折价率（负数为溢价），此处直接使用
    df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
    # 初始化可能缺失的派生列
    for col in ['基估(K)', '基溢(K)', '基差(K)', '动估(K)', '动差(K)', '动溢(K)', '估值(K)', '误差(K)', '溢价率(K)', '重仓涨幅']:
        if col not in df.columns:
            df[col] = None
    return df

def update_single_jisilu_fund(fund_code):
    """抓取单只基金的集思录历史数据并保存为 CSV 文件"""
    try:
        print(f"线程开始: {fund_code}")
        df = fetch_jisilu_history(fund_code)
        if df.empty:
            print(f"  {fund_code} 无数据，跳过")
            return
        csv_path = os.path.join(HISTORY_DATA_DIR, f"{fund_code}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"✅ {fund_code} 已保存 {len(df)} 条记录")
    except Exception as e:
        print(f"❌ {fund_code} 抓取失败: {e}")

def update_all_jisilu_multithread(max_workers=5):
    """多线程并发抓取所有基金的集思录历史数据并保存为 CSV"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_code FROM lof_funds")
    funds = cursor.fetchall()
    conn.close()
    fund_codes = [row['fund_code'] for row in funds]
    print(f"开始多线程抓取 {len(fund_codes)} 只基金，并发数={max_workers}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_single_jisilu_fund, code): code for code in fund_codes}
        for future in concurrent.futures.as_completed(futures):
            code = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{code} 线程异常: {e}")
    print("所有基金集思录数据多线程抓取完成")

def fetch_latest_jisilu_history(fund_code):
    """获取单只基金最近集思录历史数据（用于增量更新）"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    url = f"https://www.jisilu.cn/data/lof/hist_list/{fund_code}"
    params = {'page': 1, 'size': 5, 'start': start_date, 'end': end_date}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.jisilu.cn/',
        'X-Requested-With': 'XMLHttpRequest'
    }
    try:
        resp = session.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get('rows', [])
        if not rows:
            return None
        latest = None
        latest_date = None
        for item in rows:
            cell = item['cell']
            date_str = cell.get('price_dt')
            if not date_str:
                continue
            if latest_date is None or date_str > latest_date:
                latest_date = date_str
                latest = cell
        if not latest:
            return None
        record = {
            '日期': latest.get('price_dt'),
            '收盘价': latest.get('price'),
            '净值': latest.get('net_value'),
            '溢价率': latest.get('discount_rt'),
            '成交额(万元)': latest.get('volume'),
            '场内份额(万份)': latest.get('amount'),
            '场内新增(万份)': latest.get('amount_incr'),
            '份额涨幅': latest.get('amount_increase_rt'),
            '指数涨幅': latest.get('ref_increase_rt'),
            '净值日期': latest.get('net_value_dt'),
        }
        if record['溢价率'] is not None:
            try:
                record['溢价率'] = float(record['溢价率'])
            except:
                record['溢价率'] = None
        for key in ['收盘价', '净值', '成交额(万元)', '场内份额(万份)', '场内新增(万份)', '份额涨幅', '指数涨幅']:
            if record[key] is not None:
                try:
                    record[key] = float(record[key])
                except:
                    record[key] = None
        return record
    except Exception as e:
        print(f"抓取 {fund_code} 最新数据失败: {e}")
        return None

def recalculate_fund_history(fund_code):
    """重新计算单只基金的历史派生字段（估值K、误差K、溢价率K），但不覆盖基估(K)"""
    print(f"开始重新计算基金 {fund_code} 的历史派生字段...")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT date, close_price, nav, index_change, heavy_change
        FROM lof_history
        WHERE fund_code = %s
        ORDER BY date ASC
    ''', (fund_code,))
    rows = cursor.fetchall()
    if not rows:
        print(f"基金 {fund_code} 无历史数据，跳过")
        conn.close()
        return

    # 获取基金类型
    cursor.execute("SELECT fund_type FROM fund_classification WHERE fund_code = %s", (fund_code,))
    fund_type_row = cursor.fetchone()
    fund_type = fund_type_row['fund_type'] if fund_type_row else '主动混合型'

    # 获取指数映射（用于估值K）
    cursor.execute("SELECT trade_date, index_code, change_pct FROM index_daily")
    index_data = cursor.fetchall()
    index_map = {}
    for row in index_data:
        date = row['trade_date']
        code = row['index_code']
        if date not in index_map:
            index_map[date] = {}
        index_map[date][code] = row['change_pct'] or 0

    prev_nav = None
    for row in rows:
        date = row['date']
        close = row['close_price']
        nav = row['nav']
        index_change = row['index_change']
        heavy_change = row['heavy_change']

        # 使用统一函数计算（基估返回 None，历史不更新）
        calc = calculate_valuation_and_benchmark(
            fund_code=fund_code,
            fund_type=fund_type,
            date=date,
            prev_nav=prev_nav,
            close=close,
            nav=nav,
            index_change=index_change,
            heavy_change=heavy_change,
            index_map=index_map
        )
        guzhi = calc['guzhi']
        premium_rate_k = calc['premium_rate_k']
        wucha = calc['wucha']

        # 只更新估值相关字段，不更新基估(K)、基差(K)、基溢(K)
        cursor.execute('''
            UPDATE lof_history 
            SET guzhi=%s, wucha=%s, premium_rate_k=%s
            WHERE fund_code=%s AND date=%s
        ''', (guzhi, wucha, premium_rate_k, fund_code, date))

        if nav is not None:
            prev_nav = nav

    conn.commit()
    conn.close()
    print(f"基金 {fund_code} 历史派生字段重新计算完成")
    calculate_dynamic_fields_for_fund(fund_code)

def insert_or_replace_lof_history(
    fund_code: str,
    date: str,
    close: float,
    nav: Optional[float],
    nav_date: Optional[str],
    index_change: Optional[float],
    heavy_change: Optional[float],
    premium_rate: Optional[float] = None,
    volume_amount: Optional[float] = None,
    fund_shares: Optional[float] = None,
    shares_add: Optional[float] = None,
    shares_change: Optional[float] = None,
    etf_change: Optional[float] = None,
    fund_type: Optional[str] = None,
    prev_nav: Optional[float] = None,
    index_map: Optional[Dict[str, Dict[str, float]]] = None,
    low: Optional[float] = None,   # 新增
) -> bool:
    """
    插入或更新 lof_history 记录。
    对于 jigu、jiyi、jicha 字段，如果新值为 NULL（历史日期），则保留数据库中的旧值。
    """
    try:
        # 1. 获取基金类型（如果未提供）
        if fund_type is None:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT fund_type FROM fund_classification WHERE fund_code = %s", (fund_code,))
            row = cursor.fetchone()
            fund_type = row['fund_type'] if row else '主动混合型'
            conn.close()

        # 2. 获取前一条净值（如果未提供）
        if prev_nav is None:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT nav FROM lof_history
                WHERE fund_code = %s AND date < %s
                ORDER BY date DESC LIMIT 1
            ''', (fund_code, date))
            row = cursor.fetchone()
            prev_nav = row['nav'] if row else None
            conn.close()

        # 3. 获取指数映射（如果未提供）
        if index_map is None:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT trade_date, index_code, change_pct FROM index_daily")
            rows = cursor.fetchall()
            index_map = {}
            for r in rows:
                dt = r['trade_date']
                code = r['index_code']
                if dt not in index_map:
                    index_map[dt] = {}
                index_map[dt][code] = r['change_pct'] or 0
            conn.close()

        # 4. 如果是 FOF 且未提供 etf_change，则计算
        if fund_type == 'QDII-FOF' and etf_change is None:
            etf_change = calculate_fof_etf_change(fund_code)

        # 5. 计算估值和基估
        calc = calculate_valuation_and_benchmark(
            fund_code=fund_code,
            fund_type=fund_type,
            date=date,
            prev_nav=prev_nav,
            close=close,
            nav=nav,
            index_change=index_change,
            heavy_change=heavy_change,
            etf_change=etf_change,
            index_map=index_map
        )

        # 计算净值涨跌幅
        nav_change_pct = None
        if nav is not None and prev_nav is not None and prev_nav != 0:
            nav_change_pct = (nav / prev_nav - 1) * 100

        # 6. 写入数据库（使用 UPSERT，基估相关字段仅在非 NULL 时更新）
        conn = get_db()
        cursor = conn.cursor()
        for retry in range(3):
            try:
                cursor.execute('''
                    INSERT INTO lof_history 
                    (fund_code, date, close_price, nav_date, nav,
                    jigu, jiyi, jicha, donggu, dongcha, dongyi,
                    guzhi, wucha, premium_rate_k, premium_rate,
                    volume_amount, fund_shares, shares_add, shares_change,
                    index_change, heavy_change, nav_change_pct, low)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fund_code, date) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        nav_date = EXCLUDED.nav_date,
                        nav = EXCLUDED.nav,
                        jigu = COALESCE(EXCLUDED.jigu, lof_history.jigu),
                        jiyi = COALESCE(EXCLUDED.jiyi, lof_history.jiyi),
                        jicha = COALESCE(EXCLUDED.jicha, lof_history.jicha),
                        donggu = EXCLUDED.donggu,
                        dongcha = EXCLUDED.dongcha,
                        dongyi = EXCLUDED.dongyi,
                        guzhi = EXCLUDED.guzhi,
                        wucha = EXCLUDED.wucha,
                        premium_rate_k = EXCLUDED.premium_rate_k,
                        premium_rate = EXCLUDED.premium_rate,
                        volume_amount = EXCLUDED.volume_amount,
                        fund_shares = EXCLUDED.fund_shares,
                        shares_add = EXCLUDED.shares_add,
                        shares_change = EXCLUDED.shares_change,
                        index_change = EXCLUDED.index_change,
                        heavy_change = EXCLUDED.heavy_change,
                        nav_change_pct = EXCLUDED.nav_change_pct,
                        low = EXCLUDED.low
                ''', (fund_code, date, close, nav_date, nav,
                    calc['jigu'], calc['jiyi'], calc['jicha'], None, None, None,
                    calc['guzhi'], calc['wucha'], calc['premium_rate_k'], premium_rate,
                    volume_amount, fund_shares, shares_add, shares_change,
                    index_change, heavy_change, nav_change_pct, low))
                conn.commit()
                break
            except Exception as e:
                if 'database is locked' in str(e) and retry < 2:
                    time.sleep(0.5)
                    continue
                else:
                    raise
        conn.close()
        return True
    except Exception as e:
        logging.error(f"插入/替换 lof_history 记录失败 {fund_code} {date}: {e}")
        return False


def get_tencent_history_low(fund_code, start_date=None, end_date=None):
    """
    从腾讯获取基金历史最低价（备选接口，不稳定）
    返回 DataFrame 包含 date 和 low
    """
    import requests
    import json
    # 确定市场前缀
    if fund_code.startswith('5') or fund_code.startswith('6'):
        market = 'sh'
    else:
        market = 'sz'
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}_{fund_code},day,19700101,20991231,,qfq"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        # 数据结构: data[market_code][day] = [["日期","开盘","收盘","最高","最低","成交量","成交额"], ...]
        key = f"{market}_{fund_code}"
        if key not in data.get('data', {}):
            return pd.DataFrame()
        kline = data['data'][key].get('day', [])
        if not kline:
            return pd.DataFrame()
        # 转换
        records = []
        for item in kline:
            if len(item) >= 6:
                records.append({
                    'date': item[0],
                    'low': float(item[4])
                })
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        print(f"腾讯历史接口请求失败: {e}")
        return pd.DataFrame()


def update_low_prices(fund_code=None, days=90, use_tencent=False):
    """
    从 AKShare（或腾讯备选）获取 LOF 基金历史最低价，更新到 lof_history 表。
    :param fund_code: 指定基金代码，为 None 时更新所有基金
    :param days: 获取最近 days 天的数据（仅用于限定范围，实际由数据源决定）
    :param use_tencent: 强制使用腾讯接口（测试用）
    """
    import akshare as ak
    import time

    if fund_code:
        codes = [fund_code]
    else:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT fund_code FROM lof_funds")
        codes = [row[0] for row in cursor.fetchall()]
        conn.close()

    print(f"{datetime.now()}: 开始更新 {len(codes)} 只基金的最低价数据...")
    total_updated = 0

    for code in codes:
        try:
            if use_tencent:
                raise Exception("强制使用腾讯接口")  # 跳过 AKShare

            df = ak.fund_lof_hist_em(symbol=code)
            if df.empty:
                print(f"  {code}: AKShare 无数据，尝试腾讯备选...")
                df = get_tencent_history_low(code)
            else:
                # 重命名列
                rename_map = {
                    '日期': 'date',
                    '最低': 'low',
                    '收盘': 'close',
                    '开盘': 'open',
                    '最高': 'high',
                    '成交量': 'volume',
                    '成交额': 'amount'
                }
                df.rename(columns=rename_map, inplace=True)
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df[['date', 'low']]

        except Exception as e:
            print(f"  {code}: AKShare 失败 ({e})，尝试腾讯备选...")
            df = get_tencent_history_low(code)

        if df.empty:
            print(f"  {code}: 所有数据源均无数据，跳过")
            continue

        # 统一更新
        conn = get_db()
        cursor = conn.cursor()
        updated = 0
        for _, row in df.iterrows():
            date_str = row['date']
            low_val = row['low']
            if low_val is None or pd.isna(low_val):
                continue
            cursor.execute("""
                UPDATE lof_history
                SET low = %s
                WHERE fund_code = %s AND date = %s
            """, (float(low_val), code, date_str))
            updated += cursor.rowcount
        conn.commit()
        conn.close()
        print(f"  {code}: 更新了 {updated} 条记录")
        total_updated += updated
        time.sleep(0.5)

    print(f"最低价数据更新完成，共更新 {total_updated} 条记录")

def backfill_yesterday_nav_for_missing_funds():
    """
    针对 MISSING_JISILU_FUNDS 列表，补全昨天（date = 昨天）的净值缺失。
    从 fund_nav 表获取 nav，更新到 lof_history。
    同时，更新后重新计算该基金的衍生字段（估值K、动估K等）。
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"{datetime.now()}: 开始补全 {yesterday} 的缺失净值（仅限缺失基金列表）...")

    if not MISSING_JISILU_FUNDS:
        print("没有配置缺失基金列表")
        return

    conn = get_db()
    cursor = conn.cursor()
    placeholders = ','.join(['%s'] * len(MISSING_JISILU_FUNDS))

    # 查询这些基金中昨天 nav 为 NULL 的记录
    cursor.execute(f"""
        SELECT fund_code, date, id
        FROM lof_history
        WHERE fund_code IN ({placeholders}) AND date = %s AND nav IS NULL
    """, MISSING_JISILU_FUNDS + [yesterday])
    rows = cursor.fetchall()

    if not rows:
        print(f"{yesterday} 没有缺失净值的记录")
        conn.close()
        return

    updated = 0
    for row in rows:
        fund_code = row['fund_code']
        date_str = row['date']
        # 从 fund_nav 获取该日净值
        cursor.execute(
            "SELECT nav, nav_date FROM fund_nav WHERE fund_code = %s AND nav_date = %s",
            (fund_code, date_str)
        )
        nav_row = cursor.fetchone()
        if nav_row and nav_row['nav'] is not None:
            cursor.execute(
                "UPDATE lof_history SET nav = %s, nav_date = %s WHERE id = %s",
                (nav_row['nav'], nav_row['nav_date'], row['id'])
            )
            updated += 1
        else:
            print(f"⚠️ 未在 fund_nav 中找到 {fund_code} 在 {date_str} 的净值")

    conn.commit()
    conn.close()
    print(f"补全完成，共更新 {updated} 条记录")

    # 对每个更新的基金重新计算衍生字段
    for row in rows:
        fund_code = row['fund_code']
        recalculate_fund_history(fund_code)  # 重算估值K等
        calculate_dynamic_fields_for_fund(fund_code)


# ================== 新增：仅更新基础数据，不重算派生指标 ==================

def fetch_recent_jisilu_history(fund_code: str, days: int = 5) -> List[Dict]:
    """
    获取基金最近 days 天的历史记录（从集思录），返回记录列表（按日期升序）。
    每条记录包含：日期, 收盘价, 净值, 溢价率, 成交额(万元), 场内份额(万份), 
    场内新增(万份), 份额涨幅, 指数涨幅, 净值日期。
    若失败返回空列表。
    """
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    url = f"https://www.jisilu.cn/data/lof/hist_list/{fund_code}"
    params = {'page': 1, 'size': days, 'start': start_date, 'end': end_date}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.jisilu.cn/',
        'X-Requested-With': 'XMLHttpRequest'
    }
    try:
        resp = session.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get('rows', [])
        if not rows:
            return []
        records = []
        for item in rows:
            cell = item['cell']
            record = {
                '日期': cell.get('price_dt'),
                '收盘价': cell.get('price'),
                '净值': cell.get('net_value'),
                '溢价率': cell.get('discount_rt'),
                '成交额(万元)': cell.get('volume'),
                '场内份额(万份)': cell.get('amount'),
                '场内新增(万份)': cell.get('amount_incr'),
                '份额涨幅': cell.get('amount_increase_rt'),
                '指数涨幅': cell.get('ref_increase_rt'),
                '净值日期': cell.get('net_value_dt'),
            }
            # 数值字段转换
            for key in ['收盘价', '净值', '溢价率', '成交额(万元)', '场内份额(万份)', 
                        '场内新增(万份)', '份额涨幅', '指数涨幅']:
                if record.get(key) is not None:
                    try:
                        record[key] = float(record[key])
                    except:
                        record[key] = None
            records.append(record)
        records.sort(key=lambda x: x['日期'] if x['日期'] else '')
        return records
    except Exception as e:
        print(f"抓取 {fund_code} 最近 {days} 天数据失败: {e}")
        return []


def insert_or_replace_lof_history_basic(
    fund_code: str,
    date: str,
    close: float,
    nav: Optional[float],
    nav_date: Optional[str],
    index_change: Optional[float],
    heavy_change: Optional[float],
    premium_rate: Optional[float] = None,
    volume_amount: Optional[float] = None,
    fund_shares: Optional[float] = None,
    shares_add: Optional[float] = None,
    shares_change: Optional[float] = None,
) -> bool:
    """
    仅更新 lof_history 的基础字段（不涉及估值K、基估K、动估K等派生指标）。
    保留已有派生字段不变。
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        for retry in range(3):
            try:
                cursor.execute('''
                    INSERT INTO lof_history 
                    (fund_code, date, close_price, nav_date, nav,
                     premium_rate, volume_amount, fund_shares, shares_add, shares_change,
                     index_change, heavy_change)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fund_code, date) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        nav_date = EXCLUDED.nav_date,
                        nav = EXCLUDED.nav,
                        premium_rate = EXCLUDED.premium_rate,
                        volume_amount = EXCLUDED.volume_amount,
                        fund_shares = EXCLUDED.fund_shares,
                        shares_add = EXCLUDED.shares_add,
                        shares_change = EXCLUDED.shares_change,
                        index_change = EXCLUDED.index_change,
                        heavy_change = EXCLUDED.heavy_change
                ''', (fund_code, date, close, nav_date, nav,
                      premium_rate, volume_amount, fund_shares, shares_add, shares_change,
                      index_change, heavy_change))
                conn.commit()
                break
            except Exception as e:
                if 'database is locked' in str(e) and retry < 2:
                    time.sleep(0.5)
                    continue
                else:
                    raise
        conn.close()
        return True
    except Exception as e:
        logging.error(f"基础写入 lof_history 失败 {fund_code} {date}: {e}")
        return False


@app.route('/admin/backfill_yesterday_nav')
def admin_backfill_yesterday_nav():
    """补全缺失基金昨天的净值"""
    def task():
        backfill_yesterday_nav_for_missing_funds()
    threading.Thread(target=task).start()
    return "✅ 已启动补全昨天缺失净值任务，请查看后台日志"



@app.route('/admin/backfill_nav_missing')
def admin_backfill_nav_missing():
    """专门针对 MISSING_JISILU_FUNDS 列表中的基金回填净值"""
    def task():
        backfill_nav_from_nav_table()  # 此函数内部已处理，但我们可以只针对缺失列表？原函数是全局的，我们不改动，直接调用。
    threading.Thread(target=task).start()
    return f"✅ 已启动针对 {len(MISSING_JISILU_FUNDS)} 只缺失基金的净值回填任务，请查看后台日志"

@app.route('/admin/update_low_prices')
def admin_update_low_prices():
    fund_code = request.args.get('fund_code')
    use_tencent = request.args.get('use_tencent', '').lower() in ('1', 'true', 'yes')
    def task():
        update_low_prices(fund_code=fund_code, use_tencent=use_tencent)
    threading.Thread(target=task).start()
    return f"✅ 已启动最低价更新任务{f'（基金 {fund_code}）' if fund_code else ''}{'（强制腾讯）' if use_tencent else ''}，请查看后台日志"


@app.route('/admin/update_all_recent')
def admin_update_all_recent():
    """
    管理员接口：更新所有基金最近 N 天的历史数据（从集思录），默认5天。
    多线程获取 + 多线程写入，只更新基础字段，不重算派生指标。
    """
    days = request.args.get('days', default=5, type=int)
    if days < 1 or days > 30:
        return "❌ 天数请控制在1~30之间", 400

    def task():
        print(f"{datetime.now()}: 开始多线程更新所有基金最近 {days} 天数据（并发获取+写入）...")
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT fund_code FROM lof_funds")
        fund_codes = [row['fund_code'] for row in cursor.fetchall()]
        conn.close()
        print(f"共 {len(fund_codes)} 只基金需要更新，并发数=3")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        import random

        # 线程安全计数器
        lock = threading.Lock()
        success_funds = 0
        fail_funds = 0
        total_records = 0

        def write_records(fund_code, records):
            """将记录写入数据库（内部重试）"""
            nonlocal success_funds, fail_funds, total_records
            if not records:
                with lock:
                    fail_funds += 1
                return

            local_success = 0
            for rec in records:
                date_str = rec['日期']
                if not date_str:
                    continue
                close = rec['收盘价']
                nav = rec['净值']
                index_change = rec.get('指数涨幅')
                heavy_change = None
                nav_date = rec.get('净值日期')
                premium_rate = rec.get('溢价率')
                volume = rec.get('成交额(万元)')
                shares = rec.get('场内份额(万份)')
                shares_add = rec.get('场内新增(万份)')
                shares_change = rec.get('份额涨幅')

                # 写入重试（最多3次）
                for attempt in range(3):
                    try:
                        conn_write = get_db()
                        cursor_write = conn_write.cursor()
                        cursor_write.execute('''
                            INSERT INTO lof_history 
                            (fund_code, date, close_price, nav_date, nav,
                             premium_rate, volume_amount, fund_shares, shares_add, shares_change,
                             index_change, heavy_change)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (fund_code, date) DO UPDATE SET
                                close_price = EXCLUDED.close_price,
                                nav_date = EXCLUDED.nav_date,
                                nav = EXCLUDED.nav,
                                premium_rate = EXCLUDED.premium_rate,
                                volume_amount = EXCLUDED.volume_amount,
                                fund_shares = EXCLUDED.fund_shares,
                                shares_add = EXCLUDED.shares_add,
                                shares_change = EXCLUDED.shares_change,
                                index_change = EXCLUDED.index_change,
                                heavy_change = EXCLUDED.heavy_change
                        ''', (fund_code, date_str, close if close is not None else 0.0,
                              nav_date, nav, premium_rate, volume, shares, shares_add,
                              shares_change, index_change, heavy_change))
                        conn_write.commit()
                        conn_write.close()
                        local_success += 1
                        break  # 写入成功，跳出重试循环
                    except Exception as e:
                        if 'database is locked' in str(e) and attempt < 2:
                            time.sleep(0.5)
                            continue
                        else:
                            print(f"  ❌ 写入失败 {fund_code} {date_str}: {e}")
                            break
                    except Exception as e:
                        print(f"  ❌ 写入异常 {fund_code} {date_str}: {e}")
                        break

            with lock:
                total_records += local_success
                if local_success > 0:
                    success_funds += 1
                else:
                    fail_funds += 1

        def fetch_and_write(fund_code, retry=2):
            nonlocal fail_funds  # ✅ 声明允许修改外部变量
            """获取数据并立即写入"""
            for attempt in range(retry):
                try:
                    # 随机延迟，避免同时请求
                    time.sleep(random.uniform(0.3, 1.0))
                    records = fetch_recent_jisilu_history(fund_code, days)
                    if records:
                        write_records(fund_code, records)
                        return
                    else:
                        # 无数据，重试一次
                        if attempt < retry - 1:
                            time.sleep(1)
                            continue
                        else:
                            with lock:
                                fail_funds += 1
                            print(f"  ⚠️ {fund_code} 无数据")
                            return
                except Exception as e:
                    print(f"  ⚠️ {fund_code} 抓取失败 (尝试 {attempt+1}/{retry}): {e}")
                    if attempt < retry - 1:
                        time.sleep(2)
                    else:
                        with lock:
                            fail_funds += 1

        # 启动多线程（并发数4）
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_and_write, code) for code in fund_codes]
            for i, future in enumerate(as_completed(futures), 1):
                future.result()  # 等待完成，若有异常会抛出
                if i % 10 == 0:
                    print(f"进度: {i}/{len(fund_codes)} 只基金已处理")

        print(f"写入完成，成功基金 {success_funds} 只，失败 {fail_funds} 只，共写入 {total_records} 条记录")
        print("基础数据更新完成，未重新计算派生指标。如需派生指标，请手动触发 /admin/calc_dynamic_fields_all")

    threading.Thread(target=task).start()
    return f"✅ 已启动所有基金最近 {days} 天基础数据更新任务（多线程获取+写入），请查看后台日志"


# ---------- 其他 API 路由 ----------
# 用于避免重复更新任务的状态集合
updating_holdings_funds = set()
updating_asset_funds = set()
updating_rates_funds = set()
recalculating_funds = set()
updating_latest_funds = set()

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/favorites')
def favorites():
    """自选页面"""
    return render_template('favorites.html')

@app.route('/fund/<fund_code>')
def detail(fund_code):
    """基金详情页面"""
    return render_template('detail.html', fund_code=fund_code)

@app.route('/api/lof/list')
def api_list():
    """API: 获取 LOF 基金列表及实时行情 + 最新份额新增"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT 
            f.fund_code, f.fund_name,
            f.current_price, f.change_percent,
            f.nav, f.nav_date,
            f.premium_rate,
            f.estimated_nav, f.estimated_premium_rate,
            f.purchase_status, f.redemption_status, f.daily_purchase_limit,
            f.total_market_value, f.volume_hands, f.volume_amount,
            f.updated_at,
            s.shares_add  -- 最新份额新增（万份）
        FROM lof_funds f
        LEFT JOIN (
            SELECT fund_code, shares_add, snapshot_date
            FROM lof_funds_snapshot
            WHERE (fund_code, snapshot_date) IN (
                SELECT fund_code, MAX(snapshot_date)
                FROM lof_funds_snapshot
                GROUP BY fund_code
            )
        ) s ON f.fund_code = s.fund_code
        ORDER BY f.current_price DESC
    ''')
    rows = cursor.fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d['daily_limit_display'] = format_limit(d.get('daily_purchase_limit'))
        d['total_mv_display'] = format_amount(d.get('total_market_value'))
        d['volume_amount_display'] = format_amount(d.get('volume_amount'))
        # shares_add 直接传递，前端处理显示
        result.append(d)
    conn.close()
    return jsonify(result)





@app.route('/api/lof/history/<fund_code>')
def api_history(fund_code):
    """API: 获取基金历史日线数据及所有衍生指标"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fund_type FROM fund_classification WHERE fund_code = %s", (fund_code,))
    fund_type_row = cursor.fetchone()
    fund_type = fund_type_row['fund_type'] if fund_type_row else '被动指数型'
    cursor.execute('''
        SELECT date, close_price, nav_date, nav, nav_change_pct,
               jigu, jiyi, jicha, donggu, dongcha, dongyi,
               guzhi, wucha, premium_rate_k, premium_rate,
               volume_amount, fund_shares, shares_add, shares_change,
               index_change, heavy_change,
               low   -- 新增最低价
        FROM lof_history
        WHERE fund_code = %s
        ORDER BY date ASC
    ''', (fund_code,))
    rows = cursor.fetchall()
    conn.close()
    result = []
    prev_nav = None  # 用于计算最低价相对前一日净值的跌幅
    for row in rows:
        item = {
            "date": row['date'], "close": row['close_price'],"low": row['low'],
            "nav_date": row['nav_date'], "nav": row['nav'],"nav_change_pct": row['nav_change_pct'],
            "jigu": row['jigu'], "jiyi": row['jiyi'], "jicha": row['jicha'],
            "donggu": row['donggu'], "dongcha": row['dongcha'], "dongyi": row['dongyi'],
            "guzhi": row['guzhi'], "wucha": row['wucha'],
            "premium_rate_k": row['premium_rate_k'], "premium_rate": row['premium_rate'],
            "volume": row['volume_amount'], "shares": row['fund_shares'],
            "shares_add": row['shares_add'], "shares_change": row['shares_change'],"index_change": row['index_change'],
        }
        # 计算最低价相对前一日净值的跌幅
        low_val = row['low']
        if low_val is not None and prev_nav is not None and prev_nav != 0:
            low_nav_change = (low_val - prev_nav) / prev_nav * 100
        else:
            low_nav_change = None
        item['low_nav_change'] = low_nav_change
        result.append(item)
        # 更新前一日净值（仅当当前净值有效）
        if row['nav'] is not None:
            prev_nav = row['nav']
        
    return jsonify(result)

@app.route('/api/lof/detail/<fund_code>')
def api_detail(fund_code):
    """API: 获取基金详细信息（包括持仓、资产配置、费率，并异步触发缺失数据更新）"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM lof_funds WHERE fund_code = %s', (fund_code,))
    fund = dict(cursor.fetchone() or {})
    
    # 获取基金类型
    cursor.execute("SELECT fund_type FROM fund_classification WHERE fund_code = %s", (fund_code,))
    fund_type_row = cursor.fetchone()
    fund_type = fund_type_row['fund_type'] if fund_type_row else '其他'
    
    # 获取持仓数据（先查数据库）
    cursor.execute('''
        SELECT stock_code, stock_name, nav_ratio, shares, holding_rank
        FROM lof_holdings
        WHERE fund_code = %s
        ORDER BY holding_rank
    ''', (fund_code,))
    db_holdings = [dict(row) for row in cursor.fetchall()]
    
    # 如果是 FOF 基金，使用配置覆盖持仓
    if fund_type == 'QDII-FOF' and fund_code in FOF_HOLDINGS:
        underlying = FOF_HOLDINGS[fund_code]['underlying']
        holdings = []
        for idx, (ticker, weight) in enumerate(underlying, start=1):
            holdings.append({
                'holding_rank': idx,
                'stock_code': ticker,
                'stock_name': ETF_NAMES.get(ticker, ticker),
                'nav_ratio': weight * 100,
                'shares': None,
                'current_price': None,
                'change_percent': None
            })
        print(f"FOF基金 {fund_code} 使用配置持仓，共 {len(holdings)} 条")
    else:
        holdings = db_holdings
        print(f"非FOF基金 {fund_code} 从数据库获取持仓，共 {len(holdings)} 条")
    
    # 获取资产配置
    cursor.execute('''
        SELECT report_date, stock_ratio, bond_ratio, cash_ratio, net_assets
        FROM lof_asset_allocation
        WHERE fund_code = %s
        ORDER BY report_date DESC LIMIT 1
    ''', (fund_code,))
    asset_row = cursor.fetchone()
    asset_allocation = dict(asset_row) if asset_row else None
    
    # 获取费率
    cursor.execute('''
        SELECT purchase_fee, redeem_fee, management_fee, custody_fee, service_fee
        FROM fund_rates WHERE fund_code = %s
    ''', (fund_code,))
    rate_row = cursor.fetchone()
    rates = dict(rate_row) if rate_row else None
    conn.close()
    
    # ========== 异步触发更新任务（如果数据缺失） ==========
    if len(holdings) == 0 and fund_code not in updating_holdings_funds:
        updating_holdings_funds.add(fund_code)
        def async_holdings():
            update_single_fund_holdings(fund_code)
            updating_holdings_funds.discard(fund_code)
        threading.Thread(target=async_holdings).start()
        print(f"后台开始更新 {fund_code} 持仓数据")
    if asset_allocation is None and fund_code not in updating_asset_funds:
        updating_asset_funds.add(fund_code)
        def async_asset():
            update_single_asset_allocation(fund_code)
            updating_asset_funds.discard(fund_code)
        threading.Thread(target=async_asset).start()
        print(f"后台开始更新 {fund_code} 资产配置")
    if rates is None and fund_code not in updating_rates_funds:
        updating_rates_funds.add(fund_code)
        def async_rates():
            update_fund_rates(fund_code)
            updating_rates_funds.discard(fund_code)
        threading.Thread(target=async_rates).start()
        print(f"后台开始更新 {fund_code} 费率")
    if fund_code not in recalculating_funds:
        recalculating_funds.add(fund_code)
        def async_recalc():
            recalculate_fund_history(fund_code)
            recalculating_funds.discard(fund_code)
        threading.Thread(target=async_recalc).start()
        print(f"后台开始重新计算 {fund_code} 历史派生字段")
    
    # ========== 为所有持仓补充实时行情 ==========
    stock_codes = [h['stock_code'] for h in holdings if h.get('stock_code')]
    if stock_codes:
        quotes = get_cached_batch_stock_quote(stock_codes)
        for h in holdings:
            code = h.get('stock_code')
            if code and code in quotes:
                h['current_price'] = quotes[code]['current_price']
                h['change_percent'] = quotes[code]['change_percent']
            else:
                h['current_price'] = h['change_percent'] = None
    else:
        for h in holdings:
            h['current_price'] = h['change_percent'] = None
    
    return jsonify({'info': fund, 'holdings': holdings, 'asset_allocation': asset_allocation, 'fund_type': fund_type, 'rates': rates})

@app.route('/api/lof/compare')
def api_compare():
    """API: 对比多个基金的实时数据"""
    codes = request.args.get('codes', '').split(',')
    if not codes: return jsonify([])
    conn = get_db()
    cursor = conn.cursor()
    placeholders = ','.join(['%s'] * len(codes))
    cursor.execute(f'''
        SELECT fund_code, fund_name, current_price, change_percent, nav, premium_rate, estimated_nav, estimated_premium_rate
        FROM lof_funds WHERE fund_code IN ({placeholders})
    ''', codes)
    rows = cursor.fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])

@app.route('/api/refresh_nav')
def refresh_nav():
    """手动触发净值、估算净值及溢价率刷新（异步）"""
    def task():
        try:
            supplement_fund_details()
            update_premium_rate()
            
            print("✅ 净值刷新任务完成")
        except Exception as e:
            print(f"❌ 净值刷新任务失败: {e}")
    
    threading.Thread(target=task).start()
    return "净值、估算净值刷新任务已启动（后台运行）"



@app.route('/api/refresh_holdings')
def refresh_holdings():
    """手动触发全量持仓数据刷新（多线程）"""
    threading.Thread(target=update_all_holdings_multithread, args=(3,)).start()
    return "全量持仓刷新任务已启动（多线程）"

@app.route('/api/refresh_history')
def refresh_history():
    """手动触发历史数据导入（从本地CSV）"""
    threading.Thread(target=import_history_from_local).start()
    return "历史数据导入任务已启动"

@app.route('/api/update_latest_history/<fund_code>')
def update_latest_history(fund_code):
    if fund_code in updating_latest_funds:
        return f"基金 {fund_code} 最新数据更新已在执行中"
    updating_latest_funds.add(fund_code)

    def task():
        try:
            record = fetch_latest_jisilu_history(fund_code)
            if not record:
                print(f"未获取到 {fund_code} 的最新数据")
                return
            date_str = record['日期']
            if not date_str:
                return

            close = record['收盘价']
            nav = record['净值']
            index_change = record.get('指数涨幅')
            heavy_change = record.get('重仓涨幅')
            nav_date = record.get('净值日期')
            premium_rate = record.get('溢价率')
            volume = record.get('成交额(万元)')
            shares = record.get('场内份额(万份)')
            shares_add = record.get('场内新增(万份)')
            shares_change = record.get('份额涨幅')

            # ---------- 新增：获取当日最低价 ----------
            today = datetime.now().strftime('%Y-%m-%d')
            low_value = None
           
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT day_low FROM lof_funds WHERE fund_code = %s", (fund_code,))
            row = cursor.fetchone()
            if row and row['day_low'] is not None:
                low_value = row['day_low']
            conn.close()
            # -------------------------------------------------

            # 使用通用插入函数，传入 low
            success = insert_or_replace_lof_history(
                fund_code=fund_code,
                date=date_str,
                close=close,
                nav=nav,
                nav_date=nav_date,
                index_change=index_change,
                heavy_change=heavy_change,
                premium_rate=premium_rate,
                volume_amount=volume,
                fund_shares=shares,
                shares_add=shares_add,
                shares_change=shares_change,
                low=low_value,   # 新增：传入最低价
            )
            if success:
                print(f"✅ 更新 {fund_code} 最新历史数据: {date_str}")
                calculate_dynamic_fields_for_fund(fund_code)
            else:
                print(f"❌ 更新 {fund_code} 最新历史数据失败")
        finally:
            updating_latest_funds.discard(fund_code)

    threading.Thread(target=task).start()
    return f"已启动更新 {fund_code} 的最新历史数据"

@app.route('/admin/update_jicha_all')
def admin_update_jicha_all():
    """管理员接口：全量更新所有历史记录的基差(K)字段"""
    def task():
        update_jicha_all()
    threading.Thread(target=task).start()
    return "✅ 已启动全量更新基差(K)任务，请查看后台日志"



# 全局标志，避免重复执行
_updating_all_latest = False



@app.route('/admin/backfill_missing_funds')
def admin_backfill_missing_funds():
    threading.Thread(target=backfill_shares_for_missing_funds).start()
    return f"✅ 已启动份额回填任务，仅针对 {len(MISSING_JISILU_FUNDS)} 只集思录无数据基金，请查看后台日志"



@app.route('/admin/patch_lof_history')
def admin_patch_lof_history():
    threading.Thread(target=patch_lof_history_from_sources).start()
    return "✅ 已启动历史数据补全任务，请查看后台日志"



@app.route('/admin/update_all_latest')
def update_all_latest():
    global _updating_all_latest
    if _updating_all_latest:
        return "❌ 已有全量最新数据更新任务正在执行，请稍后再试"
    _updating_all_latest = True

    def task():
        try:
            print(f"{datetime.now()}: 开始多线程全量更新所有基金的最新一天数据...")
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT fund_code FROM lof_funds")
            fund_codes = [row['fund_code'] for row in cursor.fetchall()]
            conn.close()
            print(f"共 {len(fund_codes)} 只基金需要更新，并发数=5")

            from concurrent.futures import ThreadPoolExecutor, as_completed

            results = []
            lock = threading.Lock()

            def fetch_one(fund_code):
                try:
                    record = fetch_latest_jisilu_history(fund_code)
                    if not record or record.get('日期') is None:
                        with lock:
                            results.append((fund_code, None, "无数据"))
                        return
                    with lock:
                        results.append((fund_code, record, None))
                except Exception as e:
                    with lock:
                        results.append((fund_code, None, str(e)))

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(fetch_one, code) for code in fund_codes]
                for future in as_completed(futures):
                    future.result()

            print(f"数据获取完成，开始写入数据库...")

            success_count = 0
            fail_count = 0
            today = datetime.now().strftime('%Y-%m-%d')

            for fund_code, record, err in results:
                if err or record is None:
                    print(f"  ⚠️ {fund_code} 获取失败: {err or '无数据'}")
                    fail_count += 1
                    continue

                date_str = record['日期']
                close = record['收盘价']
                nav = record['净值']
                index_change = record.get('指数涨幅')
                heavy_change = record.get('重仓涨幅')
                nav_date = record.get('净值日期')
                premium_rate = record.get('溢价率')
                volume = record.get('成交额(万元)')
                shares = record.get('场内份额(万份)')
                shares_add = record.get('场内新增(万份)')
                shares_change = record.get('份额涨幅')

                # ---------- 新增：获取当日最低价 ----------
                low_value = None
               
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute("SELECT day_low FROM lof_funds WHERE fund_code = %s", (fund_code,))
                row = cursor.fetchone()
                if row and row['day_low'] is not None:
                    low_value = row['day_low']
                conn.close()
                # -------------------------------------------------

                success = insert_or_replace_lof_history(
                    fund_code=fund_code,
                    date=date_str,
                    close=close,
                    nav=nav,
                    nav_date=nav_date,
                    index_change=index_change,
                    heavy_change=heavy_change,
                    premium_rate=premium_rate,
                    volume_amount=volume,
                    fund_shares=shares,
                    shares_add=shares_add,
                    shares_change=shares_change,
                    low=low_value,   # 传入最低价
                )
                if success:
                    success_count += 1
                else:
                    fail_count += 1

                if (success_count + fail_count) % 10 == 0:
                    print(f"已写入 {success_count + fail_count} 只，成功 {success_count}，失败 {fail_count}")

            print(f"全量最新数据更新完成，成功: {success_count}, 失败: {fail_count}")

            print("开始重新计算所有基金的动态加权字段...")
            conn2 = get_db()
            cur2 = conn2.cursor()
            cur2.execute("SELECT DISTINCT fund_code FROM lof_history")
            funds_with_history = cur2.fetchall()
            conn2.close()
            for (code,) in funds_with_history:
                calculate_dynamic_fields_for_fund(code)
            print("动态加权字段计算完成")
        finally:
            global _updating_all_latest
            _updating_all_latest = False

    threading.Thread(target=task).start()
    return "✅ 已启动多线程全量最新数据更新任务，请查看后台日志"


@app.route('/api/refresh_index')
def refresh_index():
    """手动触发指数数据更新"""
    threading.Thread(target=update_index_data).start()
    return "指数数据更新任务已启动"

@app.route('/api/recalc_fund/<fund_code>')
def recalc_fund(fund_code):
    """手动触发重新计算单只基金的历史派生字段"""
    threading.Thread(target=recalculate_fund_history, args=(fund_code,)).start()
    return f"基金 {fund_code} 历史数据重新计算任务已启动"

@app.route('/admin/update_all_asset_allocation')
def update_all_asset_allocation():
    """管理员接口：多线程更新所有基金的资产配置"""
    threading.Thread(target=update_all_asset_allocation_multithread, args=(5,)).start()
    return "资产配置批量更新任务已启动（多线程）"

@app.route('/admin/update_classifications')
def admin_update_classifications():
    """管理员接口：更新所有基金的分类"""
    threading.Thread(target=update_all_classifications).start()
    return "基金分类批量更新任务已启动"

@app.route('/admin/update_all_rates')
def admin_update_all_rates():
    """管理员接口：多线程更新所有基金的费率"""
    threading.Thread(target=update_all_rates).start()
    return "全量费率更新任务已启动"

@app.route('/admin/force_refresh_all')
def force_refresh_all():
    def task():
        fetch_realtime_data()
        update_estimated_nav()
        update_estimated_premium_rate()
    threading.Thread(target=task).start()
    return "全量数据刷新（实时行情+估算净值）已启动"

@app.route('/admin/fetch_and_import_jisilu')
def fetch_and_import_jisilu():
    """管理员接口：全量抓取集思录历史数据并导入数据库"""
    def task():
        print("========== 开始多线程全量抓取集思录数据 ==========")
        update_all_jisilu_multithread(max_workers=5)
        print("========== 抓取完成，开始导入数据库 ==========")
        import_history_from_local()
        print("========== 抓取+导入+动态计算全部完成 ==========")
    threading.Thread(target=task).start()
    return "✅ 多线程全量抓取+导入+动态计算任务已启动"



@app.route('/api/test_sync_update/<fund_code>')
def test_sync_update(fund_code):
    record = fetch_latest_jisilu_history(fund_code)
    if not record:
        return jsonify({'error': 'no data'})
    
    date_str = record['日期']
    close = record['收盘价']
    # 直接写入数据库（同步）
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO lof_history (fund_code, date, close_price)
            VALUES (%s, %s, %s)
            ON CONFLICT (fund_code, date) DO UPDATE SET close_price = EXCLUDED.close_price
        ''', (fund_code, date_str, close))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)})
    finally:
        conn.close()
    
    # 查询刚写入的值
    conn2 = get_db()
    cur2 = conn2.cursor()
    cur2.execute("SELECT close_price FROM lof_history WHERE fund_code=%s AND date=%s", (fund_code, date_str))
    row = cur2.fetchone()
    conn2.close()
    
    return jsonify({
        'input_close': close,
        'stored_close': row['close_price'] if row else None
    })

@app.route('/admin/calc_dynamic_fields_all')
def calc_dynamic_fields_all():
    """管理员接口：全量计算所有基金的动估/动差/动溢字段"""
    def task():
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT fund_code FROM lof_history")
        funds = cursor.fetchall()
        conn.close()
        for (code,) in funds:
            calculate_dynamic_fields_for_fund(code)
    threading.Thread(target=task).start()
    return "已启动全量动态字段计算，请查看后台日志"


@app.route('/admin/process_missing_funds_advanced')
def admin_process_missing_funds_advanced():
    """管理员接口：处理 missing_funds 表中的基金（使用股票行情接口）"""
    def task():
        process_missing_funds_advanced()
    threading.Thread(target=task).start()
    return "✅ 已启动缺失基金高级自动处理任务，请查看后台日志"


@app.route('/api/missing_funds_list')
def api_missing_funds_list():
    """返回缺失基金列表（MISSING_JISILU_FUNDS）"""
    return jsonify(MISSING_JISILU_FUNDS)


# ---------- 定时任务 ----------
scheduler = BackgroundScheduler()
# 东方财富实时行情：每30分钟
scheduler.add_job(func=fetch_realtime_data, trigger="interval", minutes=20, id='eastmoney')
# AKShare 净值补充：每天23点
scheduler.add_job(func=supplement_fund_details, trigger="cron", hour=23, minute=0, id='akshare')
# 溢价率计算：每31分钟
scheduler.add_job(func=update_premium_rate, trigger="interval", minutes=31, id='premium')
# 估算净值：每32分钟
scheduler.add_job(func=update_estimated_nav, trigger="interval", minutes=32, id='estimated_nav')
# 估算溢价率：每33分钟
scheduler.add_job(func=update_estimated_premium_rate, trigger="interval", minutes=33, id='estimated_premium')
# 交易时段每30分钟运行一次（函数内部已做时段判断）
scheduler.add_job(func=process_missing_funds_advanced, trigger="cron", hour='9-15', minute='*/30', id='missing_funds_timer')
# 每晚 22:00 运行一次缺失基金处理
scheduler.add_job(func=process_missing_funds_advanced, trigger="cron", hour=22, minute=0, id='missing_funds_night')




scheduler.start()


# ===== 在 gunicorn 环境下自动初始化数据库（表结构） =====
try:
    init_db()
    init_missing_funds()
    print("✅ PostgreSQL 数据库表初始化完成")
except Exception as e:
    print(f"❌ 数据库初始化失败: {e}")


if __name__ == '__main__':
    import os
    # 检测是否为子进程（debug模式下的自动重启）
    if not os.environ.get('WERKZEUG_RUN_MAIN'):
        print("启动初始化（主进程）...")
        init_db()
        init_missing_funds()
        fetch_realtime_data()
        time.sleep(2)
        supplement_fund_details()
        update_premium_rate()
        update_estimated_nav()
        update_estimated_premium_rate()
        update_all_latest()
        process_missing_funds_advanced()
        threading.Thread(target=update_index_data).start()
        threading.Thread(target=calc_dynamic_fields_all).start()
    app.run(debug=False, host='0.0.0.0', port=5000)
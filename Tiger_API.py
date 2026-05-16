#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
老虎证券 API 数据获取核心代码（行情专用版 + 灵活历史查询）
"""

import os
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

import logging
from datetime import datetime, timedelta
from pytz import timezone as pytz_timezone
import pandas as pd

from tigeropen.tiger_open_config import TigerOpenClientConfig
from tigeropen.common.util.signature_utils import read_private_key
from tigeropen.quote.quote_client import QuoteClient
from tigeropen.common.consts import Language, BarPeriod, QuoteRight

# ==================== 配置区 ====================
PRIVATE_KEY_PATH = 'tiger.pem'
TIGER_ID = '20150215'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
# --- 添加这行代码，屏蔽 getmac 的警告 ---
logging.getLogger('getmac').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

SYMBOL_MAPPING = {
    "BRK-B": "BRK.B",
    "BF-B": "BF.B",
    "MOG-A": "MOG.A"
}

# 支持的字段（防止外部传错）
VALID_FIELDS = {'open', 'high', 'low', 'close', 'volume', 'amount'}


def _normalize_symbol(symbol):
    return SYMBOL_MAPPING.get(symbol, symbol)


class TigerDataFetcher:
    def __init__(self, private_key_path: str, tiger_id: str):
        self.private_key_path = private_key_path
        self.tiger_id = tiger_id
        self.quote_client = None
        # 历史K线缓存: {symbol: DataFrame(index=date字符串)}
        self._hist_cache = {}
        self._pe_cache = {}
        self._init_clients()

    def _init_clients(self):
        try:
            client_config = TigerOpenClientConfig()
            client_config.private_key = read_private_key(self.private_key_path)
            client_config.tiger_id = self.tiger_id
            client_config.language = Language.zh_CN
            client_config.timezone = 'US/Eastern'
            self.quote_client = QuoteClient(client_config)
            logger.info("Tiger 行情客户端初始化成功")
        except Exception as e:
            logger.error(f"客户端初始化失败: {e}")
            raise

    # ==================== 实时行情（保持不变） ====================

    def get_historical_pe(self, symbol: str,
                        start_date: str = None,
                        end_date: str = None,
                        market: str = 'US',
                        use_cache: bool = True) -> pd.DataFrame:
        """
        获取单只股票的历史 PE 序列（日频）
        返回: DataFrame, index=date('YYYY-MM-DD'),
            columns 包含 ['pe_ttm', 'pe_lyr']（按接口实际返回为准）
        """
        from tigeropen.common.consts import Market

        symbol = _normalize_symbol(symbol)

        us_eastern = pytz_timezone('US/Eastern')
        if end_date is None:
            end_date = datetime.now(us_eastern).strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now(us_eastern) - timedelta(days=365)).strftime('%Y-%m-%d')

        cache_key = (symbol, start_date, end_date)
        if use_cache and cache_key in getattr(self, '_pe_cache', {}):
            return self._pe_cache[cache_key].copy()

        # ---------- 兼容多版本导入 FinancialDailyField ----------
        FinancialDailyField = None
        for _path in (
            'tigeropen.common.consts.financial_fields',
            'tigeropen.common.consts.filter_fields',
            'tigeropen.common.consts',
        ):
            try:
                _mod = __import__(_path, fromlist=['FinancialDailyField'])
                FinancialDailyField = getattr(_mod, 'FinancialDailyField', None)
                if FinancialDailyField is not None:
                    break
            except ImportError:
                continue

        # 取字段：优先用枚举，拿不到就退回字符串
        if FinancialDailyField is not None:
            try:
                fields = [FinancialDailyField.pe_ttm, FinancialDailyField.pe_lyr]
            except AttributeError:
                fields = ['pe_ttm', 'pe_lyr']
        else:
            fields = ['pe_ttm', 'pe_lyr']
        # -------------------------------------------------------

        market_map = {'US': Market.US, 'HK': Market.HK, 'CN': Market.CN}
        mkt = market_map.get(market.upper(), Market.US)

        # 接口本身是否存在（老版本可能没有）
        if not hasattr(self.quote_client, 'get_financial_daily'):
            logger.error("当前 tigeropen 版本不支持 get_financial_daily，请升级: pip install -U tigeropen")
            return pd.DataFrame()

        try:
            df = self.quote_client.get_financial_daily(
                symbols=[symbol],
                market=mkt,
                fields=fields,
                begin_date=start_date,
                end_date=end_date,
            )
            if df is None or df.empty:
                logger.warning(f"{symbol} 未取到 PE 历史数据")
                return pd.DataFrame()

            # 接口返回常见形式: columns=[symbol, date, field, value]（长表）
            if {'field', 'value', 'date'}.issubset(df.columns):
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                wide = df.pivot_table(
                    index='date', columns='field', values='value', aggfunc='last'
                ).sort_index()
            else:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                    wide = df.set_index('date').sort_index()
                else:
                    wide = df.copy()

            # 有些版本 field 列返回的是枚举对象，pivot 之后列名不是字符串
            wide.columns = [str(c).split('.')[-1] if not isinstance(c, str) else c
                            for c in wide.columns]

            if not hasattr(self, '_pe_cache'):
                self._pe_cache = {}
            self._pe_cache[cache_key] = wide
            return wide.copy()

        except Exception as e:
            logger.error(f"获取 {symbol} 历史 PE 失败: {e}")
            return pd.DataFrame()


    def get_pe_on_date(self, symbol: str, date: str,
                    field: str = 'pe_ttm') -> float:
        """
        取某只股票某一天的 PE（若当天非交易日，向前回溯到最近一个交易日）
        """
        df = self.get_historical_pe(symbol,
                                    start_date=(datetime.strptime(date, '%Y-%m-%d')
                                                - timedelta(days=20)).strftime('%Y-%m-%d'),
                                    end_date=date)
        if df.empty or field not in df.columns:
            return None
        sub = df[df.index <= date]
        if sub.empty:
            return None
        val = sub[field].dropna()
        if val.empty:
            return None
        return float(val.iloc[-1])
    
    def get_realtime_prices(self, symbols):
        if not symbols:
            return {}
        symbols = [_normalize_symbol(s) for s in symbols]
        result = {}
        symbols_list = list(symbols)
        batch_size = 50
        try:
            for i in range(0, len(symbols_list), batch_size):
                batch_symbols = symbols_list[i:i + batch_size]
                briefs = self.quote_client.get_stock_briefs(
                    symbols=batch_symbols,
                    include_hour_trading=True,
                    lang=Language.zh_CN
                )
                if briefs is None or briefs.empty:
                    continue
                for _, row in briefs.iterrows():
                    sym = row.get('symbol')
                    if not sym:
                        continue
                    hour_price = row.get('hour_trading_latest_price')
                    price = None
                    if hour_price is not None and hour_price != '' and not pd.isna(hour_price):
                        try:
                            price = float(hour_price)
                        except Exception:
                            price = None
                    if price is None or price == 0:
                        try:
                            price = float(row.get('latest_price', 0))
                        except Exception:
                            continue
                    if price and price > 0:
                        result[sym] = price
            return result
        except Exception as e:
            logger.error(f"批量获取实时价格失败: {e}")
            return result

    def get_realtime_quote(self, symbol: str) -> dict:
        try:
            symbol = _normalize_symbol(symbol)
            df = self.quote_client.get_stock_briefs(
                symbols=[symbol],
                include_hour_trading=True,
                lang=Language.zh_CN
            )
            if df is None or df.empty:
                return {}
            row = df.iloc[0]
            hour_price = row.get('hour_trading_latest_price')
            if hour_price is not None and hour_price != '':
                price = float(hour_price)
                tag = row.get('hour_trading_tag', '常规')
                is_extended = True
            else:
                price = float(row.get('latest_price', 0))
                tag = '常规'
                is_extended = False
            return {
                'symbol': symbol,
                'price': price,
                'volume': int(row.get('volume', 0)),
                'pre_close': float(row.get('pre_close', 0)),
                'tag': tag,
                'is_extended': is_extended
            }
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            return {}

    # ==================== 历史K线核心方法 ====================

    def get_historical_bars(self, symbol: str, days: int = 100,
                             use_cache: bool = True) -> pd.DataFrame:
        """
        获取历史日K线数据（底层方法，带缓存）
        返回 DataFrame，index 为 date 字符串 'YYYY-MM-DD'
        """
        symbol = _normalize_symbol(symbol)

        # 缓存命中：如果缓存中已有，且数据量够用，直接返回
        if use_cache and symbol in self._hist_cache:
            cached = self._hist_cache[symbol]
            if len(cached) >= days:
                return cached.tail(days).copy()

        us_eastern = pytz_timezone('US/Eastern')
        end_dt = datetime.now(us_eastern)
        begin_dt = end_dt - timedelta(days=days + 30)

        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        begin_time = begin_dt.strftime('%Y-%m-%d %H:%M:%S')

        try:
            df = self.quote_client.get_bars_by_page(
                symbol=symbol,
                period=BarPeriod.DAY,
                begin_time=begin_time,
                end_time=end_time,
                total=5000,
                page_size=1000,
                right=QuoteRight.BR,
                time_interval=0.5
            )
            if df is None or df.empty:
                logger.warning(f"获取 {symbol} 历史日K数据为空")
                return pd.DataFrame()

            df['time'] = pd.to_numeric(df['time'], errors='coerce')
            df['date'] = pd.to_datetime(df['time'], unit='ms') \
                           .dt.tz_localize('UTC') \
                           .dt.tz_convert('US/Eastern') \
                           .dt.strftime('%Y-%m-%d')
            df = df.sort_values('time', ascending=True).reset_index(drop=True)
            df = df.set_index('date')

            # 写入缓存
            self._hist_cache[symbol] = df
            return df.tail(days).copy()
        except Exception as e:
            logger.error(f"获取历史数据失败: {e}")
            return pd.DataFrame()

    def get_historical_bars_by_range(self, symbol: str,
                                      start_date: str = None,
                                      end_date: str = None,
                                      use_cache: bool = True) -> pd.DataFrame:
        """
        按日期范围获取历史K线
        start_date / end_date 格式: 'YYYY-MM-DD'
        """
        symbol = _normalize_symbol(symbol)
        us_eastern = pytz_timezone('US/Eastern')
        today = datetime.now(us_eastern).strftime('%Y-%m-%d')

        if end_date is None:
            end_date = today
        if start_date is None:
            start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

        # 估算需要多少天
        delta_days = (datetime.strptime(end_date, '%Y-%m-%d') -
                      datetime.strptime(start_date, '%Y-%m-%d')).days
        # 拉到今天为止，确保覆盖
        need_days = (datetime.strptime(today, '%Y-%m-%d') -
                     datetime.strptime(start_date, '%Y-%m-%d')).days + 5

        df = self.get_historical_bars(symbol, days=max(need_days, delta_days + 5),
                                       use_cache=use_cache)
        if df.empty:
            return df
        return df.loc[(df.index >= start_date) & (df.index <= end_date)].copy()

    # ==================== 灵活查询接口 ====================

    def get_historical_field(self, symbol: str, date: str,
                              field: str = 'close') -> float:
        """
        获取某只股票在某一天的某个字段值
        :param symbol: 股票代码
        :param date: 'YYYY-MM-DD'，若当天无交易则返回 None
        :param field: 'open'/'high'/'low'/'close'/'volume'
        :return: float 或 None
        """
        if field not in VALID_FIELDS:
            logger.error(f"非法字段: {field}，支持: {VALID_FIELDS}")
            return None

        # 多拉一些天，保证目标日期覆盖在缓存里
        us_eastern = pytz_timezone('US/Eastern')
        today = datetime.now(us_eastern).strftime('%Y-%m-%d')
        try:
            need_days = (datetime.strptime(today, '%Y-%m-%d') -
                         datetime.strptime(date, '%Y-%m-%d')).days + 10
            need_days = max(need_days, 30)
        except Exception:
            need_days = 200

        df = self.get_historical_bars(symbol, days=need_days)
        if df.empty or date not in df.index:
            logger.warning(f"{symbol} 在 {date} 无数据（可能非交易日）")
            return None
        try:
            return float(df.loc[date, field])
        except Exception as e:
            logger.error(f"取值失败: {e}")
            return None

    def get_historical_fields(self, symbol: str,
                               start_date: str = None,
                               end_date: str = None,
                               fields=('close',)) -> pd.DataFrame:
        """
        获取某只股票在一个区间内的一个或多个字段
        :return: DataFrame，index=date，columns=fields
        """
        if isinstance(fields, str):
            fields = [fields]
        for f in fields:
            if f not in VALID_FIELDS:
                logger.error(f"非法字段: {f}")
                return pd.DataFrame()

        df = self.get_historical_bars_by_range(symbol, start_date, end_date)
        if df.empty:
            return df
        return df[list(fields)].copy()

    def get_historical_prices_batch(self, symbols, date: str,
                                     field: str = 'close') -> dict:
        """
        批量：获取一批股票在某一天的某个字段
        返回: {symbol: value}
        """
        result = {}
        for s in symbols:
            v = self.get_historical_field(s, date, field)
            if v is not None:
                result[s] = v
        return result


# ==================== 单例 & 模块级便捷函数 ====================

_global_fetcher = None

def _get_global_fetcher():
    global _global_fetcher
    if _global_fetcher is None:
        _global_fetcher = TigerDataFetcher(
            private_key_path=PRIVATE_KEY_PATH,
            tiger_id=TIGER_ID
        )
    return _global_fetcher

if __name__ == "__main__":
    # 用法演示
    logger.info("--- 1) 单个股票某天的收盘价 ---")

    
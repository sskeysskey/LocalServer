# -*- coding: utf-8 -*-
"""
=====================================================================
 多应用服务器  (OVideo / ONews / Finance / Prediction)
=====================================================================
 文件结构（修改某模块时，只需把对应的节喂给 AI）：
   §1  公共基础（三者共用）
   §2A ONews + OVideo 共用：点数账本 / 邀请码
   §2B 在线客服会话（OVideo 寻片/举报 + ONews/Finance 在线客服）
   §3  OVideo 视频（独享）
   §4  ONews 新闻（独享）
   §5  Finance 美股（独享）
   §6  后台网页 ADMIN_HTML（原样粘贴）
   §7  启动入口

 改 OVideo  → §1 + §2A + §2B + §3
 改 ONews   → §1 + §2A + §4        （涉及客服再加 §2B）
 改 Finance → §1 + §5              （涉及客服再加 §2B）
=====================================================================
"""
import os
import re
import sys
import hmac
import json
import time
import sqlite3
import secrets
import hashlib
import threading
import traceback
from difflib import SequenceMatcher
from functools import wraps
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, send_from_directory, request, g
from flask_cors import CORS
from flask_compress import Compress
from werkzeug.utils import safe_join


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §1 公共基础（OVideo / ONews / Finance 三者共用）                       ║
# ╚══════════════════════════════════════════════════════════════════════╝

# ---------------- §1.1 Flask 实例 ----------------
app = Flask(__name__)
CORS(app)
Compress(app)          # Gzip 压缩（application/json, text/csv 等）

# ---------------- §1.2 全局配置 ----------------
# 以北京时间作为"每日免费次数"的统一基准，不依赖服务器系统时区
APP_TZ = timezone(timedelta(hours=8))

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))     # LocalServer
PARENT_DIR = os.path.dirname(CURRENT_DIR)                     # /root 或 /Users/yanzhang/Coding
BASE_RESOURCES_DIR = os.path.join(CURRENT_DIR, 'Resources')
USER_DB_PATH = os.path.join(PARENT_DIR, 'user_data.db')
ANALYTICS_DB_PATH = os.path.join(PARENT_DIR, 'analytics.db')

ALLOWED_APPS = ['ONews', 'Finance', 'Prediction', 'OVideo']

# 订阅：客户端上报的到期时间允许的最大跨度（天）。超过即视为伪造/后门时间。
MAX_SUBSCRIPTION_HORIZON_DAYS = 400
# 永久 VIP 的对外哨兵值（只由服务器根据 is_permanent 生成，绝不接受客户端写入）
PERMANENT_SENTINEL = "2099-12-31T23:59:59Z"
# 兼容老客户端的 {"days": 30} 充值方式（False = 关闭白嫖漏洞）
ALLOW_LEGACY_DAYS_GRANT = False

# 永久 VIP 邀请码  格式: "邀请码": "备注"   ⚠️ 请改掉
VALID_INVITE_CODES = {
    "ONEWS_FAMILY_2024": "Family Access",
    "VIP_FRIEND_888": "Friend Access",
    "DEV_TEST_KEY": "Developer Key"
}

# 视频模块黑名单：这些用户即使是永久 VIP 也看不到视频模块（auth/status 也会下发）
VIDEO_MODULE_BLOCKED_USERS = {
    "001356.cdec6d350edb4646b0130f9363b6d37e.2149",
}

# 播放/阅读/点击的权限来源
ALLOWED_ACCESS_TYPES = {
    'subscription', 'vip_permanent',
    'points', 'points_bonus', 'points_daily', 'points_mixed',
    'free', 'free_day', 'unlocked_today', 'guest',
    'unknown'
}

# 邀请码字母表：去掉 0/O/1/I/L 等易混字符
INVITE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

# 活跃用户明细/流水仅保留最近 N 天
ANALYTICS_LOG_KEEP_DAYS = 15
_last_unlock_cleanup_date = None

# ⚠️ 推荐用环境变量设置后台密码：export ADMIN_PASSWORD='你的密码'（不设则用下面的默认值）
ADMIN_PASSWORD_HASH = (os.environ.get('ADMIN_PASSWORD_SHA256') or
                       hashlib.sha256(os.environ.get('ADMIN_PASSWORD', "YourStrongPassword123!").encode()).hexdigest())
ADMIN_TOKENS = {}                    # token -> 过期时间戳（内存，重启失效）
ADMIN_TOKEN_TTL = 7 * 86400          # 后台登录 7 天有效
_admin_login_fail = {}               # ip -> [失败次数, 首次失败时间]

_sub_kind_cache = {}                 # (user_id, app_name) -> (kind, ts)
_SUB_KIND_TTL = 60                   # 秒
_vip_perm_cache = {}                 # user_id -> (bool, ts)   任一 App 永久 VIP（视频屏蔽配置用）
_user_exists_cache = {}              # user_id -> ts           只缓存"存在"的结果
_USER_EXISTS_TTL = 300
_CACHE_MAX = 20000                   # 内存缓存条目上限，超过直接清空

# 下载接口禁止外泄的文件（小写比较；macOS 文件系统大小写不敏感）
DOWNLOAD_DENY_BASENAMES = {
    'url_mapping.json', 'blacklist_url.json', 'blocked_channels.json', 'ovideo.db',
}
DOWNLOAD_DENY_SUFFIXES = ('-wal', '-shm', '-journal', '.building')


# ---------------- §1.3 通用工具 ----------------
def utcnow():
    """带时区的当前 UTC 时间"""
    return datetime.now(timezone.utc)


def today_str():
    """统一的"自然日"字符串，永远按北京时间 00:00 切分"""
    return datetime.now(APP_TZ).strftime('%Y-%m-%d')


def now_iso():
    """北京时间、不带 +08:00 后缀（naive），保证 SQLite date(created_at) 得到北京自然日"""
    return datetime.now(APP_TZ).replace(tzinfo=None).isoformat()


# fromisoformat() 是 3.7+；strptime 的 %z 在 3.6 不支持 "+08:00"，所以手写解析
_TZ_SUFFIX_RE = re.compile(r'([+-])(\d{2}):?(\d{2})\s*$')
_FRACTION_RE = re.compile(r'\.(\d+)$')


def parse_expiry(value):
    """任意常见格式时间 -> aware UTC datetime；失败返回 None（绝不抛异常）。
       支持：...Z / +00:00 / +0800 / 无时区(按UTC) / 空格分隔 / 微秒 / 仅日期 / datetime 对象 / 2026/10/01"""
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        s = str(value).strip()
        if not s:
            return None

        tz = None
        if s[-1] in ('Z', 'z'):
            s = s[:-1].strip()
            tz = timezone.utc
        else:
            m = _TZ_SUFFIX_RE.search(s)
            # 防止把 "2026-09-22" 的 "-22" 误判成时区
            if m and ('T' in s or ' ' in s):
                sign = 1 if m.group(1) == '+' else -1
                offset = timedelta(hours=int(m.group(2)), minutes=int(m.group(3)))
                tz = timezone(sign * offset)
                s = s[:m.start()].strip()
        if tz is None:
            tz = timezone.utc

        if 'T' not in s and ' ' in s:
            s = s.replace(' ', 'T', 1)

        fm = _FRACTION_RE.search(s)
        if fm and len(fm.group(1)) > 6:
            s = s[:fm.start()] + '.' + fm.group(1)[:6]

        dt = None
        s2 = s.replace('/', '-')
        for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M',
                    '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
            for cand in (s, s2):
                try:
                    dt = datetime.strptime(cand, fmt)
                    break
                except ValueError:
                    continue
            if dt is not None:
                break
        if dt is None:
            return None
        return dt.replace(tzinfo=tz).astimezone(timezone.utc)
    except Exception as e:
        print("[parse_expiry][ERROR] %r -> %s" % (value, e))
        return None


def iso_utc(dt):
    """统一对外/入库格式：2026-09-10T06:21:00Z"""
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _row_get(row, key, default=None):
    """sqlite3.Row 安全取值：列不存在时返回默认值"""
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _json_body():
    """安全读取 JSON 请求体：空体 / Content-Type 不对 时返回 {} 而不是 400/415"""
    return request.get_json(silent=True) or {}


def _arg_int(name, default, lo=None, hi=None):
    """安全读取整数 query 参数，带上下限"""
    try:
        v = int(request.args.get(name, default))
    except (TypeError, ValueError):
        v = default
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def _too_frequent(store, key, interval):
    """内存软限流：interval 秒内重复调用返回 True；字典过大时自动清理旧条目"""
    now = time.time()
    if now - store.get(key, 0) < interval:
        return True
    store[key] = now
    if len(store) > 5000:
        cutoff = now - 600
        for k, t in list(store.items()):
            if t < cutoff:
                store.pop(k, None)
    return False

def _cache_put(store, key, value):
    if len(store) > _CACHE_MAX:
        store.clear()
    store[key] = value


_json_file_cache = {}                # path -> (mtime, data)
_json_file_lock = threading.Lock()


def _load_json_cached(path):
    """按 mtime 缓存的 JSON 读取；文件不存在返回 None；读取失败时沿用旧缓存。
       ⚠️ 返回的是共享对象，调用方不要修改它（要改请先 dict() 复制）。"""
    try:
        m = os.path.getmtime(path)
    except OSError:
        return None
    hit = _json_file_cache.get(path)
    if hit and hit[0] == m:
        return hit[1]
    with _json_file_lock:
        hit = _json_file_cache.get(path)
        if hit and hit[0] == m:
            return hit[1]
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"[json-cache] 读取 {path} 失败: {e}")
            return hit[1] if hit else None
        _json_file_cache[path] = (m, data)
        return data


def _load_version_json(app_name):
    """Resources/<app>/version.json（缓存）；不存在或格式不对返回 {}"""
    data = _load_json_cached(os.path.join(BASE_RESOURCES_DIR, app_name, 'version.json'))
    return data if isinstance(data, dict) else {}


def _user_exists(uid):
    """Apple 登录用户是否存在于 users 表（命中缓存 5 分钟）；数据库异常会抛出"""
    if not is_real_login_user(uid):
        return False
    now = time.time()
    hit = _user_exists_cache.get(uid)
    if hit and now - hit < _USER_EXISTS_TTL:
        return True
    conn = sqlite3.connect(USER_DB_PATH, timeout=10.0)
    try:
        exists = conn.execute("SELECT 1 FROM users WHERE apple_user_id=?", (uid,)).fetchone() is not None
    finally:
        conn.close()
    if exists:
        _cache_put(_user_exists_cache, uid, now)
    return exists


def _is_admin_request():
    token = request.headers.get('X-Admin-Token') or request.args.get('token')
    if not token:
        return False
    exp = ADMIN_TOKENS.get(token)
    if not exp:
        return False
    if exp < time.time():
        ADMIN_TOKENS.pop(token, None)
        return False
    return True


def require_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _is_admin_request():
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

def is_real_login_user(user_id):
    """只有 Apple 登录用户才享受点数。dev_ 开头是设备标识，guest_user 是兜底。"""
    return bool(user_id) and not user_id.startswith('dev_') and user_id != 'guest_user'


def _query_analytics(sql, params=()):
    """analytics.db 只读查询，撞锁时最多重试 3 次"""
    last_err = None
    for _ in range(3):
        conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except sqlite3.OperationalError as e:
            last_err = e
            time.sleep(0.2)
        finally:
            conn.close()
    raise last_err


def require_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = request.headers.get('X-Admin-Token') or request.args.get('token')
        if not token or token not in ADMIN_TOKENS:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper


# ---------------- §1.4 订阅身份（埋点/权限共用） ----------------
def get_subscription_kind(user_id, app_name='ONews'):
    """服务器权威判断用户在【该 App】的付费身份：'vip_permanent' / 'subscription' / None
       【修复】① 缓存按 (user_id, app) 区分，不再串 App；
              ② 判定规则与 check_user_subscription_status 一致（只看本 App 的列），
                 和客户端实际拿到的订阅状态一致，埋点不再把"实际扣点"记成 VIP。"""
    if not is_real_login_user(user_id):
        return None
    key = (user_id, app_name)
    hit = _sub_kind_cache.get(key)
    if hit and (time.time() - hit[1]) < _SUB_KIND_TTL:
        return hit[0]
    kind = None
    try:
        conn = sqlite3.connect(USER_DB_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT * FROM users WHERE apple_user_id=?", (user_id,)).fetchone()
        finally:
            conn.close()
        if row:
            ok, exp = check_user_subscription_status(row, app_name)
            if ok:
                kind = 'vip_permanent' if exp == PERMANENT_SENTINEL else 'subscription'
    except Exception as e:
        print(f"[access] 查询订阅身份失败: {e}")
    _cache_put(_sub_kind_cache, key, (kind, time.time()))
    return kind


def _invalidate_sub_cache(user_id):
    if not user_id:
        return
    for a in ALLOWED_APPS:
        _sub_kind_cache.pop((user_id, a), None)
    _vip_perm_cache.pop(user_id, None)
    _user_exists_cache.pop(user_id, None)


def check_user_subscription_status(user_row, app_name):
    """返回 (is_subscribed, expires_at_str)；优先级：is_permanent > expire_at。保证不抛异常。"""
    if user_row is None:
        return False, None
    prefix = app_name.lower()
    perm_col = f"{prefix}_is_permanent"
    expire_col = f"{prefix}_expire_at"
    try:
        if _row_get(user_row, perm_col, 0) in (1, '1', True):
            return True, PERMANENT_SENTINEL
        raw = _row_get(user_row, expire_col)
        if raw:
            dt = parse_expiry(raw)
            if dt is None:
                print(f"[subscription][WARN] 无法解析 {expire_col} = {raw!r}，视为未订阅")
                return False, None
            normalized = iso_utc(dt)
            return (dt > utcnow()), normalized
    except Exception as e:
        traceback.print_exc()
        print(f"[subscription][ERROR] 判定订阅状态异常: {e}")
        return False, None
    return False, None


# ---------------- §1.5 流水清理（三模块流水表） ----------------
def analytics_cutoff_iso(days=ANALYTICS_LOG_KEEP_DAYS):
    """北京时间 N 天前的 naive ISO 字符串，用于与 created_at 比较"""
    return (datetime.now(APP_TZ) - timedelta(days=days)).replace(tzinfo=None).isoformat()


def cleanup_old_event_logs(days_to_keep=ANALYTICS_LOG_KEEP_DAYS):
    """删除 N 天前的三张流水表记录"""
    cutoff = analytics_cutoff_iso(days_to_keep)
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    try:
        c = conn.cursor()
        total = 0
        for table in ('event_logs', 'news_event_logs', 'finance_event_logs'):
            c.execute(f"DELETE FROM {table} WHERE created_at < ?", (cutoff,))
            total += c.rowcount
        conn.commit()
        if total:
            print(f"[cleanup] 已清理 {total} 条过期流水 (< {cutoff})")
    except Exception as e:
        print(f"[cleanup] 流水清理失败: {e}")
    finally:
        conn.close()


def cleanup_old_unlocks(days_to_keep=7):
    """删除 days_to_keep 天前的视频解锁记录"""
    cutoff = (datetime.now(APP_TZ) - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    try:
        c = conn.cursor()
        c.execute("DELETE FROM video_free_unlocks WHERE unlock_date < ?", (cutoff,))
        deleted = c.rowcount
        conn.commit()
        if deleted:
            print(f"[cleanup] 已清理 {deleted} 条过期解锁记录 (< {cutoff})")
    except Exception as e:
        print(f"[cleanup] 清理失败: {e}")
    finally:
        conn.close()


_cleanup_lock = threading.Lock()


def compact_sqlite(path, force_vacuum=False, min_free_ratio=0.25, min_free_pages=2560):
    """回收磁盘：DELETE 不会让 SQLite 文件变小。
       空闲页 ≥25% 且 ≥约10MB 时 VACUUM；每次都把 -wal 文件截断回 0。"""
    if not os.path.exists(path):
        return
    try:
        conn = sqlite3.connect(path, timeout=60.0, isolation_level=None)
        try:
            pages = conn.execute("PRAGMA page_count").fetchone()[0] or 0
            free = conn.execute("PRAGMA freelist_count").fetchone()[0] or 0
            before = os.path.getsize(path)
            vacuumed = False
            if force_vacuum or (pages and free >= min_free_pages and float(free) / pages >= min_free_ratio):
                conn.execute("VACUUM")
                vacuumed = True
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("PRAGMA optimize")
            if vacuumed:
                print(f"[maintenance] VACUUM {os.path.basename(path)}: "
                      f"{before // 1024} KB -> {os.path.getsize(path) // 1024} KB")
        finally:
            conn.close()
    except Exception as e:
        print(f"[maintenance] 压缩 {path} 失败: {e}")


def _daily_maintenance():
    try:
        cleanup_old_unlocks(days_to_keep=7)
        cleanup_old_event_logs(days_to_keep=ANALYTICS_LOG_KEEP_DAYS)
        compact_sqlite(ANALYTICS_DB_PATH)
        compact_sqlite(USER_DB_PATH)
    except Exception:
        traceback.print_exc()


def maybe_cleanup_old_unlocks():
    """每个北京自然日最多执行一次；放到后台线程跑，不拖慢当前请求"""
    global _last_unlock_cleanup_date
    today = today_str()
    if _last_unlock_cleanup_date == today:
        return
    with _cleanup_lock:
        if _last_unlock_cleanup_date == today:
            return
        _last_unlock_cleanup_date = today
    threading.Thread(target=_daily_maintenance, name='daily-maintenance', daemon=True).start()


# ---------------- §1.6 公共数据库初始化 ----------------
def init_user_db():
    """user_data.db：users 表（Finance 的点数表在 §5 init_finance_user_db）"""
    print(f"检查用户数据库: {USER_DB_PATH}")
    os.makedirs(os.path.dirname(USER_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apple_user_id TEXT NOT NULL UNIQUE,
            device_id TEXT,
            created_at TIMESTAMP NOT NULL,
            last_login_at TIMESTAMP,
            finance_expire_at TIMESTAMP,
            finance_is_permanent INTEGER DEFAULT 0,
            onews_expire_at TIMESTAMP,
            onews_is_permanent INTEGER DEFAULT 0,
            prediction_expire_at TIMESTAMP,
            prediction_is_permanent INTEGER DEFAULT 0
        )
    ''')
    for ddl in ('ALTER TABLE users ADD COLUMN device_id TEXT',
                'ALTER TABLE users ADD COLUMN prediction_expire_at TIMESTAMP',
                'ALTER TABLE users ADD COLUMN prediction_is_permanent INTEGER DEFAULT 0'):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()
    print("用户数据库已准备就绪。")


def init_anonymous_sub_db():
    print("检查匿名（免登录）订阅表 ...")
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS anonymous_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app TEXT NOT NULL DEFAULT 'ONews',
            original_transaction_id TEXT NOT NULL,
            device_id TEXT,
            transaction_id TEXT,
            product_id TEXT,
            environment TEXT,
            purchase_date TIMESTAMP,
            expire_at TIMESTAMP,
            app_version TEXT,
            report_count INTEGER DEFAULT 1,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            UNIQUE(app, original_transaction_id)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_anon_dev ON anonymous_subscriptions(device_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_anon_exp ON anonymous_subscriptions(expire_at)')
    conn.commit()
    conn.close()
    print("匿名订阅表已就绪。")


def init_analytics_base():
    """analytics.db 开启 WAL（各模块的表在各自节里建）"""
    print(f"检查行为数据库: {ANALYTICS_DB_PATH}")
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.commit()
    conn.close()


# ---------------- §1.7 登录 / 状态 / 兑换 / 付费 / 删号 ----------------
def handle_auth(app_name):
    try:
        data = _json_body()
        user_id = data.get('user_id')
        device_id = data.get('device_id')
        if not user_id:
            return jsonify({"error": "Missing user_id"}), 400
        conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        user = c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,)).fetchone()
        now = utcnow().replace(tzinfo=None)   # 存 naive UTC，兼容老数据
        is_subscribed, expiration_date = False, None
        if user:
            c.execute("UPDATE users SET last_login_at = ?, device_id = ? WHERE apple_user_id = ?",
                      (now, device_id, user_id))
            is_subscribed, expiration_date = check_user_subscription_status(user, app_name)
        else:
            c.execute("INSERT INTO users (apple_user_id, device_id, created_at, last_login_at) VALUES (?, ?, ?, ?)",
                      (user_id, device_id, now, now))
        conn.commit()
        conn.close()
        return jsonify({
            "status": "success",
            "is_subscribed": is_subscribed,
            "subscription_expires_at": expiration_date,
            "video_module_blocked": user_id in VIDEO_MODULE_BLOCKED_USERS
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def handle_status_check(app_name):
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    debug = request.args.get('debug') in ('1', 'true', 'yes') and _is_admin_request()   # 【修复】仅管理员可看调试信息
    conn = None
    try:
        conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,)).fetchone()
        is_subscribed, expires_at_str = check_user_subscription_status(row, app_name)
        payload = {
            "is_subscribed": is_subscribed,
            "subscription_expires_at": expires_at_str,
            "video_module_blocked": user_id in VIDEO_MODULE_BLOCKED_USERS
        }
        if debug:
            prefix = app_name.lower()
            raw = _row_get(row, f"{prefix}_expire_at") if row else None
            parsed = parse_expiry(raw)
            payload["_debug"] = {
                "db_path": USER_DB_PATH,
                "row_found": bool(row),
                "row_id": _row_get(row, 'id') if row else None,
                "checked_columns": [f"{prefix}_expire_at", f"{prefix}_is_permanent"],
                "raw_expire_at": str(raw),
                "raw_type": type(raw).__name__,
                "parsed_expire_at_utc": iso_utc(parsed) if parsed else None,
                "is_permanent_raw": _row_get(row, f"{prefix}_is_permanent", None) if row else None,
                "server_now_utc": iso_utc(utcnow()),
                "python_version": sys.version,
                "has_fromisoformat": hasattr(datetime, 'fromisoformat'),
            }
        print(f"[status] {app_name} user={user_id[:14]}... -> sub={is_subscribed} exp={expires_at_str}")
        resp = jsonify(payload)
        resp.headers['Cache-Control'] = 'no-store, no-cache, max-age=0'
        return resp
    except Exception as e:
        # 出错也返回结构合法的 JSON，避免客户端误判成"服务器不可达"
        traceback.print_exc()
        resp = jsonify({
            "is_subscribed": False,
            "subscription_expires_at": None,
            "video_module_blocked": user_id in VIDEO_MODULE_BLOCKED_USERS,
            "_error": str(e)
        })
        resp.headers['Cache-Control'] = 'no-store, no-cache, max-age=0'
        return resp
    finally:
        if conn is not None:
            conn.close()


def handle_redeem_invite(app_name):
    """永久 VIP 邀请码兑换（固定码，见 VALID_INVITE_CODES）"""
    data = _json_body()
    user_id = data.get('user_id')
    invite_code = data.get('invite_code')
    if not user_id or not invite_code:
        return jsonify({"error": "缺少参数"}), 400
    if invite_code not in VALID_INVITE_CODES:
        return jsonify({"error": "无效的邀请码"}), 403
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    try:
        perm_col = f"{app_name.lower()}_is_permanent"
        c.execute(f"UPDATE users SET {perm_col} = 1 WHERE apple_user_id = ?", (user_id,))
        if c.rowcount == 0:
            return jsonify({"error": "用户不存在，请先登录"}), 404
        conn.commit()
        _invalidate_sub_cache(user_id)
        print(f"[{app_name}] 用户 {user_id} 使用邀请码 {invite_code} 升级为永久 VIP")
        return jsonify({"status": "success", "is_subscribed": True,
                        "subscription_expires_at": PERMANENT_SENTINEL})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


def handle_payment(app_name):
    """⚠️ 目前信任客户端上报的到期时间（未做 Apple 票据校验）"""
    data = _json_body()
    user_id = data.get('user_id')
    explicit_expiry = data.get('explicit_expiry')
    days = data.get('days')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    prefix = app_name.lower()
    expire_col = f"{prefix}_expire_at"
    perm_col = f"{prefix}_is_permanent"

    # 1. 计算目标时间
    if explicit_expiry:
        new_dt = parse_expiry(explicit_expiry)
        if new_dt is None:
            return jsonify({"error": f"invalid explicit_expiry: {explicit_expiry}"}), 400
        # 拒绝 2099 之类的后门时间，否则 is_permanent 改回 0 也降不了权
        if new_dt > utcnow() + timedelta(days=MAX_SUBSCRIPTION_HORIZON_DAYS):
            print(f"[payment][REJECT] {user_id} 上报了超范围到期时间 {explicit_expiry}，已拒绝")
            return jsonify({"error": "expiry out of allowed range"}), 400
    elif days is not None:
        if not ALLOW_LEGACY_DAYS_GRANT:
            return jsonify({"error": "explicit_expiry required"}), 400
        try:
            d = max(1, min(int(days), MAX_SUBSCRIPTION_HORIZON_DAYS))
        except Exception:
            return jsonify({"error": "invalid days"}), 400
        new_dt = utcnow() + timedelta(days=d)
        print(f"[payment][LEGACY] {user_id} 走旧的 days={d} 充值路径")
    else:
        return jsonify({"error": "missing explicit_expiry"}), 400

    # 2. 写库（只允许延长，不允许缩短）
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        row = c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,)).fetchone()
        if not row:
            return jsonify({"error": "User not found"}), 404
        if _row_get(row, perm_col, 0) in (1, '1', True):
            return jsonify({"status": "success", "is_subscribed": True,
                            "subscription_expires_at": PERMANENT_SENTINEL})
        cur_dt = parse_expiry(_row_get(row, expire_col))
        final_dt = new_dt if (cur_dt is None or new_dt > cur_dt) else cur_dt
        final_str = iso_utc(final_dt)
        c.execute(f"UPDATE users SET {expire_col} = ? WHERE apple_user_id = ?", (final_str, user_id))
        conn.commit()
        _invalidate_sub_cache(user_id)
        print(f"[payment] {app_name} 用户 {user_id} 到期时间 -> {final_str}")
        return jsonify({"status": "success", "is_subscribed": final_dt > utcnow(),
                        "subscription_expires_at": final_str})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/<app_name>/user/delete', methods=['POST'])
def delete_user(app_name):
    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    data = _json_body()
    user_id = data.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM users WHERE apple_user_id = ?", (user_id,))
        if c.rowcount == 0:
            return jsonify({"error": "User not found"}), 404
        conn.commit()
        _invalidate_sub_cache(user_id)
        print(f"[{app_name}] 用户 {user_id} 已成功删除账号。")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"删除账号失败: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---- 各 App 的登录/订阅路由绑定（OVideo 走 ONews 的账号体系） ----
@app.route('/api/ONews/auth/apple', methods=['POST'])
def onews_auth(): return handle_auth('ONews')

@app.route('/api/ONews/payment/subscribe', methods=['POST'])
def onews_pay(): return handle_payment('ONews')

@app.route('/api/ONews/user/status', methods=['GET'])
def onews_status(): return handle_status_check('ONews')

@app.route('/api/ONews/user/redeem', methods=['POST'])
def onews_redeem(): return handle_redeem_invite('ONews')

@app.route('/api/Finance/auth/apple', methods=['POST'])
def finance_auth(): return handle_auth('Finance')

@app.route('/api/Finance/payment/subscribe', methods=['POST'])
def finance_pay(): return handle_payment('Finance')

@app.route('/api/Finance/user/status', methods=['GET'])
def finance_status(): return handle_status_check('Finance')

@app.route('/api/Finance/user/redeem', methods=['POST'])
def finance_redeem(): return handle_redeem_invite('Finance')

# Prediction（暂未使用，仅保留路由）
@app.route('/api/Prediction/auth/apple', methods=['POST'])
def prediction_auth(): return handle_auth('Prediction')

@app.route('/api/Prediction/payment/subscribe', methods=['POST'])
def prediction_pay(): return handle_payment('Prediction')

@app.route('/api/Prediction/user/status', methods=['GET'])
def prediction_status(): return handle_status_check('Prediction')

@app.route('/api/Prediction/user/redeem', methods=['POST'])
def prediction_redeem(): return handle_redeem_invite('Prediction')

@app.route('/api/Prediction/user/delete', methods=['POST'])
def prediction_delete(): return delete_user('Prediction')


# ---------------- §1.8 免登录订阅（匿名） ----------------
@app.route('/api/<app_name>/payment/anonymous_subscribe', methods=['POST'])
def anonymous_subscribe(app_name):
    """免登录订阅记录。唯一键是 original_transaction_id。"""
    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    data = _json_body()
    otid = (data.get('original_transaction_id') or '').strip()
    device_id = (data.get('device_id') or '').strip()
    expiry_raw = data.get('expiry')
    if not otid or not expiry_raw:
        return jsonify({"error": "missing original_transaction_id / expiry"}), 400
    exp = parse_expiry(expiry_raw)
    if exp is None:
        return jsonify({"error": "invalid expiry"}), 400
    if exp > utcnow() + timedelta(days=MAX_SUBSCRIPTION_HORIZON_DAYS):
        return jsonify({"error": "expiry out of allowed range"}), 400

    purchase = parse_expiry(data.get('purchase_date'))
    now = utcnow().replace(tzinfo=None)
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO anonymous_subscriptions
                (app, original_transaction_id, device_id, transaction_id, product_id,
                 environment, purchase_date, expire_at, app_version,
                 report_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(app, original_transaction_id) DO UPDATE SET
                device_id = excluded.device_id,
                transaction_id = excluded.transaction_id,
                product_id = excluded.product_id,
                environment = excluded.environment,
                app_version = excluded.app_version,
                expire_at = CASE
                    WHEN excluded.expire_at > anonymous_subscriptions.expire_at
                    THEN excluded.expire_at ELSE anonymous_subscriptions.expire_at END,
                report_count = anonymous_subscriptions.report_count + 1,
                updated_at = excluded.updated_at
        ''', (app_name, otid, device_id, data.get('transaction_id'), data.get('product_id'),
              data.get('environment'), iso_utc(purchase) if purchase else None,
              iso_utc(exp), data.get('app_version'), now, now))
        conn.commit()
        print(f"[anon-sub] {app_name} otid={otid[-8:]} dev={device_id[-8:]} exp={iso_utc(exp)}")
        return jsonify({"status": "success", "is_subscribed": exp > utcnow(),
                        "expire_at": iso_utc(exp)}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/<app_name>/payment/anonymous_status', methods=['GET'])
def anonymous_status(app_name):
    """辅助查询（客户端仅参考，最终以 Apple 本地凭证为准）"""
    device_id = request.args.get('device_id', '')
    otid = request.args.get('original_transaction_id', '')
    if not device_id and not otid:
        return jsonify({"error": "missing device_id"}), 400
    conn = sqlite3.connect(USER_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        if otid:
            row = conn.execute("""SELECT * FROM anonymous_subscriptions
                                  WHERE app=? AND original_transaction_id=?""",
                               (app_name, otid)).fetchone()
        else:
            row = conn.execute("""SELECT * FROM anonymous_subscriptions
                                  WHERE app=? AND device_id=?
                                  ORDER BY expire_at DESC LIMIT 1""",
                               (app_name, device_id)).fetchone()
        if not row:
            return jsonify({"is_subscribed": False, "expire_at": None})
        exp = parse_expiry(row['expire_at'])
        return jsonify({"is_subscribed": bool(exp and exp > utcnow()),
                        "expire_at": iso_utc(exp) if exp else None})
    finally:
        conn.close()


# ---------------- §1.9 版本检查 / 文件下载 ----------------
@app.route('/api/<app_name>/check_version', methods=['GET'])
def check_version(app_name):
    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    cached = _load_json_cached(os.path.join(BASE_RESOURCES_DIR, app_name, 'version.json'))
    if cached is None:
        return jsonify({"error": "Version file not found"}), 404
    data = dict(cached)                      # 复制一份，不能改共享缓存
    data['server_date'] = datetime.now(APP_TZ).strftime('%y%m%d')
    data['is_free_access_day'] = is_free_access_day()   # 定义在 §5
    return jsonify(data)


@app.route('/api/<app_name>/download', methods=['GET'])
def download_file(app_name):
    filename = request.args.get('filename')
    print(f"收到来自应用 '{app_name}' 的文件下载请求: {filename}")
    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    if not filename:
        return jsonify({"error": "缺少文件名参数"}), 400

    try:
        full_path = safe_join(BASE_RESOURCES_DIR, app_name, filename)
    except Exception:
        full_path = None
    if not full_path:
        print(f"错误: 请求的路径不安全: {filename}")
        return jsonify({"error": "无效的路径"}), 400

    # 【修复】按"规范化后的真实文件名"判断：'./Finance.db'、'Finance.db/'、大小写变体都绕不过
    base_lower = os.path.basename(full_path).lower()
    if base_lower in DOWNLOAD_DENY_BASENAMES or base_lower.endswith(DOWNLOAD_DENY_SUFFIXES):
        return jsonify({"error": "文件未找到"}), 404

    if app_name == 'Finance' and base_lower == 'finance.db':
        uid = request.args.get('user_id', '')
        if not is_real_login_user(uid):
            return jsonify({"error": "请先登录后再下载离线数据库"}), 401
        try:
            if not _user_exists(uid):
                return jsonify({"error": "用户不存在"}), 401
        except Exception:
            return jsonify({"error": "auth_check_failed"}), 500

    if not os.path.isfile(full_path):
        print(f"错误: 请求的文件不存在: {full_path}")
        return jsonify({"error": "文件未找到"}), 404
    try:
        directory, file = os.path.split(full_path)
        return send_from_directory(directory, file, as_attachment=True)
    except Exception as e:
        print(f"发生错误: {e}")
        return jsonify({"error": str(e)}), 500


# ---------------- §1.10 后台：登录 / 订阅管理 / 清库 ----------------
@app.route('/admin/login', methods=['POST'])
def admin_login():
    ip = request.headers.get('X-Forwarded-For', request.remote_addr or '').split(',')[0].strip()
    rec = _admin_login_fail.get(ip)
    if rec and rec[0] >= 10 and time.time() - rec[1] < 600:
        return jsonify({"error": "尝试次数过多，请 10 分钟后再试"}), 429
    pwd = _json_body().get('password', '') or ''
    if hmac.compare_digest(hashlib.sha256(pwd.encode()).hexdigest(), ADMIN_PASSWORD_HASH):
        _admin_login_fail.pop(ip, None)
        now = time.time()
        if len(ADMIN_TOKENS) > 200:
            for t, e in list(ADMIN_TOKENS.items()):
                if e < now:
                    ADMIN_TOKENS.pop(t, None)
            if len(ADMIN_TOKENS) > 200:
                ADMIN_TOKENS.clear()
        token = secrets.token_urlsafe(32)
        ADMIN_TOKENS[token] = now + ADMIN_TOKEN_TTL
        return jsonify({"token": token})
    if not rec or time.time() - rec[1] >= 600:
        _admin_login_fail[ip] = [1, time.time()]
    else:
        rec[0] += 1
    if len(_admin_login_fail) > 5000:
        _admin_login_fail.clear()
    return jsonify({"error": "密码错误"}), 401


@app.route('/admin/api/set_subscription', methods=['POST'])
@require_admin
def admin_set_subscription():
    """后台直接授予 / 撤销某个 App 的订阅"""
    data = _json_body()
    user_id = (data.get('user_id') or '').strip()
    app_name = (data.get('app') or 'Finance').strip()
    days = data.get('days')                  # 例如 30；负数/0 = 立即到期
    permanent = data.get('permanent')        # True/False
    if not user_id or app_name not in ALLOWED_APPS or app_name == 'OVideo':
        return jsonify({"error": "invalid params"}), 400
    prefix = app_name.lower()
    expire_col, perm_col = f"{prefix}_expire_at", f"{prefix}_is_permanent"
    conn = sqlite3.connect(USER_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        if not c.execute("SELECT 1 FROM users WHERE apple_user_id=?", (user_id,)).fetchone():
            return jsonify({"error": "user not found"}), 404
        if permanent is not None:
            c.execute(f"UPDATE users SET {perm_col}=? WHERE apple_user_id=?",
                      (1 if permanent else 0, user_id))
        if days is not None:
            try:
                d = int(days)
            except Exception:
                return jsonify({"error": "invalid days"}), 400
            if d <= 0:
                new_str = iso_utc(utcnow() - timedelta(minutes=1))
            else:
                new_str = iso_utc(utcnow() + timedelta(days=min(d, MAX_SUBSCRIPTION_HORIZON_DAYS)))
            c.execute(f"UPDATE users SET {expire_col}=? WHERE apple_user_id=?", (new_str, user_id))
        conn.commit()
        row = c.execute("SELECT * FROM users WHERE apple_user_id=?", (user_id,)).fetchone()
        ok, exp = check_user_subscription_status(row, app_name)
        _invalidate_sub_cache(user_id)
        return jsonify({"status": "success", "is_subscribed": ok, "subscription_expires_at": exp})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/admin/anonymous_subs', methods=['GET'])
@require_admin
def admin_anonymous_subs():
    """后台统计：免登录付费人数 / 当前有效数"""
    conn = sqlite3.connect(USER_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM anonymous_subscriptions ORDER BY updated_at DESC LIMIT 500")]
        total = conn.execute(
            "SELECT COUNT(DISTINCT original_transaction_id) FROM anonymous_subscriptions").fetchone()[0]
        now = utcnow()
        active = sum(1 for r in rows if (parse_expiry(r['expire_at']) or now) > now)
        return jsonify({"total_paid_users": total, "active_in_page": active, "items": rows})
    finally:
        conn.close()


@app.route('/admin/api/clear_db', methods=['POST'])
@require_admin
def admin_clear_db():
    clear_type = _json_body().get('type')   # 'analytics' / 'users' / 'all'
    if clear_type not in ['analytics', 'users', 'all']:
        return jsonify({"error": "无效的清除类型"}), 400
    analytics_tables = [
        'user_video_events', 'event_logs', 'user_news_events', 'news_event_logs',
        'video_link_reports', 'video_wish_requests', 'finance_invite_logs',
        'user_finance_events', 'finance_event_logs', 'onews_points', 'news_points',
        'video_points', 'news_free_unlocks', 'onews_invite_logs', 'video_bonus_quota',
        'video_free_unlocks', 'support_messages', 'support_threads', 'support_meta',
    ]
    # 按钮写的是"用户及订阅数据"，所以把免登录订阅也一起清；不想清就把最后一项删掉
    user_tables = ['users', 'finance_points', 'finance_daily_unlocks', 'anonymous_subscriptions']
    try:
        jobs = []
        if clear_type in ('analytics', 'all'):
            jobs.append((ANALYTICS_DB_PATH, analytics_tables))
        if clear_type in ('users', 'all'):
            jobs.append((USER_DB_PATH, user_tables))
        for path, tables in jobs:
            conn = sqlite3.connect(path, timeout=30.0)
            c = conn.cursor()
            for t in tables:
                try:
                    c.execute(f"DELETE FROM {t}")
                except sqlite3.OperationalError as e:
                    print(f"[clear_db] 跳过 {t}: {e}")
            conn.commit()
            conn.close()
            compact_sqlite(path, force_vacuum=True)      # 立即把磁盘空间还回来
        for store in (_sub_kind_cache, _vip_perm_cache, _user_exists_cache):
            store.clear()
        return jsonify({"status": "success", "message": f"成功清空了 {clear_type} 相关的数据。"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §2A ONews + OVideo 共用：点数账本 / 邀请码                             ║
# ║      表：onews_points(旧) / news_points / video_points / onews_invite_logs ║
# ║      依赖：§1；配置函数 get_news_points_config(§4) / get_video_points_config(§3) ║
# ╚══════════════════════════════════════════════════════════════════════╝

def init_points_db():
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    for tname in ('onews_points', 'news_points', 'video_points'):
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {tname} (
                user_id TEXT PRIMARY KEY,
                invite_code TEXT UNIQUE,
                bonus_remaining INTEGER DEFAULT 0,
                bonus_total INTEGER DEFAULT 0,
                invited_by_code TEXT,
                invite_reward_count INTEGER DEFAULT 0,
                first_login_bonus_granted INTEGER DEFAULT 0,
                created_at TIMESTAMP
            )
        ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS onews_invite_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inviter_id TEXT NOT NULL,
            inviter_code TEXT,
            invitee_id TEXT NOT NULL,
            reward_points INTEGER,
            created_at TIMESTAMP NOT NULL
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_onews_invite_inviter ON onews_invite_logs(inviter_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_onews_invite_time ON onews_invite_logs(created_at)')
    conn.commit()
    conn.close()


def _gen_points_code(cursor, table, length=6):
    for _ in range(30):
        code = ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length))
        if not cursor.execute(f"SELECT 1 FROM {table} WHERE invite_code=?", (code,)).fetchone():
            return code
    return ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length + 2))


def _ensure_points(c, table, user_id, cfg, migrate_from=None):
    """确保点数行存在；返回 (row, just_granted)。
       news_points 首次创建时可从旧 onews_points 迁移。
       【修复】INSERT OR IGNORE：status 与 unlock 并发首建时不再 IntegrityError 500"""
    row = c.execute(f"SELECT * FROM {table} WHERE user_id=?", (user_id,)).fetchone()
    just_granted = False
    if row is None:
        code = _gen_points_code(c, table)
        migrated = None
        if migrate_from:
            migrated = c.execute(
                f"SELECT invite_code, bonus_remaining, bonus_total, invited_by_code, invite_reward_count "
                f"FROM {migrate_from} WHERE user_id=?", (user_id,)).fetchone()
        if migrated:
            old_code = migrated['invite_code']
            if old_code and not c.execute(f"SELECT 1 FROM {table} WHERE invite_code=?", (old_code,)).fetchone():
                code = old_code
            c.execute(f'''INSERT OR IGNORE INTO {table}
                (user_id, invite_code, bonus_remaining, bonus_total, invited_by_code,
                 invite_reward_count, first_login_bonus_granted, created_at)
                VALUES (?,?,?,?,?,?,1,?)''',
                (user_id, code, migrated['bonus_remaining'], migrated['bonus_total'],
                 migrated['invited_by_code'], migrated['invite_reward_count'], now_iso()))
        else:
            bonus = cfg['first_login_bonus']
            c.execute(f'''INSERT OR IGNORE INTO {table}
                (user_id, invite_code, bonus_remaining, bonus_total, invited_by_code,
                 invite_reward_count, first_login_bonus_granted, created_at)
                VALUES (?,?,?,?,NULL,0,?,?)''',
                (user_id, code, bonus, bonus, 1 if bonus > 0 else 0, now_iso()))
            just_granted = bonus > 0 and c.rowcount == 1
        row = c.execute(f"SELECT * FROM {table} WHERE user_id=?", (user_id,)).fetchone()
    return row, just_granted


def _grant_points_bonus(c, table, user_id, points, cfg):
    migrate = 'onews_points' if table == 'news_points' else None
    _ensure_points(c, table, user_id, cfg, migrate_from=migrate)
    c.execute(f"UPDATE {table} SET bonus_remaining=bonus_remaining+?, bonus_total=bonus_total+? WHERE user_id=?",
              (points, points, user_id))


def _news_daily_used(c, user_id, today):
    return c.execute('''SELECT COUNT(*) AS n FROM news_free_unlocks
                        WHERE user_id=? AND unlock_date=? AND COALESCE(source,'daily')='daily' ''',
                     (user_id, today)).fetchone()['n']


def _video_daily_used(c, user_id, today):
    return c.execute('''SELECT COUNT(*) AS n FROM video_free_unlocks
                        WHERE user_id=? AND unlock_date=? AND COALESCE(source,'daily')='daily' ''',
                     (user_id, today)).fetchone()['n']


def _log_onews_invite(inviter_id, code, invitee_id, points):
    try:
        conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
        conn.execute('''INSERT INTO onews_invite_logs
            (inviter_id, inviter_code, invitee_id, reward_points, created_at)
            VALUES (?,?,?,?,?)''', (inviter_id, code, invitee_id, points, now_iso()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"记录ONews邀请日志失败: {e}")


def _do_invite_redeem(table, cfg_getter):
    data = _json_body()
    invitee_id = data.get('user_id')
    code = (data.get('invite_code') or '').strip().upper()
    if not is_real_login_user(invitee_id):
        return jsonify({"error": "请先登录后再使用邀请码"}), 401
    if not code:
        return jsonify({"error": "请输入邀请码"}), 400
    cfg = cfg_getter()
    reward = cfg['invite_reward_points']
    migrate = 'onews_points' if table == 'news_points' else None
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    inviter_id = None
    invitee_bonus = 0
    invitee_total = 0
    try:
        c.execute("BEGIN IMMEDIATE")
        invitee_row, _ = _ensure_points(c, table, invitee_id, cfg, migrate_from=migrate)
        if invitee_row['invited_by_code']:
            c.execute("COMMIT")
            return jsonify({"error": "您已经使用过邀请码了，每位用户仅限使用一次"}), 403
        inviter = c.execute(f"SELECT * FROM {table} WHERE invite_code=?", (code,)).fetchone()
        if not inviter:
            c.execute("COMMIT")
            return jsonify({"error": "视频和新闻的邀请码不能混用或您输入了错误的邀请码，请检查后重试"}), 404
        inviter_id = inviter['user_id']
        if inviter_id == invitee_id:
            c.execute("COMMIT")
            return jsonify({"error": "不能使用自己的邀请码哦"}), 400
        if inviter['invited_by_code'] and invitee_row['invite_code'] \
           and inviter['invited_by_code'] == invitee_row['invite_code']:
            c.execute("COMMIT")
            return jsonify({"error": "你们已经互相邀请过啦，每对好友仅能领取一次奖励"}), 403

        _grant_points_bonus(c, table, invitee_id, reward, cfg)
        _grant_points_bonus(c, table, inviter_id, reward, cfg)
        c.execute(f"UPDATE {table} SET invited_by_code=? WHERE user_id=?", (code, invitee_id))
        c.execute(f"UPDATE {table} SET invite_reward_count=invite_reward_count+1 WHERE user_id=?", (inviter_id,))

        invitee_bonus = c.execute(f"SELECT bonus_remaining FROM {table} WHERE user_id=?",
                                  (invitee_id,)).fetchone()['bonus_remaining']
        today = today_str()
        du = _news_daily_used(c, invitee_id, today) if table == 'news_points' \
            else _video_daily_used(c, invitee_id, today)
        invitee_total = invitee_bonus + max(0, cfg['daily_quota'] - du)
        c.execute("COMMIT")
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
    _log_onews_invite(inviter_id, code, invitee_id, reward)
    return jsonify({"status": "success", "reward_points": reward,
                    "bonus_remaining": invitee_bonus, "remaining_total": invitee_total})


@app.route('/api/ONews/invite/redeem', methods=['POST'])
def onews_invite_redeem():
    return _do_invite_redeem('news_points', get_news_points_config)


@app.route('/api/OVideo/invite/redeem', methods=['POST'])
def ovideo_invite_redeem():
    return _do_invite_redeem('video_points', get_video_points_config)


# ---- 后台：ONews/OVideo 邀请统计 ----
@app.route('/admin/api/onews/invite_overview', methods=['GET'])
@require_admin
def admin_onews_invite_overview():
    today = today_str()
    return jsonify({
        "total_invites":   _query_analytics("SELECT COUNT(*) c FROM onews_invite_logs")[0]['c'],
        "today_invites":   _query_analytics("SELECT COUNT(*) c FROM onews_invite_logs WHERE date(created_at)=?", (today,))[0]['c'],
        "unique_inviters": _query_analytics("SELECT COUNT(DISTINCT inviter_id) c FROM onews_invite_logs")[0]['c'],
        "total_reward_points": _query_analytics("SELECT COALESCE(SUM(reward_points),0) c FROM onews_invite_logs")[0]['c'],
    })


@app.route('/admin/api/onews/top_inviters', methods=['GET'])
@require_admin
def admin_onews_top_inviters():
    return jsonify(_query_analytics('''
        SELECT inviter_id, MAX(inviter_code) AS inviter_code,
               COUNT(*) AS invite_count, SUM(reward_points) AS total_points,
               MAX(created_at) AS last_time
        FROM onews_invite_logs GROUP BY inviter_id
        ORDER BY invite_count DESC LIMIT 50
    '''))


@app.route('/admin/api/onews/invite_logs', methods=['GET'])
@require_admin
def admin_onews_invite_logs():
    return jsonify(_query_analytics('''
        SELECT inviter_id, inviter_code, invitee_id, reward_points, created_at
        FROM onews_invite_logs ORDER BY created_at DESC LIMIT 100
    '''))


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §2B 在线客服会话（OVideo 寻片/举报 + ONews/Finance 在线客服）           ║
# ║      表：support_threads / support_messages / support_meta            ║
# ║      会同步老表 video_wish_requests / video_link_reports（§3 建表）     ║
# ╚══════════════════════════════════════════════════════════════════════╝

SUPPORT_APPS = {'ONews', 'Finance'}
support_last_time = {}   # 软限流: user_id -> 最近提交时间戳

REPORT_TYPE_NAMES = {'playback_failed': '无法播放', 'download_failed': '无法缓存',
                     'media_error': '画面/声音异常', 'content_mismatch': '内容不符',
                     'other': '其他问题'}


def init_support_db():
    print("检查客服会话表 ...")
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute('''
        CREATE TABLE IF NOT EXISTS support_threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app TEXT NOT NULL DEFAULT 'ONews',
            thread_key TEXT NOT NULL UNIQUE,
            thread_type TEXT NOT NULL,          -- wish / report / support
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',     -- apple / device / guest
            title TEXT,
            subtitle TEXT,
            ref_id TEXT,
            status TEXT DEFAULT 'pending',      -- pending / replied / resolved
            last_sender TEXT,
            last_message TEXT,
            unread_user INTEGER DEFAULT 0,
            unread_admin INTEGER DEFAULT 0,
            app_version TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_st_user ON support_threads(app, user_id, updated_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_st_admin ON support_threads(app, thread_type, status, updated_at)')
    c.execute('''
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_key TEXT NOT NULL,
            app TEXT NOT NULL DEFAULT 'ONews',
            sender TEXT NOT NULL,               -- user / admin
            content TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            read_user INTEGER DEFAULT 0,
            read_admin INTEGER DEFAULT 0
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sm_thread ON support_messages(thread_key, id)')
    c.execute("CREATE TABLE IF NOT EXISTS support_meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()
    print("客服会话表已就绪。")


def _norm_user_type(user_id, user_type=None):
    if not user_id:
        return 'guest'
    if user_id.startswith('dev_'):
        return 'device'
    if user_id == 'guest_user':
        return 'guest'
    return user_type if user_type in ('apple', 'device') else 'apple'


def support_thread_key(app_name, ttype, *parts):
    return "|".join([app_name, ttype] + [str(p) for p in parts])


def support_upsert_thread(c, app_name, thread_key, thread_type, user_id, user_type=None,
                          title=None, subtitle=None, ref_id=None, app_version=None):
    """创建或更新会话头。c 为 sqlite cursor(row_factory=Row)"""
    now = now_iso()
    ut = _norm_user_type(user_id, user_type)
    row = c.execute("SELECT id FROM support_threads WHERE thread_key=?", (thread_key,)).fetchone()
    if row is None:
        c.execute('''INSERT INTO support_threads
            (app, thread_key, thread_type, user_id, user_type, title, subtitle, ref_id,
             status, last_sender, last_message, unread_user, unread_admin,
             app_version, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,'pending',NULL,NULL,0,0,?,?,?)''',
            (app_name, thread_key, thread_type, user_id, ut, title or '', subtitle or '',
             str(ref_id or ''), app_version or '', now, now))
    else:
        sets, params = ["updated_at=?", "user_type=?"], [now, ut]
        if title:
            sets.append("title=?"); params.append(title)
        if subtitle:
            sets.append("subtitle=?"); params.append(subtitle)
        if app_version:
            sets.append("app_version=?"); params.append(app_version)
        params.append(thread_key)
        c.execute(f"UPDATE support_threads SET {','.join(sets)} WHERE thread_key=?", params)


def support_add_message(c, app_name, thread_key, sender, content, created_at=None):
    now = created_at or now_iso()
    c.execute('''INSERT INTO support_messages
        (thread_key, app, sender, content, created_at, read_user, read_admin)
        VALUES (?,?,?,?,?,?,?)''',
        (thread_key, app_name, sender, content, now,
         1 if sender == 'user' else 0, 1 if sender == 'admin' else 0))
    if sender == 'user':
        c.execute('''UPDATE support_threads
                     SET last_sender='user', last_message=?, updated_at=?,
                         unread_admin=unread_admin+1, status='pending'
                     WHERE thread_key=?''', (content[:200], now, thread_key))
    else:
        c.execute('''UPDATE support_threads
                     SET last_sender='admin', last_message=?, updated_at=?,
                         unread_user=unread_user+1, status='replied'
                     WHERE thread_key=?''', (content[:200], now, thread_key))


def migrate_support_threads_once():
    """把已有的寻片/举报数据一次性搬成会话+消息（保证历史记录不丢）"""
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        if c.execute("SELECT value FROM support_meta WHERE key='migrated_v1'").fetchone():
            return
        # ---- 寻片 ----
        wishes = c.execute('''SELECT id, user_id, user_type, keyword, wish_content, app_version,
                                     first_at, last_at, status, admin_reply, reply_status, replied_at
                              FROM video_wish_requests''').fetchall()
        for w in wishes:
            tk = support_thread_key('ONews', 'wish', w['id'])
            support_upsert_thread(c, 'ONews', tk, 'wish', w['user_id'], w['user_type'],
                                  title=w['wish_content'],
                                  subtitle=(f"搜索词: {w['keyword']}" if w['keyword'] else ''),
                                  ref_id=w['id'], app_version=w['app_version'])
            c.execute('''INSERT INTO support_messages
                (thread_key, app, sender, content, created_at, read_user, read_admin)
                VALUES (?,?,'user',?,?,1,1)''',
                (tk, 'ONews', f"我想看：{w['wish_content']}", w['first_at']))
            if w['admin_reply']:
                unread = 1 if w['reply_status'] == 'unread' else 0
                c.execute('''INSERT INTO support_messages
                    (thread_key, app, sender, content, created_at, read_user, read_admin)
                    VALUES (?,?,'admin',?,?,?,1)''',
                    (tk, 'ONews', w['admin_reply'], w['replied_at'] or w['last_at'], 0 if unread else 1))
                c.execute('''UPDATE support_threads
                             SET unread_user=?, last_sender='admin', last_message=?, status=?, updated_at=?
                             WHERE thread_key=?''',
                          (unread, w['admin_reply'][:200],
                           'resolved' if w['status'] == 'resolved' else 'replied',
                           w['replied_at'] or w['last_at'], tk))
            else:
                c.execute('''UPDATE support_threads
                             SET last_sender='user', last_message=?, unread_admin=1, updated_at=?
                             WHERE thread_key=?''',
                          ((w['wish_content'] or '')[:200], w['last_at'], tk))
        # ---- 坏链接举报 ----
        reports = c.execute('''SELECT user_id, video_title, episode_url, channel_name, episode_name,
                                      report_type, note, app_version, first_at, last_at, status,
                                      admin_reply, reply_status, replied_at
                               FROM video_link_reports''').fetchall()
        for r in reports:
            tk = support_thread_key('ONews', 'report', r['user_id'], r['episode_url'])
            title = r['video_title'] or '(未知影片)'
            sub = " · ".join([x for x in [r['channel_name'], r['episode_name']] if x])
            support_upsert_thread(c, 'ONews', tk, 'report', r['user_id'], None,
                                  title=title, subtitle=sub, ref_id=r['episode_url'],
                                  app_version=r['app_version'])
            body = f"报错反馈：{REPORT_TYPE_NAMES.get(r['report_type'], r['report_type'])}"
            if r['note']:
                body += f"\n补充说明：{r['note']}"
            c.execute('''INSERT INTO support_messages
                (thread_key, app, sender, content, created_at, read_user, read_admin)
                VALUES (?,?,'user',?,?,1,1)''', (tk, 'ONews', body, r['first_at']))
            if r['admin_reply']:
                unread = 1 if r['reply_status'] == 'unread' else 0
                c.execute('''INSERT INTO support_messages
                    (thread_key, app, sender, content, created_at, read_user, read_admin)
                    VALUES (?,?,'admin',?,?,?,1)''',
                    (tk, 'ONews', r['admin_reply'], r['replied_at'] or r['last_at'], 0 if unread else 1))
                c.execute('''UPDATE support_threads
                             SET unread_user=?, last_sender='admin', last_message=?, status=?, updated_at=?
                             WHERE thread_key=?''',
                          (unread, r['admin_reply'][:200],
                           'resolved' if r['status'] == 'resolved' else 'replied',
                           r['replied_at'] or r['last_at'], tk))
            else:
                c.execute('''UPDATE support_threads
                             SET last_sender='user', last_message=?, unread_admin=1, updated_at=?
                             WHERE thread_key=?''', (body[:200], r['last_at'], tk))

        c.execute("INSERT OR REPLACE INTO support_meta VALUES ('migrated_v1','1')")
        conn.commit()
        print(f"[support] 已迁移 {len(wishes)} 条寻片 / {len(reports)} 条举报为会话记录。")
    except Exception:
        conn.rollback()
        traceback.print_exc()
    finally:
        conn.close()


# ---- 客户端在线客服 API ----
@app.route('/api/support/threads', methods=['GET'])
def api_support_threads():
    app_name = request.args.get('app', 'ONews')
    user_id = request.args.get('user_id')
    if app_name not in SUPPORT_APPS or not user_id:
        return jsonify({"threads": [], "unread_total": 0})
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute('''SELECT thread_key, thread_type, title, subtitle, status,
                                      last_sender, last_message, unread_user, updated_at
                               FROM support_threads
                               WHERE app=? AND user_id=?
                               ORDER BY updated_at DESC LIMIT 100''',
                            (app_name, user_id)).fetchall()
        total = sum(int(r['unread_user'] or 0) for r in rows)
        return jsonify({"threads": [dict(r) for r in rows], "unread_total": total})
    finally:
        conn.close()


@app.route('/api/support/messages', methods=['GET'])
def api_support_messages():
    app_name = request.args.get('app', 'ONews')
    user_id = request.args.get('user_id')
    thread_key = request.args.get('thread_key')
    if not user_id or not thread_key:
        return jsonify({"messages": []})
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        t = c.execute("SELECT * FROM support_threads WHERE thread_key=? AND app=?",
                      (thread_key, app_name)).fetchone()
        if not t or t['user_id'] != user_id:
            return jsonify({"messages": []})
        rows = c.execute('''SELECT id, sender, content, created_at FROM support_messages
                            WHERE thread_key=? ORDER BY id ASC''', (thread_key,)).fetchall()
        c.execute("UPDATE support_messages SET read_user=1 WHERE thread_key=? AND sender='admin'", (thread_key,))
        c.execute("UPDATE support_threads SET unread_user=0 WHERE thread_key=?", (thread_key,))
        # 同步把老横幅标记为已读，避免首页重复提醒
        if t['thread_type'] == 'wish' and t['ref_id']:
            c.execute("UPDATE video_wish_requests SET reply_status='read' WHERE id=? AND user_id=?",
                      (t['ref_id'], user_id))
        elif t['thread_type'] == 'report' and t['ref_id']:
            c.execute('''UPDATE video_link_reports SET reply_status='read'
                         WHERE user_id=? AND episode_url=?''', (user_id, t['ref_id']))
        return jsonify({"messages": [dict(r) for r in rows], "thread": dict(t)})
    finally:
        conn.close()


@app.route('/api/support/send', methods=['POST'])
def api_support_send():
    data = _json_body()
    app_name = data.get('app', 'ONews')
    user_id = data.get('user_id')
    content = (data.get('content') or '').strip()[:1000]
    if app_name not in SUPPORT_APPS or not user_id or not content:
        return jsonify({"error": "Invalid params"}), 400
    if _too_frequent(support_last_time, user_id, 2):
        return jsonify({"error": "Too frequent"}), 429

    general_key = support_thread_key(app_name, 'support', user_id)
    thread_key = data.get('thread_key') or general_key
    app_version = data.get('app_version', '')

    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("BEGIN IMMEDIATE")
        t = c.execute("SELECT * FROM support_threads WHERE thread_key=?", (thread_key,)).fetchone()
        if t is None:
            if thread_key != general_key:
                c.execute("COMMIT")
                return jsonify({"error": "Thread not found"}), 404
            support_upsert_thread(c, app_name, general_key, 'support', user_id,
                                  data.get('user_type'),
                                  title=(data.get('title') or content)[:60],
                                  subtitle='', ref_id='', app_version=app_version)
        elif t['user_id'] != user_id:
            c.execute("COMMIT")
            return jsonify({"error": "Forbidden"}), 403

        support_add_message(c, app_name, thread_key, 'user', content)

        # 追问也要让后台的寻片/举报列表重新变成"待处理"
        t2 = c.execute("SELECT thread_type, ref_id FROM support_threads WHERE thread_key=?",
                       (thread_key,)).fetchone()
        if t2:
            if t2['thread_type'] == 'wish' and t2['ref_id']:
                c.execute("UPDATE video_wish_requests SET status='pending', last_at=? WHERE id=?",
                          (now_iso(), t2['ref_id']))
            elif t2['thread_type'] == 'report' and t2['ref_id']:
                c.execute('''UPDATE video_link_reports SET status='pending', last_at=?
                             WHERE user_id=? AND episode_url=?''',
                          (now_iso(), user_id, t2['ref_id']))
        c.execute("COMMIT")
        return jsonify({"status": "ok", "thread_key": thread_key})
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---- 后台在线客服 API ----
@app.route('/admin/api/support/threads', methods=['GET'])
@require_admin
def admin_support_threads():
    app_name = request.args.get('app', 'ONews')
    status = request.args.get('status', 'open')
    where = "app=? AND thread_type='support'"
    if status == 'open':
        where += " AND status!='resolved'"
    sql = f'''SELECT thread_key, user_id, user_type, title, status, last_sender, last_message,
                     unread_admin, app_version, created_at, updated_at,
                     (SELECT COUNT(*) FROM support_messages m WHERE m.thread_key=support_threads.thread_key) AS msg_count
              FROM support_threads WHERE {where}
              ORDER BY (unread_admin>0) DESC, updated_at DESC LIMIT 200'''
    return jsonify(_query_analytics(sql, [app_name]))


@app.route('/admin/api/support/messages', methods=['GET'])
@require_admin
def admin_support_messages():
    thread_key = request.args.get('thread_key')
    if not thread_key:
        return jsonify({"messages": [], "thread": None})
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        t = c.execute("SELECT * FROM support_threads WHERE thread_key=?", (thread_key,)).fetchone()
        rows = c.execute('''SELECT id, sender, content, created_at FROM support_messages
                            WHERE thread_key=? ORDER BY id ASC''', (thread_key,)).fetchall()
        c.execute("UPDATE support_messages SET read_admin=1 WHERE thread_key=? AND sender='user'", (thread_key,))
        c.execute("UPDATE support_threads SET unread_admin=0 WHERE thread_key=?", (thread_key,))
        return jsonify({"thread": dict(t) if t else None, "messages": [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/admin/api/support/reply', methods=['POST'])
@require_admin
def admin_support_reply():
    data = _json_body()
    thread_key = data.get('thread_key')
    reply = (data.get('reply') or '').strip()[:1000]
    if not thread_key or not reply:
        return jsonify({"error": "Missing params"}), 400
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("BEGIN IMMEDIATE")
        t = c.execute("SELECT * FROM support_threads WHERE thread_key=?", (thread_key,)).fetchone()
        if not t:
            c.execute("COMMIT")
            return jsonify({"error": "Thread not found"}), 404
        support_add_message(c, t['app'], thread_key, 'admin', reply)
        now = now_iso()
        # 同步老表，让客户端首页横幅照旧生效
        if t['thread_type'] == 'wish' and t['ref_id']:
            c.execute('''UPDATE video_wish_requests
                         SET status='resolved', admin_reply=?, reply_status='unread', replied_at=?
                         WHERE id=?''', (reply, now, t['ref_id']))
        elif t['thread_type'] == 'report' and t['ref_id']:
            c.execute('''UPDATE video_link_reports
                         SET status='resolved', admin_reply=?, reply_status='unread', replied_at=?
                         WHERE user_id=? AND episode_url=?''',
                      (reply, now, t['user_id'], t['ref_id']))
        c.execute("COMMIT")
        return jsonify({"status": "success"})
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/admin/api/support/resolve', methods=['POST'])
@require_admin
def admin_support_resolve():
    thread_key = _json_body().get('thread_key')
    if not thread_key:
        return jsonify({"error": "Missing thread_key"}), 400
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.execute("UPDATE support_threads SET status='resolved' WHERE thread_key=?", (thread_key,))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §3 OVideo 视频（独享）                                                ║
# ║   3.1 配置与缓存  3.2 渠道屏蔽  3.3 数据规范化/集数解析                  ║
# ║   3.4 OVideo.db 构建  3.5 通用辅助/点数/埋点表                          ║
# ║   3.6 客户端路由  3.7 搜索  3.8 举报/寻片  3.9 埋点/点数  3.10 后台      ║
# ╚══════════════════════════════════════════════════════════════════════╝

# ---------------- §3.1 配置与缓存 ----------------
OVIDEO_DIR = os.path.join(BASE_RESOURCES_DIR, 'OVideo')
OVIDEO_COVER_DIR = os.path.join(OVIDEO_DIR, 'cover_image')
OVIDEO_DB_PATH = os.path.join(OVIDEO_DIR, 'OVideo.db')
OVIDEO_VIDEO_FILE = os.path.join(OVIDEO_DIR, 'OVideos.json')
OVIDEO_BLACKLIST_FILE = os.path.join(OVIDEO_DIR, 'blacklist_url.json')
OVIDEO_MAPPING_FILE = os.path.join(OVIDEO_DIR, 'url_mapping.json')
OVIDEO_SCHEMA_VERSION = "3"          # 表结构变更 -> +1，触发自动重建

# Featured 首页「按上映日期」排序时 Drama 改用 (更新日期 − N 天)（当前未启用）
FEATURED_DRAMA_DATE_OFFSET_DAYS = 2

ALLOWED_EVENT_TYPES = {'play', 'download_complete'}
ALLOWED_REPORT_TYPES = {'playback_failed', 'download_failed', 'media_error', 'content_mismatch', 'other'}
report_last_time = {}   # 软限流
wish_last_time = {}     # 软限流

_video_db_lock = threading.Lock()
_VIDEO_DB_CHECK_INTERVAL = 3.0       # ensure_video_db 最多每 N 秒真正检查一次
_video_db_state = {"checked_at": 0.0}

_fuzzy_cache = {"mtime": None, "rows": None}
_fuzzy_cache_lock = threading.Lock()


def _get_valid_urls():
    """url_mapping 的 dict（只用来做 `in` 判断，等价于原来的 key 集合）"""
    return _load_json_cached(OVIDEO_MAPPING_FILE) or {}


# ---------------- §3.2 播放渠道(channel)屏蔽 ----------------
# 配置文件（可热改，改完下一次请求自动重建 OVideo.db）：Resources/OVideo/blocked_channels.json
# {
#   "enabled": true,
#   "names": ["暴风", "shangxidq"],     # 精确匹配渠道名(忽略大小写/空格)
#   "keywords": [],                     # 子串匹配
#   "block_resolve": true,              # 拦截老客户端缓存的播放页 URL
#   "hide_when_no_playable": false      # 剩余渠道一个能播的都没有时隐藏
# }
OVIDEO_CHANNEL_BLOCK_FILE = os.path.join(OVIDEO_DIR, 'blocked_channels.json')
DEFAULT_CHANNEL_BLOCK = {
    "enabled": True,
    "names": ["暴风", "shangxidq"],
    "keywords": [],
    "block_resolve": True,
    "hide_when_no_playable": False,
}
_channel_block_cache = {"mtime": None, "cfg": None}
_channel_block_lock = threading.Lock()
_blocked_eps_cache = {"mtime": None, "urls": set()}


def _norm_channel_name(name):
    return (name or "").strip().replace(" ", "").replace("　", "").lower()


def load_channel_block_config(force=False):
    """读取并缓存渠道屏蔽配置；文件 mtime 变化即自动重载。"""
    try:
        m = os.path.getmtime(OVIDEO_CHANNEL_BLOCK_FILE)
    except OSError:
        m = 0.0
    if (not force and _channel_block_cache["cfg"] is not None
            and _channel_block_cache["mtime"] == m):
        return _channel_block_cache["cfg"]
    with _channel_block_lock:
        raw = dict(DEFAULT_CHANNEL_BLOCK)
        if m:
            try:
                with open(OVIDEO_CHANNEL_BLOCK_FILE, 'r', encoding='utf-8') as f:
                    user = json.load(f)
                if isinstance(user, dict):
                    raw.update(user)
                elif isinstance(user, list):
                    raw["names"] = user
            except Exception as e:
                print(f"[OVideo] blocked_channels.json 读取失败，使用默认配置: {e}")
        names = {_norm_channel_name(n) for n in (raw.get("names") or []) if str(n).strip()}
        keywords = [k for k in (_norm_channel_name(x) for x in (raw.get("keywords") or [])) if k]
        cfg = {
            "enabled": bool(raw.get("enabled", True)) and bool(names or keywords),
            "names": names,
            "keywords": keywords,
            "block_resolve": bool(raw.get("block_resolve", True)),
            "hide_when_no_playable": bool(raw.get("hide_when_no_playable", False)),
        }
        _channel_block_cache["cfg"] = cfg
        _channel_block_cache["mtime"] = m
        return cfg


def is_channel_blocked(channel_name, cfg=None):
    cfg = cfg or load_channel_block_config()
    if not cfg["enabled"]:
        return False
    n = _norm_channel_name(channel_name)
    if not n:
        return False
    if n in cfg["names"]:
        return True
    return any(k in n for k in cfg["keywords"])


def channel_block_signature(cfg):
    """生效配置的指纹：配置变了 -> OVideo.db 自动重建"""
    payload = json.dumps({
        "e": cfg["enabled"], "n": sorted(cfg["names"]), "k": sorted(cfg["keywords"]),
        "r": cfg["block_resolve"], "h": cfg["hide_when_no_playable"],
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def _get_blocked_episode_urls():
    """被屏蔽渠道独占的 episode URL 集合（随 OVideo.db mtime 失效）"""
    if not load_channel_block_config()["block_resolve"]:
        return set()
    try:
        m = os.path.getmtime(OVIDEO_DB_PATH)
    except OSError:
        return set()
    if _blocked_eps_cache["mtime"] == m:
        return _blocked_eps_cache["urls"]
    try:
        conn = sqlite3.connect(OVIDEO_DB_PATH, timeout=10.0)
        try:
            urls = {r[0] for r in conn.execute("SELECT url FROM blocked_channel_eps")}
        finally:
            conn.close()
    except Exception as e:
        print(f"[OVideo] 读取 blocked_channel_eps 失败: {e}")
        return set()
    _blocked_eps_cache["urls"] = urls
    _blocked_eps_cache["mtime"] = m
    return urls


def filter_blocked_channels(playlist, cfg=None):
    """给直读 JSON 的老接口用：返回过滤后的 playlist"""
    cfg = cfg or load_channel_block_config()
    if not cfg["enabled"] or not playlist:
        return playlist
    return [ch for ch in playlist if not is_channel_blocked(ch.get('name'), cfg)]


# ---------------- §3.3 数据规范化 / 集数解析（与客户端保持同步） ----------------
TYPE_MAPPING = {
    "科幻片":"科幻","奇幻":"科幻","奇幻片":"科幻","异世界":"科幻","玄幻":"科幻","探案":"悬疑",
    "运动":"体育","动作片":"动作","武侠":"古装","战争片":"战争","战斗":"战争","同杏":"同性",
    "剧情片":"剧情","日常":"剧情","黑色电影":"剧情","韩剧":"剧情","美剧":"剧情","国产剧":"剧情",
    "港台剧":"剧情","日剧":"剧情","国产":"剧情","大陆":"剧情","泰剧":"剧情","美国":"剧情","欧美":"剧情",
    "美国剧":"剧情","欧美剧":"剧情","日本剧":"剧情","日本":"剧情","香港剧":"剧情","韩国剧":"剧情",
    "韩国":"剧情","日韩":"剧情","日韩剧":"剧情","香港":"剧情","台湾":"剧情","港台":"剧情","邵氏电影":"剧情",
    "泰国":"剧情","海外剧":"剧情","海外":"剧情","台湾剧":"剧情",
    "喜剧片":"喜剧","搞笑":"喜剧","爱情片":"爱情","恋爱":"爱情","情":"爱情","浪漫":"爱情",
    "丧尸":"恐怖","恐怖片":"恐怖","惊栗":"惊悚","惊悚片":"惊悚","犯罪片":"犯罪",
    "记录":"纪录片","其他":"纪录片","纪录":"纪录片","记录片":"纪录片",
    "选秀":"其它","大陆综艺":"其它","晚会":"其它","日韩综艺":"其它","欧美综艺":"其它","相声":"其它",
    "访谈":"其它","戏曲":"其它","港台综艺":"其它","国产综艺":"其它","动画":"其它","海外动漫":"其它",
    "鬼怪":"其它","日本动漫":"其它","综艺":"其它","有声动漫":"其它","机战":"其它","日韩动漫":"其它",
    "欧美动漫":"其它","脱口秀":"其它","游戏":"其它","热血":"其它","致郁":"其它","动漫片":"其它","动画片":"其它",
    "动漫":"其它","动漫电影":"其它","动画电影":"其它","国产动漫":"其它","真人秀":"其它","港台动漫":"其它",
}
_R_CHINA = {"中国大陆","内地","澳门","大陆","大陆国语","中国","国产"}
_R_TW    = {"台湾","港台","中国台湾"}
_R_HK    = {"香港","中国香港","中国澳门"}
_R_EU    = {"欧美","英国","西班牙","挪威","瑞典","丹麦","乌克兰","南斯拉夫","塞浦路斯","奥地利","UK","United Kingdom",
            "保加利亚","克罗地亚","塞尔维亚","德国","意大利","捷克","捷克斯洛伐克","法国","波黑","玻利维亚",
            "突尼斯","罗马尼亚","西德","马耳他","澳大利亚","爱尔兰","瑞士","立陶宛","芬兰","荷兰","匈牙利",
            "希腊","拉脱维亚","马其顿","新西兰","比利时","波兰","NZ","冰岛","北马其顿","卢森堡","斯洛伐克",
            "斯洛文尼亚","澳大利亚Australia","爱沙尼亚","英语","葡萄牙"}
_R_ASIA  = {"乌兹别克斯坦","俄罗斯","印度尼西亚","土耳其","新加坡","格鲁吉亚","泰国","苏联","菲律宾",
            "巴基斯坦","不丹","哈萨克斯坦","塔吉克斯坦","尼泊尔","柬埔寨","蒙古","越南","马来西亚"}
_R_MID   = {"伊拉克","伊朗","以色列","埃及","巴勒斯坦","叙利亚","巴勒斯坦被占领区","沙特阿拉伯","约旦","苏丹","阿富汗","黎巴嫩","科威特"}
_R_AM    = {"加拿大","墨西哥","哥伦比亚","巴西","智利","厄瓜多尔","阿根廷","秘鲁","Aruba","Canada","Jamaica",
            "USA","乌拉圭","古巴","委内瑞拉","牙买加","特立尼达和多巴哥"}
_R_AF    = {"南非","乍得","埃塞俄比亚","塞内加尔","摩洛哥","阿尔及利亚","阿尔巴尼亚"}
_CHINESE_RE = re.compile(r'[\u4e00-\u9fa5·]+')

_SEASON_EP_RE = re.compile(r'[Ss](\d{1,2})[\s._\-]*[Ee](\d{1,3})')
_EP_LABEL_RE  = re.compile(r'第\s*(\d{1,4})\s*[集期话話幕]')
_PURE_NUM_RE  = re.compile(r'^\s*(\d{1,4})\s*$')
_ANY_NUM_RE   = re.compile(r'(\d{1,4})')


def _episode_index(name):
    """从单个 episode 名解析集号；无法解析返回 None（例如 'HD'、'20241019'）"""
    n = (name or '').strip()
    if not n:
        return None
    digits = re.sub(r'\D', '', n)
    if len(digits) >= 6 and digits[:2] in ('19', '20'):   # 日期型
        return None
    m = _EP_LABEL_RE.search(n)
    if m:
        return int(m.group(1))
    m = _PURE_NUM_RE.match(n)
    if m:
        return int(m.group(1))
    m = _ANY_NUM_RE.search(n)
    if m:
        v = int(m.group(1))
        if 0 < v <= 2000:
            return v
    return None


def _channel_progress(ep_names):
    """单个渠道的"有效集数"：
       1) 存在 SxxExx：只取最大季内的集数
       2) 否则按 第N集/第N期/纯数字 取最大集号
       3) 否则（日期型 / HD 等）用集数数量"""
    names = [n for n in ep_names if (n or '').strip()]
    if not names:
        return 0
    seasons = {}
    for n in names:
        m = _SEASON_EP_RE.search(n)
        if m:
            seasons.setdefault(int(m.group(1)), set()).add(int(m.group(2)))
    if seasons:
        top = max(seasons.keys())
        return max(max(seasons[top]), len(seasons[top]))
    valid = [v for v in (_episode_index(n) for n in names) if v is not None]
    if valid and len(valid) * 2 >= len(names):
        return max(valid)
    return len(names)


def _normalize_region(region):
    if not region or not region.strip():
        return "其它"
    first = region.split('/')[0].strip()
    if first in _R_CHINA: return "中国"
    if first in _R_TW:    return "中国台湾"
    if first in _R_HK:    return "香港澳门"
    if first in _R_EU:    return "欧洲"
    if first in _R_ASIA:  return "亚洲"
    if first in _R_MID:   return "中东"
    if first in _R_AM:    return "北美洲/南美洲"
    if first in _R_AF:    return "非洲"
    return first


def _normalize_types(types):
    if not types:
        return []
    return list({TYPE_MAPPING.get(t, t) for t in types})


def _clean_name(raw):
    t = (raw or "").strip()
    if not t:
        return ""
    m = _CHINESE_RE.search(t)
    return m.group(0).strip() if m else t


def _norm_search(text):
    return (text or "").lower().replace('·', '').replace(' ', '')

# ---- 搜索：主名称提取（"火线 第五季" / "火线1" / "火线：突击" / "火线—营救" 的主名称都是"火线"）----
_SERIES_CUT_RE = re.compile(r'\s+-\s+|[:：—–－~～|｜（(\[【《〈,，]')
_SERIES_TAIL_RE = re.compile(
    r'\s*(?:'
    r'第\s*[0-9零一二三四五六七八九十百两]+\s*[季部集期篇章辑]'
    r'|(?:season|series|part|vol|chapter)\.?\s*\d{1,3}'
    r'|(?<![a-z])s\d{1,2}'
    r'|(?<![a-z])[ivx]{1,5}'
    r'|[ⅰ-ⅻⅠ-Ⅻ]{1,3}'
    r'|(?<!\d)\d{1,3}'
    r'|(?:国语|粤语|普通话|英语|双语|中字|未删减|导演剪辑)版?'
    r'|(?:剧场|电影|真人)版'
    r')\s*$', re.I)


def _series_main_keys(text):
    """返回片名可能的"主名称"(已 _norm_search 归一化)集合"""
    t = (text or '').strip().lower()
    if not t:
        return set()
    keys = set()

    def add(s):
        s = s.strip()
        for _ in range(4):                       # 反复剥离尾部季号/版本标记
            ns = _SERIES_TAIL_RE.sub('', s).strip()
            if not ns or ns == s:
                break
            s = ns
        n = _norm_search(s)
        if n:
            keys.add(n)

    m = _SERIES_CUT_RE.search(t)
    head = t[:m.start()] if (m and m.start() > 0) else t
    add(head)
    hs = head.strip()
    if ' ' in hs and _CHINESE_RE.search(hs):     # 中文片名常用空格分隔副标题
        add(hs.split()[0])
    return keys


def _release_sort_key(date):
    return date.split('(')[0] if date else ""


def _release_year(date):
    if not date:
        return None
    try:
        return int(date.split('(')[0].split('-')[0])
    except Exception:
        return None


def _best_rating(ratings):
    if not ratings:
        return 0.0
    vals = []
    for v in ratings.values():
        try:
            vals.append(float(v))
        except Exception:
            pass
    return max(vals) if vals else 0.0


# ---------------- §3.4 OVideo.db 构建 / 自动重建 ----------------
def build_video_db():
    """把 OVideos.json 全量构建成 SQLite。
       先写到临时文件，完成后 os.replace 原子替换 —— 重建期间其他请求仍读旧库，不会报 no such table。"""
    if not os.path.exists(OVIDEO_VIDEO_FILE):
        return

    chan_cfg = load_channel_block_config(force=True)
    if chan_cfg['enabled']:
        print("[OVideo] 渠道屏蔽已启用 names=%s keywords=%s"
              % (sorted(chan_cfg['names']), chan_cfg['keywords']))
    else:
        print("[OVideo] 渠道屏蔽未启用")

    print("[OVideo] 开始构建 SQLite ...")
    src_mtime = os.path.getmtime(OVIDEO_VIDEO_FILE)
    with open(OVIDEO_VIDEO_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    blacklist_set = set()
    if os.path.exists(OVIDEO_BLACKLIST_FILE):
        try:
            with open(OVIDEO_BLACKLIST_FILE, 'r', encoding='utf-8') as bf:
                blacklist_set = set(json.load(bf).keys())
        except Exception as e:
            print(f"[OVideo] 黑名单读取失败: {e}")

    valid_url_set = set()
    if os.path.exists(OVIDEO_MAPPING_FILE):
        try:
            with open(OVIDEO_MAPPING_FILE, 'r', encoding='utf-8') as mf:
                valid_url_set = set(json.load(mf).keys())
        except Exception as e:
            print(f"[OVideo] url_mapping 读取失败: {e}")

    def _is_playable(u):
        return (u in valid_url_set) or ('.m3u8' in (u or '').lower())

    rows = []
    cat_order = list(data.keys())
    blocked_ep_urls = set()
    channel_stats = {}
    stat_dropped_channels = 0
    stat_hidden_by_channel = 0

    for category, items in data.items():
        for item in items:
            url = item.get('url')
            if not url:
                continue
            name = item.get('name', '') or ''
            region = item.get('地区', '') or ''
            date = item.get('date', '') or ''
            ratings = item.get('评分') or {}
            ntypes = _normalize_types(item.get('类型') or [])
            director = item.get('导演') or ''
            cast = item.get('主演') or []
            alias = item.get('alias') or ''
            intro = item.get('intro') or ''
            cleaned_cast = [_clean_name(x) for x in cast]
            cleaned_dir = _clean_name(director)

            item_nolist = dict(item)
            playlist = item_nolist.pop('playlist', []) or []

            # ---- 渠道屏蔽（必须在 hide / 集数计算之前） ----
            for ch in playlist:
                cn = (ch.get('name') or '').strip() or '(未命名)'
                channel_stats[cn] = channel_stats.get(cn, 0) + 1
            raw_channels_with_eps = len([ch for ch in playlist if (ch.get('episodes') or {})])

            if chan_cfg['enabled'] and playlist:
                kept_channels, dropped_channels = [], []
                for ch in playlist:
                    (dropped_channels if is_channel_blocked(ch.get('name'), chan_cfg)
                     else kept_channels).append(ch)
                if dropped_channels:
                    stat_dropped_channels += len(dropped_channels)
                    kept_urls_item = set()   # 同一 URL 也在保留渠道里就不能拦截
                    for ch in kept_channels:
                        kept_urls_item.update(u for u in (ch.get('episodes') or {}).values() if u)
                    for ch in dropped_channels:
                        for u in (ch.get('episodes') or {}).values():
                            if u and u not in kept_urls_item:
                                blocked_ep_urls.add(u)
                    playlist = kept_channels

            channels_with_eps = [ch for ch in playlist if (ch.get('episodes') or {})]

            hide_blacklisted = 0
            if raw_channels_with_eps > 0 and not channels_with_eps:
                hide_blacklisted = 1          # 所有渠道都被屏蔽
                stat_hidden_by_channel += 1
            elif blacklist_set:
                if category == 'Movie':
                    # Movie：所有 episode 全部命中黑名单才隐藏
                    all_ep_urls = [u for ch in channels_with_eps
                                   for u in (ch.get('episodes') or {}).values()]
                    if all_ep_urls and all(u in blacklist_set for u in all_ep_urls):
                        hide_blacklisted = 1
                elif category in ('Drama', 'Anime', 'Show'):
                    # 每个有剧集的渠道都满足：全部不可播 且 至少一个在黑名单 -> 隐藏
                    if channels_with_eps:
                        hide = True
                        for ch in channels_with_eps:
                            ep_urls = list((ch.get('episodes') or {}).values())
                            all_unplayable = all(not _is_playable(u) for u in ep_urls)
                            has_blacklisted = any(u in blacklist_set for u in ep_urls)
                            if not (all_unplayable and has_blacklisted):
                                hide = False
                                break
                        if hide:
                            hide_blacklisted = 1

            if (not hide_blacklisted and chan_cfg['hide_when_no_playable'] and channels_with_eps):
                if not any(_is_playable(u) for ch in channels_with_eps
                           for u in (ch.get('episodes') or {}).values()):
                    hide_blacklisted = 1

            # 追剧：有效集数（只统计可播放且不在黑名单的 episode）
            latest_ep_count = 0
            if category != 'Movie':
                per_channel = []
                for ch in channels_with_eps:
                    eps = ch.get('episodes') or {}
                    names = [n for n, u in eps.items() if _is_playable(u) and u not in blacklist_set]
                    if names:
                        per_channel.append(_channel_progress(names))
                if per_channel:
                    latest_ep_count = max(per_channel)

            rows.append((
                url, category, name,
                region, _normalize_region(region),
                _release_year(date), _release_sort_key(date),
                item.get('update', '') or '', _best_rating(ratings),
                '|' + '|'.join(ntypes) + '|',
                '|' + '|'.join(item.get('类型') or []) + '|',
                1 if '纪录片' in ntypes else 0,
                name.lower(), alias.lower(),
                ('\x1f'.join(item.get('类型') or [])).lower(),
                cleaned_dir.lower(), ('\x1f'.join(cleaned_cast)).lower(), intro.lower(),
                _norm_search(name), _norm_search(alias),
                _norm_search(cleaned_dir), _norm_search('\x1f'.join(cleaned_cast)),
                json.dumps(item_nolist, ensure_ascii=False),
                json.dumps(playlist, ensure_ascii=False),   # 已过滤屏蔽渠道
                hide_blacklisted,
                latest_ep_count,
            ))

    tmp_path = OVIDEO_DB_PATH + '.building'
    for p in (tmp_path, tmp_path + '-journal'):
        if os.path.exists(p):
            os.remove(p)
    conn = sqlite3.connect(tmp_path, timeout=60.0)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE videos (
            url TEXT PRIMARY KEY,
            category TEXT, name TEXT,
            region TEXT, norm_region TEXT,
            release_year INTEGER, release_sort_key TEXT,
            update_sort_key TEXT, best_rating REAL,
            norm_types TEXT, raw_types TEXT, has_documentary INTEGER,
            name_lower TEXT, alias_lower TEXT, types_lower TEXT,
            director_lower TEXT, cast_lower TEXT, intro_lower TEXT,
            name_norm TEXT, alias_norm TEXT, director_norm TEXT, cast_norm TEXT,
            item_json TEXT, playlist_json TEXT,
            hide_blacklisted INTEGER DEFAULT 0,
            latest_ep_count INTEGER DEFAULT 0
        )
    ''')
    c.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    c.execute("CREATE TABLE blocked_channel_eps (url TEXT PRIMARY KEY)")
    c.executemany("INSERT OR REPLACE INTO videos VALUES (%s)" % ",".join("?" * 26), rows)
    if blocked_ep_urls:
        c.executemany("INSERT OR IGNORE INTO blocked_channel_eps VALUES (?)", [(u,) for u in blocked_ep_urls])
    c.execute("CREATE INDEX idx_cat_update  ON videos(category, update_sort_key)")
    c.execute("CREATE INDEX idx_cat_release ON videos(category, release_sort_key)")
    c.execute("CREATE INDEX idx_cat_rating  ON videos(category, best_rating)")
    c.execute("CREATE INDEX idx_doc ON videos(has_documentary)")
    c.execute("CREATE INDEX idx_hide ON videos(hide_blacklisted)")
    meta = {
        'schema_version': OVIDEO_SCHEMA_VERSION,
        'source_mtime': str(src_mtime),
        'blacklist_mtime': str(os.path.getmtime(OVIDEO_BLACKLIST_FILE)) if os.path.exists(OVIDEO_BLACKLIST_FILE) else "0",
        'mapping_mtime': str(os.path.getmtime(OVIDEO_MAPPING_FILE)) if os.path.exists(OVIDEO_MAPPING_FILE) else "0",
        'channel_block_sig': channel_block_signature(chan_cfg),
        'channel_stats': json.dumps(channel_stats, ensure_ascii=False),
        'categories': json.dumps(cat_order, ensure_ascii=False),
    }
    c.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.close()
    os.replace(tmp_path, OVIDEO_DB_PATH)       # 原子替换

    _blocked_eps_cache["mtime"] = None
    _blocked_eps_cache["urls"] = set()
    print(f"[OVideo] 构建完成，共 {len(rows)} 条；屏蔽渠道 {stat_dropped_channels} 个，"
          f"因全渠道被屏蔽而隐藏 {stat_hidden_by_channel} 条，可拦截播放链接 {len(blocked_ep_urls)} 条。")


def _video_source_fingerprint():
    src_m = str(os.path.getmtime(OVIDEO_VIDEO_FILE))
    bl_m = str(os.path.getmtime(OVIDEO_BLACKLIST_FILE)) if os.path.exists(OVIDEO_BLACKLIST_FILE) else "0"
    map_m = str(os.path.getmtime(OVIDEO_MAPPING_FILE)) if os.path.exists(OVIDEO_MAPPING_FILE) else "0"
    chan_sig = channel_block_signature(load_channel_block_config())
    return (src_m, bl_m, map_m, OVIDEO_SCHEMA_VERSION, chan_sig)


def _read_video_meta():
    conn = sqlite3.connect(OVIDEO_DB_PATH, timeout=10.0)
    try:
        def _get(k):
            r = conn.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
            return r[0] if r else None
        return (_get('source_mtime'), _get('blacklist_mtime'), _get('mapping_mtime'),
                _get('schema_version'), _get('channel_block_sig'))
    finally:
        conn.close()


def ensure_video_db(force=False):
    """JSON / 黑名单 / url_mapping / 渠道屏蔽配置 / 表结构 变更时自动重建（最多每 3 秒检查一次）"""
    if not os.path.exists(OVIDEO_VIDEO_FILE):
        return
    now = time.time()
    if (not force and os.path.exists(OVIDEO_DB_PATH)
            and now - _video_db_state["checked_at"] < _VIDEO_DB_CHECK_INTERVAL):
        return
    _video_db_state["checked_at"] = now

    try:
        want = _video_source_fingerprint()
    except OSError:
        return

    def _up_to_date():
        if not os.path.exists(OVIDEO_DB_PATH):
            return False
        try:
            return _read_video_meta() == want
        except Exception:
            return False

    if _up_to_date():
        return
    with _video_db_lock:
        if _up_to_date():
            return
        try:
            build_video_db()
        except Exception:
            traceback.print_exc()
            print("[OVideo] 重建失败，继续使用旧库")


def _get_video_conn():
    conn = sqlite3.connect(OVIDEO_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------- §3.5 通用辅助：屏蔽配置 / 排序 / 模糊候选 / 点数配置 / 埋点表 ----------------
def _is_vip_permanent(user_id):
    """任一 App 永久 VIP（用于视频地区/类型屏蔽豁免），缓存 60 秒"""
    if not is_real_login_user(user_id):
        return False
    hit = _vip_perm_cache.get(user_id)
    if hit and time.time() - hit[1] < _SUB_KIND_TTL:
        return hit[0]
    try:
        conn = sqlite3.connect(USER_DB_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("""SELECT onews_is_permanent, finance_is_permanent, prediction_is_permanent
                                  FROM users WHERE apple_user_id=?""", (user_id,)).fetchone()
        finally:
            conn.close()
        val = bool(row and (row['onews_is_permanent'] == 1
                            or row['finance_is_permanent'] == 1
                            or row['prediction_is_permanent'] == 1))
    except Exception:
        return False
    _cache_put(_vip_perm_cache, user_id, (val, time.time()))
    return val


def _get_block_config(user_id):
    """返回 (region_keywords, type_keywords)；永久 VIP 或未开启时为空"""
    if _is_vip_permanent(user_id):
        return [], []
    vdata = _load_version_json('ONews')
    rf = vdata.get('video_region_filter') or {}
    tf = vdata.get('video_type_filter') or {}
    if not isinstance(rf, dict): rf = {}
    if not isinstance(tf, dict): tf = {}
    region_kw = [k for k in (rf.get('keywords') or []) if k] if rf.get('enabled', False) else []
    type_kw = [k for k in (tf.get('keywords') or []) if k] if tf.get('enabled', False) else []
    return region_kw, type_kw


def _block_where(region_kw, type_kw):
    clauses, params = [], []
    if region_kw:
        clauses.append("region NOT IN (%s)" % ",".join("?" * len(region_kw)))
        params += region_kw
    for kw in type_kw:
        clauses.append("raw_types NOT LIKE ?")
        params.append(f"%{kw}%")
    return clauses, params


def _order_clause(sort):
    if sort == 'date':   return "release_sort_key DESC"
    if sort == 'rating': return "best_rating DESC, release_sort_key DESC"
    return "update_sort_key DESC"


def _get_fuzzy_candidates():
    """全表模糊匹配候选（预切分 cast、预建字符集合），仅 OVideo.db mtime 变化时重建"""
    try:
        db_mtime = os.path.getmtime(OVIDEO_DB_PATH)
    except OSError:
        return []
    if _fuzzy_cache["mtime"] == db_mtime and _fuzzy_cache["rows"] is not None:
        return _fuzzy_cache["rows"]
    with _fuzzy_cache_lock:
        if _fuzzy_cache["mtime"] == db_mtime and _fuzzy_cache["rows"] is not None:
            return _fuzzy_cache["rows"]
        conn = _get_video_conn()
        raw = conn.execute(
            "SELECT url, category, name_norm, alias_norm, director_norm, cast_norm, "
            "name_lower, best_rating, release_sort_key, update_sort_key, "
            "hide_blacklisted, region, raw_types, release_year FROM videos"
        ).fetchall()
        conn.close()
        rows = []
        for r in raw:
            name = r['name_norm'] or ''
            alias = r['alias_norm'] or ''
            director = r['director_norm'] or ''
            cast_list = [c for c in (r['cast_norm'] or '').split('\x1f') if c]
            rows.append({
                'url': r['url'], 'category': r['category'],
                'name': name, 'alias': alias, 'director': director, 'cast_list': cast_list,
                'name_set': set(name), 'alias_set': set(alias), 'director_set': set(director),
                'cast_sets': [set(c) for c in cast_list],
                'name_len': len(r['name_lower'] or ''),
                'rating': r['best_rating'] or 0.0,
                'rel': r['release_sort_key'] or '',
                'upd': r['update_sort_key'] or '',
                'hide': r['hide_blacklisted'] or 0,
                'region': r['region'] or '',
                'raw_types': r['raw_types'] or '',
                'release_year': r['release_year'],
            })
        _fuzzy_cache["rows"] = rows
        _fuzzy_cache["mtime"] = db_mtime
        return rows


def get_video_points_config():
    data = _load_version_json('ONews')
    try:
        q = data.get('video_free_quota', {}) or {}
        enabled = bool(q.get('enabled', False))
        return {
            'daily_quota': int(q.get('daily_count', 0)) if enabled else 0,
            'first_login_bonus': int(q.get('first_login_bonus', 0)) if enabled else 0,
            'invite_reward_points': int(data.get('video_invite_reward_points', 8)),
        }
    except Exception as e:
        print(f"读取视频点数配置失败: {e}")
        return {'daily_quota': 0, 'first_login_bonus': 0, 'invite_reward_points': 8}


def resolve_video_access_type(user_id, reported=None, episode_key=None):
    """决定这次播放/下载记为"订阅"还是"点数"。订阅以服务器为准；非订阅优先看当天解锁来源。"""
    kind = get_subscription_kind(user_id, 'ONews')
    if kind:
        return kind
    r = (reported or '').strip()
    if r in ALLOWED_ACCESS_TYPES and r not in ('subscription', 'vip_permanent') and r != 'points':
        return r
    if episode_key:
        try:
            conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=10.0)
            conn.row_factory = sqlite3.Row
            row = conn.execute("""SELECT source FROM video_free_unlocks
                                  WHERE user_id=? AND episode_key=?
                                  ORDER BY id DESC LIMIT 1""", (user_id, episode_key)).fetchone()
            conn.close()
            if row:
                return 'points_bonus' if (row['source'] or 'daily') == 'bonus' else 'points_daily'
        except Exception:
            pass
    if r == 'points':
        return 'points'
    return 'points' if is_real_login_user(user_id) else 'free'


def init_ovideo_analytics_db():
    """analytics.db 中 OVideo 相关表"""
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_video_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            video_url TEXT NOT NULL,
            video_title TEXT,
            event_type TEXT NOT NULL,
            first_at TIMESTAMP NOT NULL,
            last_at TIMESTAMP NOT NULL,
            count INTEGER DEFAULT 1,
            UNIQUE(user_id, video_url, event_type)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS event_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            video_url TEXT NOT NULL,
            video_title TEXT,
            event_type TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    ''')
    for ddl in ("ALTER TABLE event_logs ADD COLUMN user_type TEXT DEFAULT 'apple'",
                "ALTER TABLE user_video_events ADD COLUMN user_type TEXT DEFAULT 'apple'",
                "ALTER TABLE event_logs ADD COLUMN source TEXT",
                "ALTER TABLE event_logs ADD COLUMN app_version TEXT",
                "ALTER TABLE event_logs ADD COLUMN access_type TEXT",
                "ALTER TABLE user_video_events ADD COLUMN access_type TEXT"):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError:
            pass
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_time ON event_logs(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_type ON event_logs(event_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_user ON event_logs(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_access ON event_logs(access_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_events_url ON user_video_events(video_url)')

    # 视频解锁表（source: bonus=一次性赠送 / daily=每日免费）
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_free_unlocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            episode_key TEXT NOT NULL,
            unlock_date TEXT NOT NULL,
            video_title TEXT,
            created_at TIMESTAMP NOT NULL,
            UNIQUE(user_id, episode_key, unlock_date)
        )
    ''')
    try:
        c.execute("ALTER TABLE video_free_unlocks ADD COLUMN source TEXT DEFAULT 'daily'")
    except sqlite3.OperationalError:
        pass
    c.execute('CREATE INDEX IF NOT EXISTS idx_unlock_user_date ON video_free_unlocks(user_id, unlock_date)')
    # 旧版一次性赠送表（已被 video_points 取代，保留兼容）
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_bonus_quota (
            user_id TEXT PRIMARY KEY,
            bonus_total INTEGER NOT NULL,
            bonus_remaining INTEGER NOT NULL,
            granted_at TIMESTAMP NOT NULL
        )
    ''')

    # 错误链接举报表
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_link_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            video_title TEXT,
            source_url TEXT,
            episode_url TEXT,
            channel_name TEXT,
            episode_name TEXT,
            real_url TEXT,
            report_type TEXT,
            note TEXT,
            app_version TEXT,
            first_at TIMESTAMP NOT NULL,
            last_at TIMESTAMP NOT NULL,
            count INTEGER DEFAULT 1,
            status TEXT DEFAULT 'pending',
            admin_reply TEXT,
            reply_status TEXT DEFAULT 'none',
            replied_at TIMESTAMP,
            UNIQUE(user_id, episode_url, report_type)
        )
    ''')
    for ddl in ("ALTER TABLE video_link_reports ADD COLUMN admin_reply TEXT",
                "ALTER TABLE video_link_reports ADD COLUMN reply_status TEXT DEFAULT 'none'",
                "ALTER TABLE video_link_reports ADD COLUMN replied_at TIMESTAMP"):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError:
            pass
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_status ON video_link_reports(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_ep ON video_link_reports(episode_url)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_reply ON video_link_reports(user_id, reply_status)')

    # 寻片/许愿请求表
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_wish_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',
            keyword TEXT,
            wish_content TEXT NOT NULL,
            app_version TEXT,
            first_at TIMESTAMP NOT NULL,
            last_at TIMESTAMP NOT NULL,
            count INTEGER DEFAULT 1,
            status TEXT DEFAULT 'pending',
            admin_reply TEXT,
            reply_status TEXT DEFAULT 'none',
            replied_at TIMESTAMP,
            UNIQUE(user_id, wish_content)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wish_status ON video_wish_requests(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wish_reply ON video_wish_requests(user_id, reply_status)')
    conn.commit()
    conn.close()

# ---------------- §3.6 OVideo 客户端路由：列表 / 详情 / 播放解析 ----------------
def _parse_max_year():
    """审核员限定：只看 <= max_year 的老片；非法值返回 None"""
    v = request.args.get('max_year')
    if not v:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _filter_playlist_playable(raw, valid):
    """只保留可播放（在 mapping 中或本身是 m3u8）的剧集，并下发原始顺序 episode_order"""
    out = []
    for channel in raw or []:
        eps, order = {}, []
        for ep_name, ep_url in (channel.get('episodes') or {}).items():
            if ep_url and (ep_url in valid or '.m3u8' in ep_url.lower()):
                eps[ep_name] = ep_url
                order.append(ep_name)
        if eps:
            nc = dict(channel)
            nc['episodes'] = eps
            nc['episode_order'] = order
            out.append(nc)
    return out


@app.route('/api/OVideo/videos', methods=['GET'])
def get_ovideos():
    """【老接口】一次性下发全部视频（新客户端走 /list /filter /search2）。
       【性能】改从 OVideo.db 读取（渠道屏蔽已在建库时处理），按 rowid 保持原 JSON 顺序。"""
    ensure_video_db()
    if not os.path.exists(OVIDEO_DB_PATH):
        return jsonify({"error": "Video file not found"}), 404
    user_id = request.args.get('user_id')
    if user_id and user_id in VIDEO_MODULE_BLOCKED_USERS:
        return jsonify({"categories": []})
    try:
        region_kw, type_kw = _get_block_config(user_id)
        bc, bp = _block_where(region_kw, type_kw)
        where = (" WHERE " + " AND ".join(bc)) if bc else ""
        valid_urls = _get_valid_urls()
        conn = _get_video_conn()
        try:
            r = conn.execute("SELECT value FROM meta WHERE key='categories'").fetchone()
            buckets = {c: [] for c in (json.loads(r['value']) if r else [])}
            for row in conn.execute(
                    f"SELECT category, item_json, playlist_json FROM videos{where} ORDER BY rowid", bp):
                it = json.loads(row['item_json'])
                it['playlist'] = _filter_playlist_playable(json.loads(row['playlist_json'] or '[]'), valid_urls)
                buckets.setdefault(row['category'], []).append(it)
        finally:
            conn.close()
        return jsonify({"categories": [{"name": k, "items": v} for k, v in buckets.items()]})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/OVideo/playlist', methods=['GET'])
def ovideo_playlist():
    ensure_video_db()
    url = request.args.get('url')
    if not url:
        return jsonify({"error": "Missing url"}), 400
    conn = _get_video_conn()
    try:
        row = conn.execute("SELECT playlist_json FROM videos WHERE url=?", (url,)).fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({"playlist": []})
    raw = json.loads(row['playlist_json'] or '[]')
    return jsonify({"playlist": _filter_playlist_playable(raw, _get_valid_urls())})


@app.route('/api/OVideo/track_series/status', methods=['POST'])
def ovideo_track_series_status():
    """追剧：批量查询一组剧的当前有效集数 / 更新时间"""
    ensure_video_db()
    urls = _json_body().get('urls') or []
    if not isinstance(urls, list):
        return jsonify({"items": []})
    urls = [u for u in urls if isinstance(u, str) and u][:400]
    if not urls:
        return jsonify({"items": []})
    conn = _get_video_conn()
    out = []
    CHUNK = 200
    try:
        for i in range(0, len(urls), CHUNK):
            chunk = urls[i:i + CHUNK]
            ph = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT url, category, name, update_sort_key, latest_ep_count, "
                f"hide_blacklisted, item_json FROM videos WHERE url IN ({ph})", chunk).fetchall()
            for r in rows:
                try:
                    it = json.loads(r['item_json'] or '{}')
                except Exception:
                    it = {}
                out.append({
                    "url": r['url'],
                    "category": r['category'],
                    "name": r['name'],
                    "image": it.get('image'),
                    "info": it.get('info'),
                    "update": r['update_sort_key'] or '',
                    "episode_count": int(r['latest_ep_count'] or 0),
                    "unavailable": bool(r['hide_blacklisted'] or 0),
                })
    finally:
        conn.close()
    return jsonify({"items": out})


@app.route('/api/OVideo/detail', methods=['GET'])
def ovideo_detail():
    """按 url 取单条完整剧集信息（追剧列表点击后跳详情页用）"""
    ensure_video_db()
    url = request.args.get('url')
    if not url:
        return jsonify({"error": "Missing url"}), 400
    conn = _get_video_conn()
    try:
        row = conn.execute("SELECT category, item_json FROM videos WHERE url=?", (url,)).fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    it = json.loads(row['item_json'] or '{}')
    it['category'] = row['category']
    return jsonify({"item": it})


@app.route('/api/OVideo/cover/<path:filename>', methods=['GET'])
def get_ovideo_cover(filename):
    try:
        safe_path = safe_join(OVIDEO_COVER_DIR, filename)
    except Exception:
        safe_path = None
    if not safe_path:
        return jsonify({"error": "Invalid path"}), 400
    if not os.path.isfile(safe_path):
        return jsonify({"error": "Image not found"}), 404
    directory, file = os.path.split(safe_path)
    response = send_from_directory(directory, file)
    response.headers['Cache-Control'] = 'public, max-age=604800'  # 7 天
    return response


@app.route('/api/OVideo/resolve', methods=['POST'])
def resolve_ovideo_url():
    """播放页 URL -> 真实 m3u8（同时做渠道屏蔽 / 黑名单拦截）"""
    data = _json_body()
    episode_url = data.get('url')
    if not episode_url or not isinstance(episode_url, str):
        return jsonify({"error": "Missing url"}), 400

    # 被封锁渠道的播放页（老客户端可能有本地缓存），必须放在 m3u8 直返之前
    if episode_url in _get_blocked_episode_urls():
        return jsonify({"error": "Blacklisted", "reason": "该播放源已下线，请切换其他播放源"}), 403

    # JSON 里直接写的 m3u8 直链，原样返回
    if '.m3u8' in episode_url.lower():
        return jsonify({"real_url": episode_url, "title": ""})

    blacklist = _load_json_cached(OVIDEO_BLACKLIST_FILE) or {}
    if episode_url in blacklist:
        return jsonify({"error": "Blacklisted", "reason": "该视频暂不可用"}), 403

    mappings = _load_json_cached(OVIDEO_MAPPING_FILE)
    if mappings is None:
        return jsonify({"error": "Mapping file not found"}), 404
    mapping_data = mappings.get(episode_url)
    if isinstance(mapping_data, list) and len(mapping_data) > 0:
        return jsonify({"real_url": mapping_data[0],
                        "title": mapping_data[1] if len(mapping_data) > 1 else ""})
    return jsonify({"error": "URL not found in mapping"}), 404


@app.route('/api/OVideo/categories', methods=['GET'])
def ovideo_categories():
    ensure_video_db()
    try:
        conn = _get_video_conn()
        try:
            r = conn.execute("SELECT value FROM meta WHERE key='categories'").fetchone()
        finally:
            conn.close()
        cats = json.loads(r['value']) if r else ["Movie", "Drama", "Show", "Anime"]
    except Exception:
        cats = ["Movie", "Drama", "Show", "Anime"]
    # "精选"置顶，作为默认页
    cats = ["Featured"] + [c for c in cats if c != "Featured"]
    return jsonify({"categories": cats})


@app.route('/api/OVideo/list', methods=['GET'])
def ovideo_list():
    ensure_video_db()
    user_id = request.args.get('user_id')
    if user_id and user_id in VIDEO_MODULE_BLOCKED_USERS:
        return jsonify({"items": [], "has_more": False, "page": 0})

    category = request.args.get('category', 'Movie')
    sort = request.args.get('sort', 'update')
    page = _arg_int('page', 0, 0, 100000)
    page_size = _arg_int('page_size', 24, 1, 60)

    where, params = [], []
    if category == 'Featured':
        pass                                   # 不加分类条件（包括 Show）
    elif category == 'Documentary':
        where.append("has_documentary=1")
    else:
        where.append("category=?"); params.append(category)

    where.append("hide_blacklisted=0")
    region_kw, type_kw = _get_block_config(user_id)
    bc, bp = _block_where(region_kw, type_kw)
    where += bc; params += bp

    max_year = _parse_max_year()
    if max_year is not None:
        where.append("release_year IS NOT NULL AND release_year <= ?")
        params.append(max_year)

    # （未启用）Featured 按上映日期排序时 Drama 改用 update − N 天：
    # if category == 'Featured' and sort == 'date':
    #     n = int(FEATURED_DRAMA_DATE_OFFSET_DAYS)
    #     order = (f"CASE WHEN category='Drama' THEN date(update_sort_key, '-{n} days') "
    #              f"ELSE release_sort_key END DESC")
    order = _order_clause(sort)

    sql = (f"SELECT category, item_json FROM videos WHERE {' AND '.join(where)} "
           f"ORDER BY {order} LIMIT ? OFFSET ?")
    params += [page_size + 1, page * page_size]
    conn = _get_video_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    has_more = len(rows) > page_size
    items = []
    for r in rows[:page_size]:
        it = json.loads(r['item_json'])
        it['category'] = r['category']
        items.append(it)
    return jsonify({"items": items, "has_more": has_more, "page": page})


@app.route('/api/OVideo/filter', methods=['GET'])
def ovideo_filter():
    ensure_video_db()
    user_id = request.args.get('user_id')
    if user_id and user_id in VIDEO_MODULE_BLOCKED_USERS:
        return jsonify({"items": [], "has_more": False, "page": 0})

    category = request.args.get('category')
    f_type = request.args.get('type')
    f_year = request.args.get('year')
    f_region = request.args.get('region')
    sort = request.args.get('sort', 'update')
    page = _arg_int('page', 0, 0, 100000)
    page_size = _arg_int('page_size', 24, 1, 60)

    where, params = [], []
    if category:
        if category == 'Documentary':
            where.append("has_documentary=1")
        else:
            where.append("category=?"); params.append(category)
    if f_type:
        where.append("norm_types LIKE ?"); params.append(f"%|{f_type}|%")
    if f_year:
        try:
            where.append("release_year=?"); params.append(int(f_year))
        except (TypeError, ValueError):
            where.pop()
    if f_region:
        where.append("norm_region=?"); params.append(f_region)

    where.append("hide_blacklisted=0")
    region_kw, type_kw = _get_block_config(user_id)
    bc, bp = _block_where(region_kw, type_kw)
    where += bc; params += bp

    max_year = _parse_max_year()
    if max_year is not None:
        where.append("release_year IS NOT NULL AND release_year <= ?")
        params.append(max_year)

    sql = (f"SELECT item_json FROM videos WHERE {' AND '.join(where)} "
           f"ORDER BY {_order_clause(sort)} LIMIT ? OFFSET ?")
    params += [page_size + 1, page * page_size]
    conn = _get_video_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    has_more = len(rows) > page_size
    items = [json.loads(r['item_json']) for r in rows[:page_size]]
    return jsonify({"items": items, "has_more": has_more, "page": page})


@app.route('/api/OVideo/filter_options', methods=['GET'])
def ovideo_filter_options():
    ensure_video_db()
    user_id = request.args.get('user_id')
    if user_id and user_id in VIDEO_MODULE_BLOCKED_USERS:
        return jsonify({"types": [], "years": [], "regions": []})

    region_kw, type_kw = _get_block_config(user_id)
    bc, bp = _block_where(region_kw, type_kw)
    wsql = " WHERE " + " AND ".join(["hide_blacklisted=0"] + bc)

    conn = _get_video_conn()
    try:
        type_set = set()
        for r in conn.execute(f"SELECT norm_types FROM videos{wsql}", bp):
            for t in (r['norm_types'] or '').split('|'):
                if t and t != '纪录片':
                    type_set.add(t)
        years = sorted({r['release_year'] for r in
                        conn.execute(f"SELECT DISTINCT release_year FROM videos{wsql}", bp)
                        if r['release_year'] is not None}, reverse=True)
        regions = sorted({r['norm_region'] for r in
                          conn.execute(f"SELECT DISTINCT norm_region FROM videos{wsql}", bp)
                          if r['norm_region'] and r['norm_region'] != '其它'})
    finally:
        conn.close()
    return jsonify({"types": sorted(type_set), "years": years, "regions": regions})


# ---------------- §3.7 搜索（子串 + 模糊） ----------------
@app.route('/api/OVideo/search2', methods=['GET'])
def ovideo_search2():
    ensure_video_db()
    user_id = request.args.get('user_id')
    if user_id and user_id in VIDEO_MODULE_BLOCKED_USERS:
        return jsonify({"items": []})

    q = request.args.get('q', '').strip().lower()
    if not q:
        return jsonify({"items": []})
    qn = q.replace('·', '').replace(' ', '')
    if not qn:
        qn = q
    limit = _arg_int('limit', 150, 1, 300)

    region_kw, type_kw = _get_block_config(user_id)
    bc, bp = _block_where(region_kw, type_kw)
    base_where = ["hide_blacklisted=0"] + bc
    base_params = list(bp)
    max_year_int = _parse_max_year()
    if max_year_int is not None:
        base_where.append("release_year IS NOT NULL AND release_year <= ?")
        base_params.append(max_year_int)

    cat_order = {"Movie": 0, "Drama": 1, "Show": 2, "Anime": 3}
    conn = _get_video_conn()
    try:
        # ---------- 阶段 1：子串匹配 ----------
        like, like_n = f"%{q}%", f"%{qn}%"
        cond = ("(name_lower LIKE ? OR name_norm LIKE ? OR alias_lower LIKE ? OR alias_norm LIKE ? "
                "OR types_lower LIKE ? OR director_lower LIKE ? OR director_norm LIKE ? "
                "OR cast_lower LIKE ? OR cast_norm LIKE ? OR intro_lower LIKE ?)")
        exact_params = [like, like_n, like, like_n, like, like, like_n, like, like_n, like]
        sql1 = ("SELECT url, category, item_json, best_rating, "
                "name_lower, name_norm, alias_lower, alias_norm, types_lower, "
                "director_lower, director_norm, cast_lower, cast_norm, intro_lower, "
                "release_sort_key, update_sort_key "
                f"FROM videos WHERE {' AND '.join([cond] + base_where)}")
        exact_rows = conn.execute(sql1, exact_params + base_params).fetchall()

        # 匹配层级 mp（越小越优先）：
        #   完全匹配：0 名称 / 1 又名 / 2 导演 / 3 演员(前2) / 4 演员(3+)
        #   部分匹配：5 名称 / 6 又名 / 7 导演 / 8 演员(前2) / 9 演员(3+) / 10 简介 / 11 类型/其它
        #   模糊匹配：13 名称 / 14 又名 / 15 导演 / 16 演员(前2) / 17 演员(3+)
        BIG = 10 ** 6

        def classify(r):
            name, name_n = r['name_lower'] or '', r['name_norm'] or ''
            alias, alias_n = r['alias_lower'] or '', r['alias_norm'] or ''
            director, director_n = r['director_lower'] or '', r['director_norm'] or ''
            cast_list = (r['cast_lower'] or '').split('\x1f')
            cast_list_n = (r['cast_norm'] or '').split('\x1f')
            intro = r['intro_lower'] or ''

            def cast_exact_pos():
                idx = BIG
                for i, c in enumerate(cast_list):
                    if c == q:
                        idx = min(idx, i); break
                for i, c in enumerate(cast_list_n):
                    if c == qn:
                        idx = min(idx, i); break
                return idx

            def cast_contains_pos():
                idx = BIG
                for i, c in enumerate(cast_list):
                    if c and q in c:
                        idx = min(idx, i); break
                for i, c in enumerate(cast_list_n):
                    if c and qn in c:
                        idx = min(idx, i); break
                return idx

            # 0 片名完全一致 / 0.5 主名称一致 / 1 又名完全一致 / 1.5 又名主名称一致
            if name == q or name_n == qn:
                return 0, BIG
            if qn in _series_main_keys(name):
                return 0.5, BIG
            alias_parts = [a.strip() for a in re.split(r'[/、,，]', alias) if a.strip()]
            if alias == q or alias_n == qn or any(_norm_search(a) == qn for a in alias_parts):
                return 1, BIG
            if any(qn in _series_main_keys(a) for a in alias_parts):
                return 1.5, BIG
            if director == q or director_n == qn:
                return 2, BIG
            cp = cast_exact_pos()
            if cp != BIG:
                return (3, cp) if cp <= 1 else (4, cp)
            if q in name or qn in name_n:
                return 5, BIG
            if q in alias or qn in alias_n:
                return 6, BIG
            if q in director or qn in director_n:
                return 7, BIG
            cp = cast_contains_pos()
            if cp != BIG:
                return (8, cp) if cp <= 1 else (9, cp)
            if q in intro:
                return 10, BIG
            return 11, BIG

        results_map = {}
        for r in exact_rows:
            mp, cast_pos = classify(r)
            results_map[r['url']] = {
                "url": r['url'], "item_json": r['item_json'],
                "mp": mp, "cast_pos": cast_pos, "fuzzy": 1.0,
                "namelen": len(r['name_lower'] or ''),
                "rating": r['best_rating'] or 0.0,
                "rel": r['release_sort_key'] or "",
                "cat": cat_order.get(r['category'], 4),
                "upd": r['update_sort_key'] or "",
            }

        # ---------- 阶段 2：模糊匹配（内存缓存 + 字符集合预筛） ----------
        FUZZY_TRIGGER = 8
        FUZZY_THRESHOLD = 0.6
        PREFILTER_OVERLAP = 0.6
        if len(results_map) < FUZZY_TRIGGER and len(qn) >= 2:
            candidates = _get_fuzzy_candidates()
            q_set = set(qn)
            qn_len = len(q_set)
            region_block = set(region_kw)
            sm = SequenceMatcher(None)
            sm.set_seq2(qn)                     # 查询词固定为 seq2，索引只算一次

            def overlap(text_set):
                if not text_set or not q_set:
                    return 0.0
                return len(q_set & text_set) / qn_len

            def fuzzy_score(text):
                if not text:
                    return 0.0
                if qn in text:
                    return 1.0
                sm.set_seq1(text)
                whole = sm.ratio() if sm.quick_ratio() >= FUZZY_THRESHOLD else 0.0
                lq, lt = len(qn), len(text)
                best_local = 0.0
                if lt > lq:
                    for i in range(lt - lq + 1):
                        sm.set_seq1(text[i:i + lq])
                        if sm.quick_ratio() < FUZZY_THRESHOLD:
                            continue
                        s = sm.ratio()
                        if s > best_local:
                            best_local = s
                            if best_local >= 0.99:
                                break
                return max(whole, best_local)

            for r in candidates:
                if r['url'] in results_map:
                    continue
                # 在 Python 里复现 SQL base_where 的过滤
                if r['hide'] != 0:
                    continue
                if region_block and r['region'] in region_block:
                    continue
                if type_kw and any(kw in r['raw_types'] for kw in type_kw):
                    continue
                if max_year_int is not None:
                    ry = r['release_year']
                    if ry is None or ry > max_year_int:
                        continue

                best_mp, best_score, best_cast_pos = None, 0.0, BIG
                if overlap(r['name_set']) >= PREFILTER_OVERLAP:
                    s = fuzzy_score(r['name'])
                    if s >= FUZZY_THRESHOLD:
                        best_mp, best_score = 13, s
                if best_mp is None and overlap(r['alias_set']) >= PREFILTER_OVERLAP:
                    s = fuzzy_score(r['alias'])
                    if s >= FUZZY_THRESHOLD:
                        best_mp, best_score = 14, s
                if best_mp is None and overlap(r['director_set']) >= PREFILTER_OVERLAP:
                    s = fuzzy_score(r['director'])
                    if s >= FUZZY_THRESHOLD:
                        best_mp, best_score = 15, s
                if best_mp is None:
                    for i, cset in enumerate(r['cast_sets']):
                        if overlap(cset) < PREFILTER_OVERLAP:
                            continue
                        s = fuzzy_score(r['cast_list'][i])
                        if s >= FUZZY_THRESHOLD and s > best_score:
                            best_score = s
                            best_cast_pos = i
                    if best_cast_pos != BIG:
                        best_mp = 16 if best_cast_pos <= 1 else 17

                if best_mp is not None:
                    results_map[r['url']] = {
                        "url": r['url'], "item_json": None,
                        "mp": best_mp, "cast_pos": best_cast_pos, "fuzzy": best_score,
                        "namelen": r['name_len'], "rating": r['rating'],
                        "rel": r['rel'], "cat": cat_order.get(r['category'], 4), "upd": r['upd'],
                    }

        # 为模糊命中的项批量补 item_json
        missing = [u for u, v in results_map.items() if v['item_json'] is None]
        CHUNK = 400
        for i in range(0, len(missing), CHUNK):
            chunk = missing[i:i + CHUNK]
            ph = ",".join("?" * len(chunk))
            for r in conn.execute(f"SELECT url, item_json FROM videos WHERE url IN ({ph})", chunk):
                if r['url'] in results_map:
                    results_map[r['url']]['item_json'] = r['item_json']
    finally:
        conn.close()

    # 排序（多趟稳定排序：从最次要到最主要）
    # 显著性：mp > 上映日期 > 评分 > 短名 > 模糊分 > 演员位次 > 分类 > 更新时间
    scored = list(results_map.values())
    scored.sort(key=lambda x: x['upd'], reverse=True)
    scored.sort(key=lambda x: x['cat'])
    scored.sort(key=lambda x: x['cast_pos'])
    scored.sort(key=lambda x: x['fuzzy'], reverse=True)
    scored.sort(key=lambda x: x['namelen'])
    scored.sort(key=lambda x: x['rating'], reverse=True)
    scored.sort(key=lambda x: x['rel'], reverse=True)
    scored.sort(key=lambda x: x['mp'])

    items = [json.loads(s['item_json']) for s in scored[:limit] if s['item_json']]
    return jsonify({"items": items, "has_more": False, "page": 0})


# ---------------- §3.8 举报 / 寻片（写入统一会话 §2B） ----------------
@app.route('/api/OVideo/report', methods=['POST'])
def report_video_link():
    data = _json_body()
    user_id = data.get('user_id')
    source_url = data.get('source_url')
    episode_url = data.get('episode_url')
    if not user_id or not (source_url or episode_url):
        return jsonify({"error": "Invalid params"}), 400
    report_type = data.get('report_type', 'other')
    if report_type not in ALLOWED_REPORT_TYPES:
        report_type = 'other'
    if _too_frequent(report_last_time, user_id, 10):
        return jsonify({"error": "Too frequent"}), 429

    video_title = data.get('video_title', '')
    channel_name = data.get('channel_name', '')
    episode_name = data.get('episode_name', '')
    real_url = data.get('real_url', '')
    note = (data.get('note', '') or '')[:500]
    app_version = data.get('app_version', '')
    now = now_iso()

    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO video_link_reports
                (user_id, video_title, source_url, episode_url, channel_name,
                 episode_name, real_url, report_type, note, app_version,
                 first_at, last_at, count, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'pending')
            ON CONFLICT(user_id, episode_url, report_type)
            DO UPDATE SET last_at=?, count=count+1, note=excluded.note, status='pending'
        ''', (user_id, video_title, source_url, episode_url, channel_name,
              episode_name, real_url, report_type, note, app_version, now, now, now))

        tk = support_thread_key('ONews', 'report', user_id, episode_url)
        sub = " · ".join([x for x in [channel_name, episode_name] if x])
        support_upsert_thread(c, 'ONews', tk, 'report', user_id, data.get('user_type'),
                              title=(video_title or '(未知影片)'), subtitle=sub,
                              ref_id=episode_url, app_version=app_version)
        body = f"报错反馈：{REPORT_TYPE_NAMES.get(report_type, report_type)}"
        if sub:
            body += f"（{sub}）"
        if note:
            body += f"\n补充说明：{note}"
        support_add_message(c, 'ONews', tk, 'user', body)
        conn.commit()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/OVideo/wish', methods=['POST'])
def submit_video_wish():
    data = _json_body()
    user_id = data.get('user_id')
    wish_content = (data.get('wish_content', '') or '').strip()[:200]
    if not user_id or not wish_content:
        return jsonify({"error": "Invalid params"}), 400
    if _too_frequent(wish_last_time, user_id, 10):
        return jsonify({"error": "Too frequent"}), 429

    user_type = _norm_user_type(user_id, data.get('user_type'))
    keyword = (data.get('keyword', '') or '')[:100]
    app_version = data.get('app_version', '')
    now = now_iso()

    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO video_wish_requests
                (user_id, user_type, keyword, wish_content, app_version,
                 first_at, last_at, count, status, reply_status)
            VALUES (?,?,?,?,?,?,?,1,'pending','none')
            ON CONFLICT(user_id, wish_content)
            DO UPDATE SET last_at=?, count=count+1, status='pending', keyword=excluded.keyword
        ''', (user_id, user_type, keyword, wish_content, app_version, now, now, now))
        row = c.execute("SELECT id FROM video_wish_requests WHERE user_id=? AND wish_content=?",
                        (user_id, wish_content)).fetchone()
        if row:
            tk = support_thread_key('ONews', 'wish', row['id'])
            support_upsert_thread(c, 'ONews', tk, 'wish', user_id, user_type,
                                  title=wish_content,
                                  subtitle=(f"搜索词: {keyword}" if keyword else ''),
                                  ref_id=row['id'], app_version=app_version)
            support_add_message(c, 'ONews', tk, 'user', f"我想看：{wish_content}")
        conn.commit()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


def _my_unread_replies(sql, user_id):
    if not user_id:
        return jsonify({"replies": []})
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        return jsonify({"replies": [dict(r) for r in conn.execute(sql, (user_id,)).fetchall()]})
    finally:
        conn.close()


def _ack_reply(table):
    data = _json_body()
    user_id = data.get('user_id')
    reply_id = data.get('id')
    if not user_id or not reply_id:
        return jsonify({"error": "Invalid params"}), 400
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    try:
        conn.execute(f"UPDATE {table} SET reply_status='read' WHERE id=? AND user_id=?",
                     (reply_id, user_id))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"status": "ok"})


@app.route('/api/OVideo/wish/my_replies', methods=['GET'])
def get_my_wish_replies():
    return _my_unread_replies('''
        SELECT id, wish_content, admin_reply, replied_at
        FROM video_wish_requests
        WHERE user_id=? AND reply_status='unread'
          AND admin_reply IS NOT NULL AND admin_reply != ''
        ORDER BY replied_at DESC''', request.args.get('user_id'))


@app.route('/api/OVideo/wish/ack_reply', methods=['POST'])
def ack_wish_reply():
    return _ack_reply('video_wish_requests')


@app.route('/api/OVideo/report/my_replies', methods=['GET'])
def get_my_report_replies():
    return _my_unread_replies('''
        SELECT id, video_title, episode_name, admin_reply, replied_at
        FROM video_link_reports
        WHERE user_id=? AND reply_status='unread'
          AND admin_reply IS NOT NULL AND admin_reply != ''
        ORDER BY replied_at DESC''', request.args.get('user_id'))


@app.route('/api/OVideo/report/ack_reply', methods=['POST'])
def ack_report_reply():
    return _ack_reply('video_link_reports')


# ---------------- §3.9 埋点 / 点数 ----------------
@app.route('/api/OVideo/track', methods=['POST'])
def track_event():
    data = _json_body()
    user_id = data.get('user_id')
    user_type = data.get('user_type', 'apple')
    video_url = data.get('video_url')
    video_title = data.get('video_title', '')
    event_type = data.get('event_type')
    source = data.get('source')
    app_version = data.get('app_version', '')
    episode_key = data.get('episode_key')
    if not user_id or not video_url or event_type not in ALLOWED_EVENT_TYPES:
        return jsonify({"error": "Invalid params"}), 400

    # 权限来源：客户端上报 + 服务器兜底纠正
    access_type = resolve_video_access_type(user_id, data.get('access_type'), episode_key or video_url)
    now = now_iso()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    try:
        c = conn.cursor()
        c.execute('''
            INSERT INTO user_video_events
                (user_id, user_type, video_url, video_title, event_type,
                 first_at, last_at, count, access_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id, video_url, event_type)
            DO UPDATE SET last_at = ?, count = count + 1, access_type = ?
        ''', (user_id, user_type, video_url, video_title, event_type, now, now,
              access_type, now, access_type))
        c.execute('''
            INSERT INTO event_logs
                (user_id, user_type, video_url, video_title, event_type,
                 created_at, source, app_version, access_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, user_type, video_url, video_title, event_type, now,
              source, app_version, access_type))
        conn.commit()
        return jsonify({"status": "ok", "access_type": access_type}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


def _guest_quota_payload(cfg):
    return {
        "daily_quota": cfg['daily_quota'], "used_today": 0,
        "remaining": 0, "daily_remaining": 0,
        "bonus_remaining": 0, "total_remaining": 0,
        "bonus_just_granted": False,
        "unlocked_episodes": [], "unlocked_news": [],
        "invite_code": None, "invite_reward_count": 0,
        "has_redeemed_invite": False,
        "invite_reward_points": cfg['invite_reward_points'],
        "logged_in": False
    }


@app.route('/api/OVideo/quota/status', methods=['GET'])
def video_quota_status():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    cfg = get_video_points_config()
    daily_quota = cfg['daily_quota']
    if not is_real_login_user(user_id):
        return jsonify(_guest_quota_payload(cfg))
    maybe_cleanup_old_unlocks()
    today = today_str()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        row, just_granted = _ensure_points(c, 'video_points', user_id, cfg)
        conn.commit()
        bonus_remaining = row['bonus_remaining']
        episodes = [r['episode_key'] for r in c.execute(
            "SELECT episode_key FROM video_free_unlocks WHERE user_id=? AND unlock_date=?",
            (user_id, today)).fetchall()]
        daily_used = _video_daily_used(c, user_id, today)
        invite_code = row['invite_code']
        invite_reward_count = row['invite_reward_count']
        has_redeemed = bool(row['invited_by_code'])
    finally:
        conn.close()
    daily_remaining = max(0, daily_quota - daily_used)
    total = bonus_remaining + daily_remaining
    return jsonify({
        "daily_quota": daily_quota, "used_today": daily_used,
        "remaining": total, "daily_remaining": daily_remaining,
        "bonus_remaining": bonus_remaining, "total_remaining": total,
        "bonus_just_granted": just_granted,
        "unlocked_episodes": episodes, "unlocked_news": [],
        "invite_code": invite_code, "invite_reward_count": invite_reward_count,
        "has_redeemed_invite": has_redeemed,
        "invite_reward_points": cfg['invite_reward_points'], "logged_in": True
    })


@app.route('/api/OVideo/quota/unlock', methods=['POST'])
def video_quota_unlock():
    data = _json_body()
    user_id = data.get('user_id')
    episode_key = data.get('episode_key')
    if not user_id or not episode_key:
        return jsonify({"error": "Missing params"}), 400
    exceeded = {"status": "quota_exceeded", "remaining": 0,
                "bonus_remaining": 0, "daily_remaining": 0, "total_remaining": 0}
    if not is_real_login_user(user_id):
        return jsonify(exceeded)
    cfg = get_video_points_config()
    daily_quota = cfg['daily_quota']
    today, now = today_str(), now_iso()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("BEGIN IMMEDIATE")
        row, _ = _ensure_points(c, 'video_points', user_id, cfg)
        bonus_remaining = row['bonus_remaining']
        du = _video_daily_used(c, user_id, today)
        dr = max(0, daily_quota - du)
        if c.execute("SELECT 1 FROM video_free_unlocks WHERE user_id=? AND episode_key=? AND unlock_date=?",
                     (user_id, episode_key, today)).fetchone():
            c.execute("COMMIT")
            total = bonus_remaining + dr
            return jsonify({"status": "already_unlocked", "remaining": total,
                            "bonus_remaining": bonus_remaining,
                            "daily_remaining": dr, "total_remaining": total})
        if bonus_remaining > 0:
            source = 'bonus'
        elif dr > 0:
            source = 'daily'
        else:
            c.execute("COMMIT")
            return jsonify(exceeded)
        c.execute('''INSERT INTO video_free_unlocks
                     (user_id, episode_key, unlock_date, video_title, created_at, source)
                     VALUES (?,?,?,?,?,?)''',
                  (user_id, episode_key, today, data.get('video_title', ''), now, source))
        if source == 'bonus':
            c.execute("UPDATE video_points SET bonus_remaining=bonus_remaining-1 WHERE user_id=?", (user_id,))
            bonus_remaining -= 1
        else:
            dr = max(0, dr - 1)
        c.execute("COMMIT")
        total = bonus_remaining + dr
        return jsonify({"status": "success", "remaining": total,
                        "bonus_remaining": bonus_remaining,
                        "daily_remaining": dr, "total_remaining": total})
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------- §3.10 后台：视频统计 / 举报 / 寻片 ----------------
def _period_where(period):
    """today / 7d / all -> SQL 片段（固定常量，无注入）"""
    if period == 'today':
        return "AND date(created_at) = date('now', '+8 hours')"
    if period == '7d':
        return "AND created_at >= datetime('now', '+8 hours', '-7 days')"
    return ""


# 后台"用户视频明细"的固定筛选（白名单，绝不拼接外部输入）
VIDEO_DETAIL_FILTERS = {
    'online':  "video_url NOT LIKE '%.m3u8'",
    'offline': "video_url LIKE '%.m3u8'",
    'vip':     "access_type IN ('subscription','vip_permanent')",
    'points':  "access_type LIKE 'points%'",
}


def _video_detail_filter(filter_key, legacy_suffix=''):
    """新版网页传 filter=online|offline|vip|points；
       兼容旧网页（浏览器缓存）传的 suffix SQL 片段：只做识别，不拼接。"""
    key = (filter_key or '').strip().lower()
    if not key and legacy_suffix:
        s = legacy_suffix.lower().replace(' ', '')
        if 'm3u8' in s:
            key = 'online' if 'notlike' in s else 'offline'
        elif 'subscription' in s or 'vip_permanent' in s:
            key = 'vip'
        elif 'points' in s:
            key = 'points'
    clause = VIDEO_DETAIL_FILTERS.get(key)
    return f" AND {clause}" if clause else ''


@app.route('/admin/api/video/user_details', methods=['GET'])
@require_admin
def admin_video_user_details():
    maybe_cleanup_old_unlocks()
    user_id = request.args.get('user_id')
    event_type = request.args.get('type')
    if not user_id or not event_type:
        return jsonify({"error": "Missing parameters"}), 400
    extra = _video_detail_filter(request.args.get('filter'), request.args.get('suffix', ''))
    sql = f'''
        SELECT date(created_at) AS day, video_url, video_title,
               MAX(created_at) AS last_time,
               COUNT(*) AS click_count,
               GROUP_CONCAT(DISTINCT source) AS sources,
               GROUP_CONCAT(DISTINCT NULLIF(app_version,'')) AS versions,
               GROUP_CONCAT(DISTINCT COALESCE(NULLIF(access_type,''),'unknown')) AS access_types
        FROM event_logs
        WHERE user_id = ? AND event_type = ? AND created_at >= ? {extra}
        GROUP BY day, video_url
        ORDER BY day DESC, last_time DESC
    '''
    return jsonify(_query_analytics(sql, (user_id, event_type, analytics_cutoff_iso())))


@app.route('/admin/api/overview', methods=['GET'])
@require_admin
def admin_overview():
    today = today_str()
    q = lambda sql, p=(): _query_analytics(sql, p)[0]['c']
    return jsonify({
        "total_users":           q("SELECT COUNT(DISTINCT user_id) AS c FROM event_logs"),
        "total_play_events":     q("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play'"),
        "total_download_events": q("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='download_complete'"),
        "today_active_users":    q("SELECT COUNT(DISTINCT user_id) AS c FROM event_logs WHERE date(created_at)=?", (today,)),
        "today_play":            q("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play' AND date(created_at)=?", (today,)),
        "today_download":        q("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='download_complete' AND date(created_at)=?", (today,)),
        "pending_reports":       q("SELECT COUNT(DISTINCT episode_url) AS c FROM video_link_reports WHERE status='pending'"),
        "today_play_vip":        q("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play' AND access_type IN ('subscription','vip_permanent') AND date(created_at)=?", (today,)),
        "today_play_points":     q("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play' AND access_type LIKE 'points%' AND date(created_at)=?", (today,)),
    })


@app.route('/admin/api/top_videos', methods=['GET'])
@require_admin
def admin_top_videos():
    event_type = request.args.get('type', 'play')
    where_time = _period_where(request.args.get('period', 'all'))
    limit = _arg_int('limit', 20, 1, 500)
    sql = f'''
        SELECT video_url, video_title,
               COUNT(DISTINCT user_id) AS unique_users,
               COUNT(*) AS total_count
        FROM event_logs
        WHERE event_type = ? {where_time}
        GROUP BY video_url
        ORDER BY unique_users DESC, total_count DESC
        LIMIT ?
    '''
    return jsonify(_query_analytics(sql, (event_type, limit)))


@app.route('/admin/api/video_users', methods=['GET'])
@require_admin
def admin_video_users():
    return jsonify(_query_analytics('''
        SELECT user_id, first_at, last_at, count
        FROM user_video_events
        WHERE video_url = ? AND event_type = ?
        ORDER BY last_at DESC
    ''', (request.args.get('video_url'), request.args.get('type', 'play'))))


@app.route('/admin/api/daily_trend', methods=['GET'])
@require_admin
def admin_daily_trend():
    return jsonify(_query_analytics('''
        SELECT date(created_at) AS day, event_type,
               COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS uu
        FROM event_logs
        WHERE created_at >= datetime('now', '+8 hours', '-30 days')
        GROUP BY day, event_type
        ORDER BY day ASC
    '''))


@app.route('/admin/api/video_reports', methods=['GET'])
@require_admin
def admin_video_reports():
    where = "WHERE status='pending'" if request.args.get('status', 'pending') == 'pending' else ""
    return jsonify(_query_analytics(f'''
        SELECT video_title, source_url, episode_url, channel_name, episode_name,
               report_type,
               MAX(real_url) AS real_url,
               COUNT(DISTINCT user_id) AS unique_users,
               GROUP_CONCAT(DISTINCT user_id) AS user_ids,
               SUM(count) AS total_count,
               MAX(last_at) AS last_at,
               GROUP_CONCAT(DISTINCT NULLIF(note,'')) AS notes,
               MAX(admin_reply) AS admin_reply,
               MAX(reply_status) AS reply_status
        FROM video_link_reports
        {where}
        GROUP BY episode_url, report_type
        ORDER BY unique_users DESC, total_count DESC
        LIMIT 200
    '''))


@app.route('/admin/api/resolve_report', methods=['POST'])
@require_admin
def admin_resolve_report():
    """reply 留空=仅标记已处理；填写=回复举报该集的所有用户"""
    data = _json_body()
    episode_url = data.get('episode_url')
    reply = (data.get('reply', '') or '').strip()[:1000]
    if not episode_url:
        return jsonify({"error": "Missing episode_url"}), 400
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        if reply:
            c.execute('''UPDATE video_link_reports
                         SET status='resolved', admin_reply=?, reply_status='unread', replied_at=?
                         WHERE episode_url=?''', (reply, now_iso(), episode_url))
            users = c.execute('''SELECT user_id, MAX(video_title) AS vt,
                                        MAX(channel_name) AS cn, MAX(episode_name) AS en
                                 FROM video_link_reports WHERE episode_url=? GROUP BY user_id''',
                              (episode_url,)).fetchall()
            for u in users:
                tk = support_thread_key('ONews', 'report', u['user_id'], episode_url)
                sub = " · ".join([x for x in [u['cn'], u['en']] if x])
                support_upsert_thread(c, 'ONews', tk, 'report', u['user_id'], None,
                                      title=(u['vt'] or '(未知影片)'), subtitle=sub, ref_id=episode_url)
                support_add_message(c, 'ONews', tk, 'admin', reply)
        else:
            c.execute("UPDATE video_link_reports SET status='resolved' WHERE episode_url=?", (episode_url,))
        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/admin/api/video_wishes', methods=['GET'])
@require_admin
def admin_video_wishes():
    where = "WHERE status='pending'" if request.args.get('status', 'pending') == 'pending' else ""
    return jsonify(_query_analytics(f'''
        SELECT id, user_id, user_type, keyword, wish_content, app_version,
               count, first_at, last_at, status, admin_reply, reply_status
        FROM video_wish_requests
        {where}
        ORDER BY (status='pending') DESC, last_at DESC
        LIMIT 200
    '''))


@app.route('/admin/api/resolve_wish', methods=['POST'])
@require_admin
def admin_resolve_wish():
    """reply 留空=仅标记已处理；填写=回复用户"""
    data = _json_body()
    wish_id = data.get('id')
    reply = (data.get('reply', '') or '').strip()[:1000]
    if not wish_id:
        return jsonify({"error": "Missing id"}), 400
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        if reply:
            c.execute('''UPDATE video_wish_requests
                         SET status='resolved', admin_reply=?, reply_status='unread', replied_at=?
                         WHERE id=?''', (reply, now_iso(), wish_id))
            w = c.execute("SELECT * FROM video_wish_requests WHERE id=?", (wish_id,)).fetchone()
            if w:
                tk = support_thread_key('ONews', 'wish', w['id'])
                support_upsert_thread(c, 'ONews', tk, 'wish', w['user_id'], w['user_type'],
                                      title=w['wish_content'],
                                      subtitle=(f"搜索词: {w['keyword']}" if w['keyword'] else ''),
                                      ref_id=w['id'])
                support_add_message(c, 'ONews', tk, 'admin', reply)
        else:
            c.execute("UPDATE video_wish_requests SET status='resolved' WHERE id=?", (wish_id,))
        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/admin/api/top_users', methods=['GET'])
@require_admin
def admin_top_users():
    return jsonify(_query_analytics('''
        SELECT user_id,
               MAX(user_type) AS user_type,
               COUNT(DISTINCT CASE WHEN event_type='play' THEN video_url END) AS play_videos,
               COUNT(DISTINCT CASE WHEN event_type='download_complete' THEN video_url END) AS download_videos,
               COUNT(DISTINCT CASE WHEN event_type='play' AND video_url NOT LIKE '%.m3u8' THEN video_url END) AS online_play,
               COUNT(DISTINCT CASE WHEN event_type='play' AND video_url LIKE '%.m3u8' THEN video_url END) AS offline_play,
               COUNT(DISTINCT CASE WHEN event_type='play'
                     AND access_type IN ('subscription','vip_permanent') THEN video_url END) AS vip_play,
               COUNT(DISTINCT CASE WHEN event_type='play'
                     AND access_type LIKE 'points%' THEN video_url END) AS points_play,
               COUNT(*) AS total_actions,
               MAX(created_at) AS last_active
        FROM event_logs
        GROUP BY user_id
        ORDER BY total_actions DESC
        LIMIT 50
    '''))


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §4 ONews 新闻（独享）                                                 ║
# ║      依赖：§1 + §2A（点数/邀请）；表：user_news_events / news_event_logs ║
# ║            / news_free_unlocks                                        ║
# ╚══════════════════════════════════════════════════════════════════════╝

ALLOWED_NEWS_EVENT_TYPES = {'view', 'listen'}


def get_news_points_config():
    data = _load_version_json('ONews')
    try:
        q = data.get('news_free_quota', {}) or {}
        enabled = bool(q.get('enabled', True))
        return {
            'daily_quota': int(q.get('daily_count', 5)) if enabled else 0,
            'first_login_bonus': int(q.get('first_login_bonus', 18)) if enabled else 0,
            'invite_reward_points': int(data.get('news_invite_reward_points', 28)),
        }
    except Exception as e:
        print(f"读取新闻点数配置失败: {e}")
        return {'daily_quota': 5, 'first_login_bonus': 18, 'invite_reward_points': 28}


def resolve_news_access_type(user_id, reported=None):
    """新闻：订阅 / 点数 / 免费(老新闻)"""
    kind = get_subscription_kind(user_id, 'ONews')
    if kind:
        return kind
    r = (reported or '').strip()
    return r if r in ALLOWED_ACCESS_TYPES else 'unknown'


def init_onews_analytics_db():
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_news_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',
            article_key TEXT NOT NULL,        -- source_id|topic 的稳定键
            article_topic TEXT,
            source_id TEXT,
            article_date TEXT,                -- 文章 yyMMdd
            event_type TEXT NOT NULL,         -- view/listen
            first_at TIMESTAMP NOT NULL,
            last_at TIMESTAMP NOT NULL,
            count INTEGER DEFAULT 1,
            UNIQUE(user_id, article_key, event_type)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS news_event_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',
            article_key TEXT NOT NULL,
            article_topic TEXT,
            source_id TEXT,
            article_date TEXT,
            event_type TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    ''')
    for ddl in ("ALTER TABLE news_event_logs ADD COLUMN app_version TEXT",
                "ALTER TABLE news_event_logs ADD COLUMN access_type TEXT",
                "ALTER TABLE user_news_events ADD COLUMN access_type TEXT"):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError:
            pass
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_time ON news_event_logs(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_source ON news_event_logs(source_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_type ON news_event_logs(event_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_user ON news_event_logs(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_access ON news_event_logs(access_type)')
    # 新闻解锁表（同一篇解锁后永久免费；每日消耗按 unlock_date 计）
    c.execute('''
        CREATE TABLE IF NOT EXISTS news_free_unlocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            article_key TEXT NOT NULL,
            unlock_date TEXT NOT NULL,
            article_topic TEXT,
            source TEXT DEFAULT 'daily',
            created_at TIMESTAMP NOT NULL,
            UNIQUE(user_id, article_key)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_unlock_user ON news_free_unlocks(user_id, unlock_date)')
    conn.commit()
    conn.close()


@app.route('/api/ONews/track', methods=['POST'])
def track_news_event():
    data = _json_body()
    user_id = data.get('user_id')
    user_type = data.get('user_type', 'apple')
    article_key = data.get('article_key')
    article_topic = data.get('article_topic', '')
    source_id = data.get('source_id', '')
    article_date = data.get('article_date', '')
    event_type = data.get('event_type')
    app_version = data.get('app_version', '')
    if not user_id or not article_key or event_type not in ALLOWED_NEWS_EVENT_TYPES:
        return jsonify({"error": "Invalid params"}), 400

    access_type = resolve_news_access_type(user_id, data.get('access_type'))
    now = now_iso()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    try:
        c = conn.cursor()
        c.execute('''
            INSERT INTO user_news_events
                (user_id, user_type, article_key, article_topic, source_id,
                 article_date, event_type, first_at, last_at, count, access_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id, article_key, event_type)
            DO UPDATE SET last_at = ?, count = count + 1, access_type = ?
        ''', (user_id, user_type, article_key, article_topic, source_id,
              article_date, event_type, now, now, access_type, now, access_type))
        c.execute('''
            INSERT INTO news_event_logs
                (user_id, user_type, article_key, article_topic, source_id,
                 article_date, event_type, created_at, app_version, access_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, user_type, article_key, article_topic, source_id,
              article_date, event_type, now, app_version, access_type))
        conn.commit()
        return jsonify({"status": "ok", "access_type": access_type}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/ONews/quota/status', methods=['GET'])
def news_quota_status():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    cfg = get_news_points_config()
    daily_quota = cfg['daily_quota']
    if not is_real_login_user(user_id):
        return jsonify(_guest_quota_payload(cfg))
    maybe_cleanup_old_unlocks()
    today = today_str()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        row, just_granted = _ensure_points(c, 'news_points', user_id, cfg, migrate_from='onews_points')
        conn.commit()
        bonus_remaining = row['bonus_remaining']
        news_keys = [r['article_key'] for r in c.execute(
            "SELECT article_key FROM news_free_unlocks WHERE user_id=?", (user_id,)).fetchall()]
        daily_used = _news_daily_used(c, user_id, today)
        invite_code = row['invite_code']
        invite_reward_count = row['invite_reward_count']
        has_redeemed = bool(row['invited_by_code'])
    finally:
        conn.close()
    daily_remaining = max(0, daily_quota - daily_used)
    total = bonus_remaining + daily_remaining
    return jsonify({
        "daily_quota": daily_quota, "used_today": daily_used,
        "remaining": total, "daily_remaining": daily_remaining,
        "bonus_remaining": bonus_remaining, "total_remaining": total,
        "bonus_just_granted": just_granted,
        "unlocked_episodes": [], "unlocked_news": news_keys,
        "invite_code": invite_code, "invite_reward_count": invite_reward_count,
        "has_redeemed_invite": has_redeemed,
        "invite_reward_points": cfg['invite_reward_points'], "logged_in": True
    })


@app.route('/api/ONews/quota/unlock', methods=['POST'])
def news_quota_unlock():
    data = _json_body()
    user_id = data.get('user_id')
    article_key = data.get('article_key')
    if not user_id or not article_key:
        return jsonify({"error": "Missing params"}), 400
    exceeded = {"status": "quota_exceeded", "remaining": 0,
                "bonus_remaining": 0, "daily_remaining": 0, "total_remaining": 0}
    if not is_real_login_user(user_id):
        return jsonify(exceeded)
    cfg = get_news_points_config()
    daily_quota = cfg['daily_quota']
    today, now = today_str(), now_iso()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("BEGIN IMMEDIATE")
        row, _ = _ensure_points(c, 'news_points', user_id, cfg, migrate_from='onews_points')
        bonus_remaining = row['bonus_remaining']
        du = _news_daily_used(c, user_id, today)
        dr = max(0, daily_quota - du)
        if c.execute("SELECT 1 FROM news_free_unlocks WHERE user_id=? AND article_key=?",
                     (user_id, article_key)).fetchone():
            c.execute("COMMIT")
            total = bonus_remaining + dr
            return jsonify({"status": "already_unlocked", "remaining": total,
                            "bonus_remaining": bonus_remaining,
                            "daily_remaining": dr, "total_remaining": total})
        if bonus_remaining > 0:
            source = 'bonus'
        elif dr > 0:
            source = 'daily'
        else:
            c.execute("COMMIT")
            return jsonify(exceeded)
        c.execute('''INSERT OR IGNORE INTO news_free_unlocks
                     (user_id, article_key, unlock_date, article_topic, source, created_at)
                     VALUES (?,?,?,?,?,?)''',
                  (user_id, article_key, today, data.get('article_topic', ''), source, now))
        if source == 'bonus':
            c.execute("UPDATE news_points SET bonus_remaining=bonus_remaining-1 WHERE user_id=?", (user_id,))
            bonus_remaining -= 1
        else:
            dr = max(0, dr - 1)
        c.execute("COMMIT")
        total = bonus_remaining + dr
        return jsonify({"status": "success", "remaining": total,
                        "bonus_remaining": bonus_remaining,
                        "daily_remaining": dr, "total_remaining": total})
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---- 后台：新闻统计 ----
@app.route('/admin/api/news/overview', methods=['GET'])
@require_admin
def admin_news_overview():
    today = today_str()
    q = lambda sql, p=(): _query_analytics(sql, p)[0]['c']
    return jsonify({
        "total_users":  q("SELECT COUNT(DISTINCT user_id) c FROM news_event_logs"),
        "total_view":   q("SELECT COUNT(*) c FROM news_event_logs WHERE event_type='view'"),
        "total_listen": q("SELECT COUNT(*) c FROM news_event_logs WHERE event_type='listen'"),
        "today_active": q("SELECT COUNT(DISTINCT user_id) c FROM news_event_logs WHERE date(created_at)=?", (today,)),
        "today_listen": q("SELECT COUNT(*) c FROM news_event_logs WHERE event_type='listen' AND date(created_at)=?", (today,)),
        "today_view":   q("SELECT COUNT(*) c FROM news_event_logs WHERE event_type='view' AND date(created_at)=?", (today,)),
    })


@app.route('/admin/api/news/top_sources', methods=['GET'])
@require_admin
def admin_top_sources():
    where = _period_where(request.args.get('period', '7d'))
    return jsonify(_query_analytics(f'''
        SELECT source_id,
               COUNT(DISTINCT user_id) AS unique_users,
               COUNT(DISTINCT article_key) AS unique_articles,
               COUNT(*) AS total_reads
        FROM news_event_logs
        WHERE event_type IN ('listen', 'view') {where}
        GROUP BY source_id
        ORDER BY total_reads DESC
    '''))


@app.route('/admin/api/news/top_articles', methods=['GET'])
@require_admin
def admin_top_articles():
    event_type = request.args.get('type', 'listen')
    where = _period_where(request.args.get('period', '7d'))
    return jsonify(_query_analytics(f'''
        SELECT article_key, article_topic, source_id,
               COUNT(DISTINCT user_id) AS unique_users,
               COUNT(*) AS total_count
        FROM news_event_logs
        WHERE event_type = ? {where}
        GROUP BY article_key
        ORDER BY unique_users DESC
        LIMIT 30
    ''', (event_type,)))


@app.route('/admin/api/news/daily_trend', methods=['GET'])
@require_admin
def admin_news_daily_trend():
    return jsonify(_query_analytics('''
        SELECT date(created_at) AS day, event_type,
               COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS uu
        FROM news_event_logs
        WHERE created_at >= datetime('now', '+8 hours', '-30 days')
        GROUP BY day, event_type
        ORDER BY day ASC
    '''))


@app.route('/admin/api/news/top_users', methods=['GET'])
@require_admin
def admin_news_top_users():
    return jsonify(_query_analytics('''
        SELECT user_id,
               MAX(user_type) AS user_type,
               COUNT(DISTINCT article_key) AS unique_articles,
               SUM(CASE WHEN event_type='listen' THEN 1 ELSE 0 END) AS listen_count,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) AS view_count,
               MAX(created_at) AS last_active
        FROM news_event_logs
        GROUP BY user_id
        ORDER BY unique_articles DESC
        LIMIT 50
    '''))


@app.route('/admin/api/news/article_users', methods=['GET'])
@require_admin
def admin_news_article_users():
    return jsonify(_query_analytics('''
        SELECT user_id, user_type, first_at, last_at, count
        FROM user_news_events
        WHERE article_key = ? AND event_type = ?
        ORDER BY last_at DESC
    ''', (request.args.get('article_key'), request.args.get('type', 'listen'))))


@app.route('/admin/api/news/user_details', methods=['GET'])
@require_admin
def admin_news_user_details():
    maybe_cleanup_old_unlocks()
    user_id = request.args.get('user_id')
    event_type = request.args.get('type')
    if not user_id or not event_type:
        return jsonify({"error": "Missing parameters"}), 400
    return jsonify(_query_analytics('''
        SELECT article_date, article_topic, source_id,
               MAX(created_at) as last_time, COUNT(*) as click_count,
               GROUP_CONCAT(DISTINCT NULLIF(app_version,'')) AS versions,
               GROUP_CONCAT(DISTINCT COALESCE(NULLIF(access_type,''),'unknown')) AS access_types
        FROM news_event_logs
        WHERE user_id = ? AND event_type = ? AND created_at >= ?
        GROUP BY article_date, article_key
        ORDER BY article_date DESC, last_time DESC
    ''', (user_id, event_type, analytics_cutoff_iso())))


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §5 Finance 美股（独享）                                               ║
# ║   5.1 配置/免点日  5.2 点数账本  5.3 建表  5.4 点数/邀请/埋点 API         ║
# ║   5.5 数据查询 API  5.6 后台统计                                        ║
# ╚══════════════════════════════════════════════════════════════════════╝

# ---------------- §5.1 配置 / 免点数日 ----------------
FINANCE_DB_PATH = os.path.join(BASE_RESOURCES_DIR, 'Finance', 'Finance.db')
ALLOWED_FINANCE_EVENT_TYPES = {'click'}

# 美股节假日（必须与客户端 TradingDateHelper.holidays 保持一致）
US_MARKET_HOLIDAYS = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
    "2027-05-31", "2027-06-18", "2027-07-05", "2027-09-06",
    "2027-11-25", "2027-12-24",
    "2028-01-17", "2028-02-21", "2028-04-14", "2028-05-29",
    "2028-06-19", "2028-07-04", "2028-09-04", "2028-11-23",
    "2028-12-25",
    "2029-01-01", "2029-01-15", "2029-02-19", "2029-03-30",
    "2029-05-28", "2029-06-19", "2029-07-04", "2029-09-03",
    "2029-11-22", "2029-12-25",
}

# 使用分组独立扣点的动作
_SECTOR_OVERRIDE_ACTIONS = {'open_sector', 'open_special_list', 'view_big_orders'}


def is_free_access_day():
    """服务器权威：北京今天往前推 1 天，若该日为周末或美股节假日 → 免点。（§1 check_version 也会调用）"""
    ref = datetime.now(APP_TZ) - timedelta(days=1)
    return ref.weekday() >= 5 or ref.strftime('%Y-%m-%d') in US_MARKET_HOLIDAYS


def get_finance_config():
    data = _load_version_json('Finance')
    try:
        return {
            'daily_free_limit': int(data.get('daily_free_limit', 25)),
            'bonus_points': int(data.get('bonus_points', 0)),
            'invite_reward_points': int(data.get('invite_reward_points', 300)),
            'cost_config': data.get('cost_config', {}) or {},
            'sector_cost_overrides': data.get('sector_cost_overrides', {}) or {},
        }
    except Exception as e:
        print(f"读取 Finance 配置失败: {e}")
        return {'daily_free_limit': 25, 'bonus_points': 0, 'invite_reward_points': 300,
                'cost_config': {}, 'sector_cost_overrides': {}}


def finance_calc_cost(cfg, action, item_key):
    """服务器权威计算单次扣点"""
    try:
        if item_key and action in _SECTOR_OVERRIDE_ACTIONS:
            ov = cfg['sector_cost_overrides'].get(item_key)
            if ov is not None:
                return int(ov)
        return int(cfg['cost_config'].get(action, 1))
    except (TypeError, ValueError):
        return 1


# ---------------- §5.2 点数账本（user_data.db） ----------------
def _gen_invite_code(cursor, length=6):
    for _ in range(30):
        code = ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length))
        if not cursor.execute("SELECT 1 FROM finance_points WHERE invite_code=?", (code,)).fetchone():
            return code
    return ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length + 2))


def _ensure_finance_points(c, user_id):
    """确保点数行存在（不存在则创建并发放一次性赠送点数）；跨天重置每日额度。返回 (row, cfg)"""
    cfg = get_finance_config()
    today = today_str()
    row = c.execute("SELECT * FROM finance_points WHERE user_id=?", (user_id,)).fetchone()
    if row is None:
        code = _gen_invite_code(c)
        bonus = cfg['bonus_points']
        c.execute('''INSERT OR IGNORE INTO finance_points
            (user_id, invite_code, bonus_remaining, bonus_total, daily_used,
             last_date, invited_by_code, invite_reward_count, created_at)
            VALUES (?,?,?,?,0,?,NULL,0,?)''',
            (user_id, code, bonus, bonus, today, now_iso()))
        row = c.execute("SELECT * FROM finance_points WHERE user_id=?", (user_id,)).fetchone()
    elif row['last_date'] != today:
        c.execute("UPDATE finance_points SET daily_used=0, last_date=? WHERE user_id=?", (today, user_id))
        row = c.execute("SELECT * FROM finance_points WHERE user_id=?", (user_id,)).fetchone()
    return row, cfg


def _grant_finance_bonus(c, user_id, points):
    _ensure_finance_points(c, user_id)
    c.execute("""UPDATE finance_points
                 SET bonus_remaining = bonus_remaining + ?, bonus_total = bonus_total + ?
                 WHERE user_id=?""", (points, points, user_id))


def _log_finance_invite(inviter_id, code, invitee_id, points):
    try:
        conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
        conn.execute('''INSERT INTO finance_invite_logs
            (inviter_id, inviter_code, invitee_id, reward_days, created_at)
            VALUES (?,?,?,?,?)''', (inviter_id, code, invitee_id, points, now_iso()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"记录邀请日志失败: {e}")


# ---------------- §5.3 建表 ----------------
def init_finance_user_db():
    """user_data.db 中 Finance 点数账本 + 当日已解锁项"""
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS finance_points (
            user_id TEXT PRIMARY KEY,
            invite_code TEXT UNIQUE,
            bonus_remaining INTEGER DEFAULT 0,
            bonus_total INTEGER DEFAULT 0,
            daily_used INTEGER DEFAULT 0,
            last_date TEXT,
            invited_by_code TEXT,
            invite_reward_count INTEGER DEFAULT 0,
            created_at TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS finance_daily_unlocks (
            user_id TEXT NOT NULL,
            item_key TEXT NOT NULL,
            unlock_date TEXT NOT NULL,
            created_at TIMESTAMP,
            PRIMARY KEY (user_id, item_key, unlock_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_unlock ON finance_daily_unlocks(user_id, unlock_date)')
    conn.commit()
    conn.close()


def init_finance_analytics_db():
    """analytics.db 中 Finance 点击统计 + 邀请流水"""
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_finance_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',
            card_key TEXT NOT NULL,
            card_name TEXT,
            event_type TEXT DEFAULT 'click',
            first_at TIMESTAMP NOT NULL,
            last_at TIMESTAMP NOT NULL,
            count INTEGER DEFAULT 1,
            UNIQUE(user_id, card_key, event_type)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS finance_event_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',
            card_key TEXT NOT NULL,
            card_name TEXT,
            event_type TEXT DEFAULT 'click',
            created_at TIMESTAMP NOT NULL
        )
    ''')
    for tbl in ('user_finance_events', 'finance_event_logs'):
        for ddl in (f"ALTER TABLE {tbl} ADD COLUMN access_type TEXT",
                    f"ALTER TABLE {tbl} ADD COLUMN points_cost INTEGER DEFAULT 0"):
            try:
                c.execute(ddl)
            except sqlite3.OperationalError:
                pass
    try:
        c.execute("ALTER TABLE finance_event_logs ADD COLUMN app_version TEXT")
    except sqlite3.OperationalError:
        pass
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_time ON finance_event_logs(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_card ON finance_event_logs(card_key)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_user ON finance_event_logs(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_access ON finance_event_logs(access_type)')
    c.execute('''
        CREATE TABLE IF NOT EXISTS finance_invite_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inviter_id TEXT NOT NULL,
            inviter_code TEXT,
            invitee_id TEXT NOT NULL,
            reward_days INTEGER,          -- 历史命名，实际存的是点数
            created_at TIMESTAMP NOT NULL
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_invite_inviter ON finance_invite_logs(inviter_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_invite_time ON finance_invite_logs(created_at)')
    conn.commit()
    conn.close()


# ---------------- §5.4 点数 / 邀请 / 埋点 API ----------------
@app.route('/api/Finance/quota/status', methods=['GET'])
def finance_quota_status():
    user_id = request.args.get('user_id')
    cfg = get_finance_config()
    if not is_real_login_user(user_id):
        return jsonify({
            "logged_in": False,
            "daily_limit": cfg['daily_free_limit'],
            "daily_used": 0, "bonus_remaining": 0, "remaining_total": 0,
            "invite_code": None, "invite_reward_count": 0,
            "has_redeemed_invite": False, "unlocked_keys": [],
            "invite_reward_points": cfg['invite_reward_points']
        })
    conn = sqlite3.connect(USER_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        row, cfg = _ensure_finance_points(c, user_id)
        keys = [r['item_key'] for r in c.execute(
            "SELECT item_key FROM finance_daily_unlocks WHERE user_id=? AND unlock_date=?",
            (user_id, today_str())).fetchall()]
        conn.commit()
        daily_remaining = max(0, cfg['daily_free_limit'] - row['daily_used'])
        return jsonify({
            "logged_in": True,
            "daily_limit": cfg['daily_free_limit'],
            "daily_used": row['daily_used'],
            "bonus_remaining": row['bonus_remaining'],
            "remaining_total": row['bonus_remaining'] + daily_remaining,
            "invite_code": row['invite_code'],
            "invite_reward_count": row['invite_reward_count'],
            "has_redeemed_invite": bool(row['invited_by_code']),
            "unlocked_keys": keys,
            "invite_reward_points": cfg['invite_reward_points']
        })
    finally:
        conn.close()


@app.route('/api/Finance/quota/consume', methods=['POST'])
def finance_quota_consume():
    data = _json_body()
    user_id = data.get('user_id')
    action = data.get('action', '') or ''
    item_key = data.get('item_key', '') or ''
    if not is_real_login_user(user_id):
        return jsonify({"status": "not_logged_in", "remaining_total": 0,
                        "bonus_remaining": 0, "daily_used": 0})
    cfg = get_finance_config()
    cost = finance_calc_cost(cfg, action, item_key)
    unlock_key = f"{action}|{item_key.upper()}" if item_key else action
    today = today_str()

    conn = sqlite3.connect(USER_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("BEGIN IMMEDIATE")
        row, cfg = _ensure_finance_points(c, user_id)
        exists = c.execute("SELECT 1 FROM finance_daily_unlocks WHERE user_id=? AND item_key=? AND unlock_date=?",
                           (user_id, unlock_key, today)).fetchone()
        daily_remaining = max(0, cfg['daily_free_limit'] - row['daily_used'])
        total = row['bonus_remaining'] + daily_remaining
        base = {"cost": cost, "remaining_total": total,
                "bonus_remaining": row['bonus_remaining'],
                "daily_used": row['daily_used'], "daily_limit": cfg['daily_free_limit']}

        if exists or cost <= 0 or is_free_access_day():
            c.execute("COMMIT")
            return jsonify(dict(base, status="already_unlocked" if exists else "free"))
        if total < cost:
            c.execute("COMMIT")
            return jsonify(dict(base, status="insufficient"))

        bonus = row['bonus_remaining']
        use_bonus = min(bonus, cost)
        bonus -= use_bonus
        use_daily = cost - use_bonus
        daily_used = row['daily_used'] + use_daily
        c.execute("UPDATE finance_points SET bonus_remaining=?, daily_used=?, last_date=? WHERE user_id=?",
                  (bonus, daily_used, today, user_id))
        c.execute('''INSERT OR IGNORE INTO finance_daily_unlocks (user_id, item_key, unlock_date, created_at)
                     VALUES (?,?,?,?)''', (user_id, unlock_key, today, now_iso()))
        c.execute("COMMIT")
        total = bonus + max(0, cfg['daily_free_limit'] - daily_used)
        return jsonify({"status": "success", "cost": cost, "remaining_total": total,
                        "bonus_remaining": bonus, "daily_used": daily_used,
                        "daily_limit": cfg['daily_free_limit'],
                        "used_bonus": use_bonus, "used_daily": use_daily})
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/Finance/invite/redeem', methods=['POST'])
def finance_invite_redeem():
    data = _json_body()
    invitee_id = data.get('user_id')
    code = (data.get('invite_code') or '').strip().upper()
    if not is_real_login_user(invitee_id):
        return jsonify({"error": "请先登录后再使用邀请码"}), 401
    if not code:
        return jsonify({"error": "请输入邀请码"}), 400
    reward_points = get_finance_config()['invite_reward_points']

    conn = sqlite3.connect(USER_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    inviter_id = None
    invitee_bonus = 0
    invitee_total = 0
    try:
        c.execute("BEGIN IMMEDIATE")
        invitee_row, cfg = _ensure_finance_points(c, invitee_id)
        if invitee_row['invited_by_code']:
            c.execute("COMMIT")
            return jsonify({"error": "您已经使用过邀请码了，每位用户仅限使用一次"}), 403
        inviter = c.execute("SELECT * FROM finance_points WHERE invite_code=?", (code,)).fetchone()
        if not inviter:
            c.execute("COMMIT")
            return jsonify({"error": "邀请码无效，请检查后重试"}), 404
        inviter_id = inviter['user_id']
        if inviter_id == invitee_id:
            c.execute("COMMIT")
            return jsonify({"error": "不能使用自己的邀请码哦"}), 400
        # 互邀检测：对方之前用的正是"我"的邀请码 → 禁止反向再领
        if inviter['invited_by_code'] and invitee_row['invite_code'] \
           and inviter['invited_by_code'] == invitee_row['invite_code']:
            c.execute("COMMIT")
            return jsonify({"error": "你们已经互相邀请过啦，每对好友仅能领取一次奖励"}), 403

        _grant_finance_bonus(c, invitee_id, reward_points)
        _grant_finance_bonus(c, inviter_id, reward_points)
        c.execute("UPDATE finance_points SET invited_by_code=? WHERE user_id=?", (code, invitee_id))
        c.execute("UPDATE finance_points SET invite_reward_count=invite_reward_count+1 WHERE user_id=?", (inviter_id,))
        inv_row = c.execute("SELECT bonus_remaining, daily_used FROM finance_points WHERE user_id=?",
                            (invitee_id,)).fetchone()
        invitee_bonus = inv_row['bonus_remaining']
        invitee_total = invitee_bonus + max(0, cfg['daily_free_limit'] - inv_row['daily_used'])
        c.execute("COMMIT")
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    _log_finance_invite(inviter_id, code, invitee_id, reward_points)
    return jsonify({"status": "success", "reward_points": reward_points,
                    "bonus_remaining": invitee_bonus, "remaining_total": invitee_total})


@app.route('/api/Finance/track', methods=['POST'])
def track_finance_event():
    data = _json_body()
    user_id = data.get('user_id')
    user_type = data.get('user_type', 'apple')
    card_key = data.get('card_key')
    card_name = data.get('card_name', '')
    event_type = data.get('event_type', 'click')
    app_version = data.get('app_version', '')
    access_type = (data.get('access_type') or 'unknown').strip()
    try:
        points_cost = max(0, int(data.get('points_cost') or 0))
    except Exception:
        points_cost = 0
    if not user_id or not card_key or event_type not in ALLOWED_FINANCE_EVENT_TYPES:
        return jsonify({"error": "Invalid params"}), 400
    if access_type not in ALLOWED_ACCESS_TYPES:
        access_type = 'unknown'

    if not is_real_login_user(user_id):
        # 未登录用户的点击（理论上已被客户端拦截），统一标记以便排查
        user_type, access_type, points_cost = 'device', 'guest', 0
        if not card_key.startswith('GUEST_'):
            card_key = 'GUEST_' + card_key
    else:
        kind = get_subscription_kind(user_id, 'Finance')   # 付费用户不可能扣点
        if kind:
            access_type, points_cost = kind, 0

    now = now_iso()
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    try:
        c = conn.cursor()
        c.execute('''
            INSERT INTO user_finance_events
                (user_id, user_type, card_key, card_name, event_type,
                 first_at, last_at, count, access_type, points_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(user_id, card_key, event_type)
            DO UPDATE SET last_at = ?, count = count + 1,
                          access_type = ?, points_cost = COALESCE(points_cost,0) + ?
        ''', (user_id, user_type, card_key, card_name, event_type, now, now,
              access_type, points_cost, now, access_type, points_cost))
        c.execute('''
            INSERT INTO finance_event_logs
                (user_id, user_type, card_key, card_name, event_type,
                 created_at, app_version, access_type, points_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, user_type, card_key, card_name, event_type,
              now, app_version, access_type, points_cost))
        conn.commit()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------- §5.5 数据查询 API（替代客户端本地 SQL） ----------------
def get_finance_db():
    db = getattr(g, '_finance_database', None)
    if db is None:
        if not os.path.exists(FINANCE_DB_PATH):
            return None
        db = g._finance_database = sqlite3.connect(FINANCE_DB_PATH, timeout=60.0)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_finance_database', None)
    if db is not None:
        db.close()


def _finance_table_ok(db, table_name):
    """【安全修复】table 参数会拼进 SQL，必须确认是 Finance.db 里真实存在的表"""
    if not table_name or '"' in table_name:
        return False
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (table_name,)).fetchone() is not None


def require_finance_user(f):
    """Finance 数据查询接口：必须携带已登录且存在的 Apple user_id（存在性缓存 5 分钟）"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        uid = request.args.get('user_id') or _json_body().get('user_id')
        if not is_real_login_user(uid):
            return jsonify({"error": "login_required"}), 401
        try:
            if not _user_exists(uid):
                return jsonify({"error": "login_required"}), 401
        except Exception:
            return jsonify({"error": "auth_check_failed"}), 500
        return f(*args, **kwargs)
    return wrapper


@app.route('/api/Finance/query/market_cap', methods=['GET'])
@require_finance_user
def query_market_cap():
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    try:
        rows = db.execute('SELECT symbol, marketcap, pe_ratio, pb FROM "MNSPP"').fetchall()
        return jsonify([{"symbol": r["symbol"], "marketCap": r["marketcap"],
                         "peRatio": r["pe_ratio"], "pb": r["pb"]} for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/historical', methods=['GET'])
@require_finance_user
def query_historical():
    symbol = request.args.get('symbol')
    table_name = request.args.get('table')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    if not all([symbol, table_name, start_date, end_date]):
        return jsonify({"error": "Missing parameters"}), 400
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    if not _finance_table_ok(db, table_name):
        return jsonify({"error": "Invalid table"}), 400
    try:
        columns = {r['name'].lower() for r in db.execute(f'PRAGMA table_info("{table_name}")').fetchall()}
        optional = [c for c in ("volume", "open", "high", "low") if c in columns]
        select_clause = ", ".join(["date", "price"] + optional)
        rows = db.execute(f'''
            SELECT {select_clause} FROM "{table_name}"
            WHERE name = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
        ''', (symbol, start_date, end_date)).fetchall()
        result = []
        for row in rows:
            item = {"date": row["date"], "price": row["price"]}
            for col in optional:
                if row[col] is not None:
                    item[col] = row[col]
            result.append(item)
        return jsonify(result)
    except Exception as e:
        print(f"Error querying historical: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/earning', methods=['GET'])
@require_finance_user
def query_earning():
    symbol = request.args.get('symbol')
    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    try:
        rows = db.execute('SELECT date, price FROM Earning WHERE name = ?', (symbol,)).fetchall()
        return jsonify([{"date": r["date"], "price": r["price"]} for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/closing_price', methods=['GET'])
@require_finance_user
def query_closing_price():
    symbol = request.args.get('symbol')
    date = request.args.get('date')
    table_name = request.args.get('table')
    if not all([symbol, date, table_name]):
        return jsonify({"error": "Missing parameters"}), 400
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    if not _finance_table_ok(db, table_name):
        return jsonify({"error": "Invalid table"}), 400
    try:
        row = db.execute(f'SELECT price FROM "{table_name}" WHERE name = ? AND date = ? LIMIT 1',
                         (symbol, date)).fetchone()
        return jsonify({"price": row["price"] if row else None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/latest_volume', methods=['GET'])
@require_finance_user
def query_latest_volume():
    symbol = request.args.get('symbol')
    table_name = request.args.get('table')
    if not all([symbol, table_name]):
        return jsonify({"error": "Missing parameters"}), 400
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    if not _finance_table_ok(db, table_name):
        return jsonify({"error": "Invalid table"}), 400
    try:
        columns = {r['name'].lower() for r in db.execute(f'PRAGMA table_info("{table_name}")').fetchall()}
        if 'volume' not in columns:
            return jsonify({"volume": None})
        row = db.execute(f'SELECT volume FROM "{table_name}" WHERE name = ? ORDER BY date DESC LIMIT 1',
                         (symbol,)).fetchone()
        return jsonify({"volume": row["volume"] if row else None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/options_summary', methods=['GET'])
@require_finance_user
def query_options_summary():
    """支持单个 symbol（返回对象）或逗号分隔 symbols（返回字典）"""
    symbol_param = request.args.get('symbol')
    symbols_param = request.args.get('symbols')
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    if symbols_param:
        target_symbols = [s.strip() for s in symbols_param.split(',') if s.strip()][:500]
    elif symbol_param:
        target_symbols = [symbol_param]
    else:
        target_symbols = []
    if not target_symbols:
        return jsonify({"error": "Missing parameters"}), 400
    try:
        results = {}
        for sym in target_symbols:
            rows = db.execute('''
                SELECT call, put, price, change, iv, date
                FROM "Options" WHERE name = ?
                ORDER BY date DESC LIMIT 2
            ''', (sym,)).fetchall()
            if rows:
                latest = rows[0]
                prev = rows[1] if len(rows) > 1 else None
                results[sym] = {
                    "call": latest["call"], "put": latest["put"],
                    "price": latest["price"], "change": latest["change"],
                    "iv": latest["iv"], "date": latest["date"],
                    "prev_iv": prev["iv"] if prev else None,
                    "prev_price": prev["price"] if prev else None,
                    "prev_change": prev["change"] if prev else None
                }
        if symbols_param:
            return jsonify(results)
        if target_symbols[0] in results:
            return jsonify(results[target_symbols[0]])
        return jsonify({"call": None, "put": None, "price": None, "change": None,
                        "iv": None, "date": None,
                        "prev_iv": None, "prev_price": None, "prev_change": None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/options_price_history', methods=['GET'])
@require_finance_user
def query_options_price_history():
    symbol = request.args.get('symbol')
    if not symbol:
        return jsonify({"error": "Missing parameters"}), 400
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    try:
        rows = db.execute('SELECT date, price, iv FROM "Options" WHERE name = ? ORDER BY date DESC',
                          (symbol,)).fetchall()
        return jsonify([{"date": r["date"], "price": r["price"], "iv": r["iv"]} for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/Finance/query/options_rank', methods=['GET'])
@require_finance_user
def query_options_rank():
    """期权 IV 榜单：最新日 IV 降序前 20 / 升序前 20（市值阀值默认 500 亿）"""
    limit = request.args.get('limit', default=50000000000, type=float)
    db = get_finance_db()
    if not db:
        return jsonify({"error": "Database not found"}), 500
    try:
        date_rows = db.execute('SELECT DISTINCT date FROM "Options" ORDER BY date DESC LIMIT 2').fetchall()
        if not date_rows:
            return jsonify({"rank_up": [], "rank_down": []})
        latest_date = date_rows[0]['date']
        prev_date = date_rows[1]['date'] if len(date_rows) > 1 else None
        rows = db.execute('''
            SELECT t1.name AS symbol,
                   t1.iv AS iv_latest, t1.price AS price_latest, t1.change AS change_latest,
                   t2.iv AS iv_prev, t2.price AS price_prev, t2.change AS change_prev,
                   m.marketcap
            FROM "Options" t1
            LEFT JOIN "Options" t2 ON t1.name = t2.name AND t2.date = ?
            JOIN "MNSPP" m ON t1.name = m.symbol
            WHERE t1.date = ? AND m.marketcap > ? AND t1.iv IS NOT NULL
        ''', (prev_date, latest_date, limit)).fetchall()

        all_results = []
        for r in rows:
            raw_iv = r["iv_latest"]
            try:
                sort_val = float(str(raw_iv).replace('%', '').strip()) if raw_iv else 0.0
            except ValueError:
                sort_val = 0.0
            all_results.append({
                "symbol": r["symbol"],
                "iv": raw_iv, "prev_iv": r["iv_prev"],
                "price": r["price_latest"], "change": r["change_latest"],
                "prev_price": r["price_prev"], "prev_change": r["change_prev"],
                "_sort": sort_val,
            })
        if not all_results:
            return jsonify({"rank_up": [], "rank_down": []})
        all_results.sort(key=lambda x: x["_sort"], reverse=True)
        rank_up = all_results[:20]
        rank_down = list(reversed(all_results[-20:]))
        strip = lambda lst: [{k: v for k, v in it.items() if k != "_sort"} for it in lst]
        return jsonify({"rank_up": strip(rank_up), "rank_down": strip(rank_down)})
    except Exception as e:
        print(f"Error querying options rank: {e}")
        return jsonify({"error": str(e)}), 500


# ---------------- §5.6 后台：美股统计 ----------------
@app.route('/admin/api/finance/overview', methods=['GET'])
@require_admin
def admin_finance_overview():
    today = today_str()
    q = lambda sql, p=(): _query_analytics(sql, p)[0]['c']
    return jsonify({
        "total_users":  q("SELECT COUNT(DISTINCT user_id) c FROM finance_event_logs"),
        "today_active": q("SELECT COUNT(DISTINCT user_id) c FROM finance_event_logs WHERE date(created_at)=?", (today,)),
        "today_clicks": q("SELECT COUNT(*) c FROM finance_event_logs WHERE date(created_at)=?", (today,)),
        "total_clicks": q("SELECT COUNT(*) c FROM finance_event_logs"),
        "today_vip_clicks":    q("SELECT COUNT(*) c FROM finance_event_logs WHERE access_type IN ('subscription','vip_permanent') AND date(created_at)=?", (today,)),
        "today_points_clicks": q("SELECT COUNT(*) c FROM finance_event_logs WHERE access_type LIKE 'points%' AND date(created_at)=?", (today,)),
        "today_points_spent":  q("SELECT COALESCE(SUM(points_cost),0) c FROM finance_event_logs WHERE date(created_at)=?", (today,)),
    })


@app.route('/admin/api/finance/top_users', methods=['GET'])
@require_admin
def admin_finance_top_users():
    return jsonify(_query_analytics('''
        SELECT user_id,
               MAX(user_type) AS user_type,
               COUNT(DISTINCT card_key) AS unique_cards,
               COUNT(*) AS total_clicks,
               SUM(CASE WHEN access_type IN ('subscription','vip_permanent') THEN 1 ELSE 0 END) AS vip_clicks,
               SUM(CASE WHEN access_type LIKE 'points%' THEN 1 ELSE 0 END) AS points_clicks,
               SUM(CASE WHEN access_type IN ('free','free_day','unlocked_today') THEN 1 ELSE 0 END) AS free_clicks,
               COALESCE(SUM(points_cost),0) AS points_spent,
               MAX(created_at) AS last_active
        FROM finance_event_logs
        GROUP BY user_id
        ORDER BY total_clicks DESC
        LIMIT 50
    '''))


@app.route('/admin/api/finance/user_details', methods=['GET'])
@require_admin
def admin_finance_user_details():
    maybe_cleanup_old_unlocks()
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    return jsonify(_query_analytics('''
        SELECT card_key, MAX(card_name) AS card_name,
               MAX(created_at) AS last_time,
               COUNT(*) AS click_count,
               GROUP_CONCAT(DISTINCT NULLIF(app_version,'')) AS versions,
               GROUP_CONCAT(DISTINCT NULLIF(access_type,'')) AS access_types,
               COALESCE(SUM(points_cost),0) AS points_cost
        FROM finance_event_logs
        WHERE user_id = ? AND created_at >= ?
        GROUP BY card_key
        ORDER BY last_time DESC
    ''', (user_id, analytics_cutoff_iso())))


@app.route('/admin/api/finance/daily_trend', methods=['GET'])
@require_admin
def admin_finance_daily_trend():
    return jsonify(_query_analytics('''
        SELECT date(created_at) AS day, COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS uu
        FROM finance_event_logs
        WHERE created_at >= datetime('now', '+8 hours', '-30 days')
        GROUP BY day
        ORDER BY day ASC
    '''))


@app.route('/admin/api/finance/top_cards', methods=['GET'])
@require_admin
def admin_finance_top_cards():
    where = _period_where(request.args.get('period', '7d'))
    return jsonify(_query_analytics(f'''
        SELECT card_key, MAX(card_name) AS card_name,
               COUNT(DISTINCT user_id) AS unique_users,
               COUNT(*) AS total_count
        FROM finance_event_logs
        WHERE 1=1 {where}
        GROUP BY card_key
        ORDER BY total_count DESC
        LIMIT 60
    '''))


@app.route('/admin/api/finance/invite_overview', methods=['GET'])
@require_admin
def admin_finance_invite_overview():
    today = today_str()
    q = lambda sql, p=(): _query_analytics(sql, p)[0]['c']
    return jsonify({
        "total_invites":     q("SELECT COUNT(*) c FROM finance_invite_logs"),
        "today_invites":     q("SELECT COUNT(*) c FROM finance_invite_logs WHERE date(created_at)=?", (today,)),
        "unique_inviters":   q("SELECT COUNT(DISTINCT inviter_id) c FROM finance_invite_logs"),
        "total_reward_days": q("SELECT COALESCE(SUM(reward_days),0) c FROM finance_invite_logs"),
    })


@app.route('/admin/api/finance/top_inviters', methods=['GET'])
@require_admin
def admin_finance_top_inviters():
    return jsonify(_query_analytics('''
        SELECT inviter_id, MAX(inviter_code) AS inviter_code,
               COUNT(*) AS invite_count, SUM(reward_days) AS total_days,
               MAX(created_at) AS last_time
        FROM finance_invite_logs
        GROUP BY inviter_id ORDER BY invite_count DESC LIMIT 50
    '''))


@app.route('/admin/api/finance/invite_logs', methods=['GET'])
@require_admin
def admin_finance_invite_logs():
    return jsonify(_query_analytics('''
        SELECT inviter_id, inviter_code, invitee_id, reward_days, created_at
        FROM finance_invite_logs ORDER BY created_at DESC LIMIT 100
    '''))


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §6 后台网页（ADMIN_HTML）                                             ║
# ║      ⚠️ 把你原来的那一整块（@app.route('/admin') + ADMIN_HTML = r'''...'''）║
# ║         原样粘贴替换下面这几行即可                                      ║
# ╚══════════════════════════════════════════════════════════════════════╝

# Dashboard 网页本体
@app.route('/admin', methods=['GET'])
def admin_page():
    return ADMIN_HTML

ADMIN_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ONews 行为监控</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,system-ui,"PingFang SC",sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
  .login-box{max-width:380px;margin:120px auto;background:#1e293b;padding:40px;border-radius:16px;box-shadow:0 20px 60px rgba(0,0,0,.4)}
  .login-box h1{margin-bottom:24px;font-size:22px;text-align:center}
  input,button{width:100%;padding:12px 14px;border-radius:10px;border:none;font-size:15px}
  input{background:#0f172a;color:#e2e8f0;border:1px solid #334155;margin-bottom:14px}
  button{background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:white;cursor:pointer;font-weight:600}
  button:hover{opacity:.9}
  .container{max-width:1400px;margin:0 auto;padding:24px;display:none}
  .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px;flex-wrap:wrap;gap:12px}
  .header h1{font-size:22px;background:linear-gradient(135deg,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
  
  .module-switch{display:flex;gap:8px;background:#1e293b;padding:6px;border-radius:12px;border:1px solid #334155}
  .module-tab{padding:8px 18px;border-radius:8px;font-size:13px;cursor:pointer;font-weight:600;color:#94a3b8;transition:all .2s}
  .module-tab.active{background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:white}
  
  .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}
  .stat-card{background:#1e293b;padding:20px;border-radius:14px;border:1px solid #334155}
  .stat-card .label{font-size:12px;color:#94a3b8;margin-bottom:8px}
  .stat-card .value{font-size:28px;font-weight:700;background:linear-gradient(135deg,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
  .row-full{display:grid;grid-template-columns:1fr;gap:16px;margin-bottom:24px}
  @media(max-width:900px){.row{grid-template-columns:1fr}}
  .panel{background:#1e293b;padding:20px;border-radius:14px;border:1px solid #334155}
  .panel h3{margin-bottom:16px;font-size:15px;color:#cbd5e1;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
  .tabs{display:flex;gap:6px}
  .tab{padding:6px 12px;background:#0f172a;border-radius:8px;font-size:12px;cursor:pointer;border:1px solid #334155}
  .tab.active{background:linear-gradient(135deg,#3b82f6,#8b5cf6);border-color:transparent}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #334155}
  th{color:#94a3b8;font-weight:500;font-size:12px}
  tr:hover td{background:#0f172a}
  .pill{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px}
  .pill-green{background:rgba(34,197,94,.2);color:#86efac}
  .pill-blue{background:rgba(59,130,246,.2);color:#93c5fd}
  .pill-purple{background:rgba(167,139,250,.2);color:#c4b5fd}
  .pill-orange{background:rgba(251,146,60,.2);color:#fdba74}
  .err{color:#f87171;text-align:center;margin-top:10px;font-size:13px}
  .clickable{cursor:pointer;color:#60a5fa}
  .clickable:hover{text-decoration:underline}
  canvas{max-height:280px}
  .module-section{display:none}
  .module-section.active{display:block}
  
  .danger-zone{border:1px solid #ef4444;background:rgba(239,68,68,.05)}
  .danger-zone h3{color:#f87171 !important}
  .btn-group{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px}
  .btn-danger{background:#dc2626;width:auto;padding:10px 20px;font-size:13px;border-radius:8px}
  .btn-danger:hover{background:#b91c1c}
  
  .modal-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.7);display:none;align-items:center;justify-content:center;z-index:1000}
  .modal{background:#1e293b;border:1px solid #ef4444;border-radius:14px;padding:24px;max-width:500px;width:90%;max-height:80vh;overflow-y:auto}
  .modal h4{color:#f87171;font-size:18px;margin-bottom:12px}
  .modal p{font-size:14px;color:#cbd5e1;line-height:1.6;margin-bottom:20px}
  .modal-btns{display:flex;justify-content:flex-end;gap:12px}
  .modal-btn{padding:8px 16px;border-radius:6px;font-size:13px;cursor:pointer;border:none;font-weight:600}
  .btn-cancel{background:#475569;color:#e2e8f0}
  .btn-confirm-final{background:#dc2626;color:white}
  
  /* 排序指示样式 */
  .sortable { cursor: pointer; user-select: none; }
  .sortable:hover { color: #60a5fa; }
  /* 【需求 a】暂时隐藏三个模块的第一行总览卡片 */
  #statsBox, #newsStatsBox, #financeStatsBox { display:none !important; }

  /* 【需求 b】鼠标悬浮明细卡 */
  .hover-card{position:fixed;z-index:2000;width:420px;max-height:60vh;overflow:auto;
    background:#0b1220;border:1px solid #3b82f6;border-radius:12px;padding:14px;
    box-shadow:0 20px 50px rgba(0,0,0,.6);font-size:12.5px;color:#cbd5e1;pointer-events:none}
  .hover-card .hover-title{font-size:13px;font-weight:700;color:#60a5fa;margin-bottom:8px}

  /* 在线客服聊天气泡 */
  .chat-wrap{max-height:46vh;overflow-y:auto;background:#0f172a;border:1px solid #334155;
    border-radius:12px;padding:12px;margin-bottom:12px}
  .msg{display:flex;margin-bottom:10px}
  .msg.user{justify-content:flex-start}
  .msg.admin{justify-content:flex-end}
  .bubble{max-width:78%;padding:8px 12px;border-radius:12px;font-size:13px;line-height:1.5;white-space:pre-wrap;word-break:break-word}
  .msg.user .bubble{background:#1e293b;border:1px solid #334155;color:#e2e8f0;border-top-left-radius:4px}
  .msg.admin .bubble{background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:#fff;border-top-right-radius:4px}
  .msg .time{font-size:10px;color:#64748b;margin:0 8px;align-self:flex-end}
</style>
</head>
<body>

<div class="login-box" id="loginBox">
  <h1>🎬 ONews 后台</h1>
  <input type="password" id="pwdInput" placeholder="管理员密码" />
  <button onclick="login()">登录</button>
  <div class="err" id="loginErr"></div>
</div>

<div class="container" id="dashboard">
  <div class="header">
    <h1>📊 ONews 用户行为监控</h1>
    <div class="module-switch">
      <div class="module-tab active" id="tabVideo" onclick="switchModule('video')">🎬 视频模块</div>
      <div class="module-tab" id="tabNews" onclick="switchModule('news')">📰 新闻模块</div>
      <div class="module-tab" id="tabFinance" onclick="switchModule('finance')">📈 美股模块</div>
    </div>
    <div>
      <span style="color:#94a3b8;font-size:13px;margin-right:12px" id="updateTime"></span>
      <button onclick="loadCurrentModule()" style="width:auto;padding:8px 16px;font-size:13px">🔄 刷新</button>
    </div>
  </div>

  <!-- 视频模块 -->
  <div class="module-section active" id="moduleVideo">
    <div class="stats" id="statsBox"></div>

    <!-- 【上移 + 改造】视频 - 活跃用户榜 -->
    <div class="panel" style="margin-bottom:24px">
      <h3>👥 视频 - 活跃用户榜 <span style="font-size:12px;color:#94a3b8;font-weight:normal;">(点击"看过/下载视频数"查看明细；点击"总操作数"或"最后活跃"可切换排序)</span></h3>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>user id_apple</th>
            <th>user id_device</th>
            <th>在线播放</th>
            <th>离线播放</th>
            <th>下载视频数</th>
            <th>💎订阅播放</th>
            <th>🎫点数播放</th>
            <th class="sortable" onclick="sortVideoUsers('total_actions')">总操作数 <span id="vsort_total_actions">▼</span></th>
            <th class="sortable" onclick="sortVideoUsers('last_active')">最后活跃 <span id="vsort_last_active"></span></th>
          </tr>
        </thead>
        <tbody id="topUsersBody"></tbody>
      </table>
    </div>

    <!-- 【新增】在线客服（ONews / 视频+新闻 客户端） -->
    <div class="panel" style="margin-bottom:24px">
      <h3>💬 在线客服 · 用户咨询（ONews）
        <span class="tabs">
          <span class="tab active" onclick="switchSupportStatus(this,'open','ONews')">待处理</span>
          <span class="tab" onclick="switchSupportStatus(this,'all','ONews')">全部</span>
        </span>
      </h3>
      <table>
        <thead><tr>
          <th>#</th><th>最近消息</th><th>user id_apple</th><th>user id_device</th>
          <th>消息数</th><th>版本</th><th>更新时间</th><th>状态</th><th>操作</th>
        </tr></thead>
        <tbody id="supportBodyONews"></tbody>
      </table>
    </div>
    <div class="panel danger-zone" style="margin-bottom:24px">
      <h3>🚨 错误链接举报
        <span class="tabs">
          <span class="tab active" onclick="switchReportStatus(this,'pending')">待处理</span>
          <span class="tab" onclick="switchReportStatus(this,'all')">全部</span>
        </span>
      </h3>
      <table>
        <thead><tr>
          <th>#</th><th>视频 / 播放页URL</th><th>播放源 (线路)</th><th>集数</th><th>问题</th>
          <th>举报人</th><th>次数</th><th>m3u8</th><th>操作</th>
        </tr></thead>
        <tbody id="reportBody"></tbody>
      </table>
    </div>
    <!-- 用户寻片请求 -->
    <div class="panel" style="margin-bottom:24px">
      <h3>🙋 用户寻片请求
        <span class="tabs">
          <span class="tab active" onclick="switchWishStatus(this,'pending')">待处理</span>
          <span class="tab" onclick="switchWishStatus(this,'all')">全部</span>
        </span>
      </h3>
      <table>
        <thead><tr>
          <th>#</th><th>想看的内容</th><th>搜索词</th><th>用户ID</th>
          <th>次数</th><th>提交时间</th><th>状态</th><th>操作</th>
        </tr></thead>
        <tbody id="wishBody"></tbody>
      </table>
    </div>
    <div class="row">
      <div class="panel">
        <h3>
          🔥 视频播放榜
          <span class="tabs">
            <span class="tab active" onclick="switchPlayPeriod(this,'today')">今日</span>
            <span class="tab" onclick="switchPlayPeriod(this,'7d')">7天</span>
            <span class="tab" onclick="switchPlayPeriod(this,'all')">总计</span>
          </span>
        </h3>
        <table>
          <thead><tr><th>#</th><th>视频</th><th>用户数</th><th>次数</th></tr></thead>
          <tbody id="topPlayBody"></tbody>
        </table>
      </div>
      <div class="panel">
        <h3>
          📥 视频下载榜
          <span class="tabs">
            <span class="tab active" onclick="switchDlPeriod(this,'today')">今日</span>
            <span class="tab" onclick="switchDlPeriod(this,'7d')">7天</span>
            <span class="tab" onclick="switchDlPeriod(this,'all')">总计</span>
          </span>
        </h3>
        <table>
          <thead><tr><th>#</th><th>视频</th><th>用户数</th><th>次数</th></tr></thead>
          <tbody id="topDlBody"></tbody>
        </table>
      </div>
    </div>

    <div class="row-full">
      <div class="panel">
        <h3>📈 视频 - 最近 30 天趋势</h3>
        <canvas id="trendChart"></canvas>
      </div>
    </div>

    <div class="stats" id="onewsInviteStatsBox" style="margin-top:8px"></div>
    <div class="panel" style="margin-bottom:24px">
      <h3>🎁 新闻/视频 - 邀请拉新排行</h3>
      <table>
        <thead><tr><th>#</th><th>邀请人</th><th>邀请码</th><th>成功邀请</th><th>累计赠送点数</th><th>最近</th></tr></thead>
        <tbody id="onewsTopInvitersBody"></tbody>
      </table>
    </div>
    <div class="panel" style="margin-bottom:24px">
      <h3>📜 新闻/视频 - 最近邀请记录</h3>
      <table>
        <thead><tr><th>#</th><th>邀请人</th><th>邀请码</th><th>被邀请人</th><th>奖励</th><th>时间</th></tr></thead>
        <tbody id="onewsInviteLogsBody"></tbody>
      </table>
    </div>
  </div>

  <!-- 新闻模块 -->
  <div class="module-section" id="moduleNews">
    <!-- 1. 统计卡片 -->
    <div class="stats" id="newsStatsBox"></div>
    
    <!-- 2. 【位置上移】新闻 - 活跃用户榜 -->
    <div class="panel" style="margin-bottom:24px">
      <h3>👥 新闻 - 活跃用户榜 <span style="font-size:12px; color:#94a3b8; font-weight:normal;">(点击“所有文章”或“最后活跃”可切换排序)</span></h3>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>user id_apple</th>
            <th>user id_device</th>
            <th class="sortable" onclick="sortNewsUsers('unique_articles')">所有文章 <span id="sort_unique_articles">▼</span></th>
            <th>朗读</th>
            <th>曝光</th>
            <th class="sortable" onclick="sortNewsUsers('last_active')">最后活跃 <span id="sort_last_active"></span></th>
          </tr>
        </thead>
        <tbody id="topNewsUsersBody"></tbody>
      </table>
    </div>

    <!-- 3. 趋势图表 -->
    <div class="row-full">
      <div class="panel">
        <h3>📈 新闻 - 最近 30 天趋势</h3>
        <canvas id="newsTrendChart"></canvas>
      </div>
    </div>
    
    <!-- 4. 新闻源热度榜与热门文章榜 -->
    <div class="row">
      <div class="panel">
        <h3>
          📰 新闻源热度榜
          <span class="tabs">
            <span class="tab" onclick="switchSourcePeriod(this,'today')">今日</span>
            <span class="tab active" onclick="switchSourcePeriod(this,'7d')">7天</span>
            <span class="tab" onclick="switchSourcePeriod(this,'all')">总计</span>
          </span>
        </h3>
        <table>
          <thead><tr><th>#</th><th>新闻源</th><th>用户数</th><th>文章数</th><th>阅读次数</th></tr></thead>
          <tbody id="topSourcesBody"></tbody>
        </table>
      </div>
      <div class="panel">
        <h3>
          🔥 热门文章榜
          <span class="tabs">
            <span class="tab" onclick="switchArticleType(this,'listen')">朗读</span>
            <span class="tab active" onclick="switchArticleType(this,'view')">曝光</span>
          </span>
          <span class="tabs">
            <span class="tab" onclick="switchArticlePeriod(this,'today')">今日</span>
            <span class="tab active" onclick="switchArticlePeriod(this,'7d')">7天</span>
            <span class="tab" onclick="switchArticlePeriod(this,'all')">总计</span>
          </span>
        </h3>
        <table>
          <thead><tr><th>#</th><th>文章</th><th>来源</th><th>用户</th><th>次数</th></tr></thead>
          <tbody id="topArticlesBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- 美股模块 -->
  <div class="module-section" id="moduleFinance">
    <div class="stats" id="financeStatsBox"></div>

    <div class="panel" style="margin-bottom:24px">
      <h3>👥 美股 - 活跃用户榜 <span style="font-size:12px;color:#94a3b8;font-weight:normal;">(点击“点击”数字查看明细；点击表头可切换排序)</span></h3>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>user id_apple</th>
            <th>user id_device</th>
            <th>身份</th>
            <th>💎订阅点击</th>
            <th>🎫扣点次数</th>
            <th class="sortable" onclick="sortFinanceUsers('points_spent')">消耗点数 <span id="fsort_points_spent"></span></th>
            <th class="sortable" onclick="sortFinanceUsers('total_clicks')">点击 <span id="fsort_total_clicks">▼</span></th>
            <th class="sortable" onclick="sortFinanceUsers('last_active')">最后活跃 <span id="fsort_last_active"></span></th>
          </tr>
        </thead>
        <tbody id="financeUsersBody"></tbody>
      </table>
    </div>

    <!-- 【新增】在线客服（美股精灵） -->
    <div class="panel" style="margin-bottom:24px">
      <h3>💬 在线客服 · 用户咨询（美股精灵）
        <span class="tabs">
          <span class="tab active" onclick="switchSupportStatus(this,'open','Finance')">待处理</span>
          <span class="tab" onclick="switchSupportStatus(this,'all','Finance')">全部</span>
        </span>
      </h3>
      <table>
        <thead><tr>
          <th>#</th><th>最近消息</th><th>user id_apple</th><th>user id_device</th>
          <th>消息数</th><th>版本</th><th>更新时间</th><th>状态</th><th>操作</th>
        </tr></thead>
        <tbody id="supportBodyFinance"></tbody>
      </table>
    </div>

    <div class="row-full">
      <div class="panel">
        <h3>📈 美股 - 最近 30 天点击趋势</h3>
        <canvas id="financeTrendChart"></canvas>
      </div>
    </div>

    <div class="panel" style="margin-bottom:24px">
      <h3>🔥 模块热度榜
        <span class="tabs">
          <span class="tab" onclick="switchFinancePeriod(this,'today')">今日</span>
          <span class="tab active" onclick="switchFinancePeriod(this,'7d')">7天</span>
          <span class="tab" onclick="switchFinancePeriod(this,'all')">总计</span>
        </span>
      </h3>
      <table>
        <thead><tr><th>#</th><th>模块 / 卡片</th><th>用户数</th><th>点击次数</th></tr></thead>
        <tbody id="financeTopCardsBody"></tbody>
      </table>
    </div>

    <div class="stats" id="financeInviteStatsBox" style="margin-top:8px"></div>
    <div class="panel" style="margin-bottom:24px">
      <h3>🎁 美股 - 邀请拉新排行</h3>
      <table>
        <thead><tr><th>#</th><th>邀请人</th><th>邀请码</th><th>成功邀请</th><th>累计赠送点数</th><th>最近</th></tr></thead>
        <tbody id="financeTopInvitersBody"></tbody>
      </table>
    </div>
    <div class="panel" style="margin-bottom:24px">
      <h3>📜 美股 - 最近邀请记录</h3>
      <table>
        <thead><tr><th>#</th><th>邀请人</th><th>邀请码</th><th>被邀请人</th><th>奖励</th><th>时间</th></tr></thead>
        <tbody id="financeInviteLogsBody"></tbody>
      </table>
    </div>
  </div>

  <!-- 危险区 -->
  <div class="panel danger-zone" style="margin-bottom:24px">
    <h3>⚠️ 数据库维护与管理 (危险区)</h3>
    <p style="font-size:13px;color:#94a3b8;margin-bottom:15px;">破坏性操作不可恢复，请慎用。</p>
    <div class="btn-group">
      <button class="btn-danger" onclick="triggerClear('analytics')">🧹 清空所有行为统计</button>
      <button class="btn-danger" onclick="triggerClear('users')">👤 清空用户及订阅数据</button>
      <button class="btn-danger" onclick="triggerClear('all')">🔥 彻底清空所有数据</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="confirmModal">
  <div class="modal">
    <h4 id="modalTitle">⚠️ 危险操作确认</h4>
    <p id="modalMsg"></p>
    <div class="modal-btns">
      <button class="modal-btn btn-cancel" onclick="closeModal()">取消</button>
      <button class="modal-btn btn-confirm-final" id="modalConfirmBtn">确认执行</button>
    </div>
  </div>
</div>

<script>
let TOKEN = localStorage.getItem('admin_token') || '';
let currentModule = 'video';
let trendChart, newsTrendChart, financeTrendChart;
let playPeriod='today', dlPeriod='today';
let sourcePeriod='7d';
let articleType='view', articlePeriod='7d';
let financePeriod='7d';
let pendingClearType = '';
let reportStatus = 'pending';
let wishStatus = 'pending';
let pendingWishId = null;
let pendingReportEpisodeUrl = null;
let pendingSupportThread = null;
const supportStatus = { ONews:'open', Finance:'open' };

let cachedVideoUsers = [], videoUserSortField = 'total_actions', videoUserSortOrder = 'desc';
let cachedNewsUsers = [], newsUserSortField = 'unique_articles', newsUserSortOrder = 'desc';
let cachedFinanceUsers = [], financeUserSortField = 'total_clicks', financeUserSortOrder = 'desc';

const REPORT_TYPE_MAP = {
  playback_failed:'无法播放', download_failed:'无法缓存',
  media_error:'音画异常', content_mismatch:'内容不符', other:'其他'
};
const PLAY_SOURCE_MAP = {
  home:'🏠 首页瀑布流', filter:'🗂 分类检索', search:'🔍 搜索结果',
  detail:'📄 详情页', history:'🕐 播放记录', unknown:'❓ 未知来源'
};
const ACCESS_TYPE_MAP = {
  subscription:'💎 付费订阅', vip_permanent:'👑 永久VIP', points:'🎫 消耗点数',
  points_bonus:'🎁 赠送点数', points_daily:'🗓 每日免费点数', points_mixed:'🎫 混合扣点',
  free:'🆓 免费内容', free_day:'🎉 休市免费日', unlocked_today:'🔓 当日已解锁',
  guest:'🚫 未登录', unknown:'❓ 未知(老数据)'
};
// 视频明细的筛选类型 —— 与服务器 VIDEO_DETAIL_FILTERS 一一对应
const VIDEO_DETAIL_TYPES = {
  online:  { text:'在线播放',       filter:'online',  showSource:true  },
  offline: { text:'离线播放',       filter:'offline', showSource:false },
  vip:     { text:'💎订阅权限播放', filter:'vip',     showSource:true  },
  points:  { text:'🎫消耗点数播放', filter:'points',  showSource:true  },
};

// ================= 安全工具（防 XSS） =================
function esc(s){
  return String(s==null?'':s).replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
// 放进 onclick="fn(...)" 的字符串参数
function jsArg(s){ return esc(JSON.stringify(String(s==null?'':s))); }
function safeUrl(u){ u = String(u||''); return /^https?:\/\//i.test(u) ? esc(u) : ''; }
function cut(s,n){ return esc(String(s==null?'':s).substring(0,n)); }
function fmtTime(s,a,b){ return esc(String(s||'').replace('T',' ').substring(a,b)); }
function emptyRow(cols,text){ return `<tr><td colspan="${cols}" style="text-align:center;color:#64748b">${text}</td></tr>`; }
function idCells(userId, isDevice){
  const id = `<span style="font-family:monospace;font-size:11px">${cut(userId,24)}...</span>`;
  const dash = '<span style="color:#475569">-</span>';
  return isDevice ? `<td>${dash}</td><td>${id}</td>` : `<td>${id}</td><td>${dash}</td>`;
}
function setActiveTab(el){
  el.parentNode.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  el.classList.add('active');
}
function sortRows(rows, field, order){
  return [...rows].sort((a,b)=>{
    let va=a[field], vb=b[field];
    if(field==='last_active'){ va=va||''; vb=vb||''; } else { va=Number(va)||0; vb=Number(vb)||0; }
    if(va<vb) return order==='desc'?1:-1;
    if(va>vb) return order==='desc'?-1:1;
    return 0;
  });
}
function sortArrow(field, cur, order){ return field===cur ? (order==='desc'?'▼':'▲') : ''; }
function accessBadges(str){
  if(!str) return '';
  return String(str).split(',').filter(Boolean).map(t=>{
    const cls = (t==='subscription'||t==='vip_permanent') ? 'pill-green'
              : (t.indexOf('points')===0 ? 'pill-orange' : 'pill-blue');
    return `<span class="pill ${cls}" style="font-size:10px;margin-left:4px;">${esc(ACCESS_TYPE_MAP[t]||t)}</span>`;
  }).join('');
}
function verBadge(versions){
  const v = String(versions||'').split(',').filter(Boolean);
  return v.length ? `<span class="pill pill-purple" style="margin-left:6px;font-size:10px;">v${esc(v.join(' / v'))}</span>` : '';
}

// ================= 悬浮明细卡 =================
const detailCache = {};          // key -> {title(纯文本), html(已转义)}
let hoverTimer = null, hoverKey = null;

function clearDetailCache(){ Object.keys(detailCache).forEach(k=>delete detailCache[k]); }
function ensureHoverCard(){
  let c = document.getElementById('hoverCard');
  if(!c){
    c = document.createElement('div');
    c.id = 'hoverCard'; c.className = 'hover-card'; c.style.display = 'none';
    document.body.appendChild(c);
  }
  return c;
}
function moveHoverCard(e){
  const c = document.getElementById('hoverCard');
  if(!c || c.style.display === 'none') return;
  const pad = 16, w = 420, h = Math.min(window.innerHeight*0.6, 460);
  let x = e.clientX + pad, y = e.clientY + pad;
  if(x + w > window.innerWidth)  x = Math.max(pad, e.clientX - w - pad);
  if(y + h > window.innerHeight) y = Math.max(pad, window.innerHeight - h - pad);
  c.style.left = x + 'px'; c.style.top = y + 'px';
}
function hideHoverCard(){
  if(hoverTimer){ clearTimeout(hoverTimer); hoverTimer = null; }
  hoverKey = null;
  const c = document.getElementById('hoverCard');
  if(c) c.style.display = 'none';
}
function renderHover(c, p){ c.innerHTML = `<div class="hover-title">${esc(p.title)}</div>${p.html}`; }
function hoverShow(e, key, loader){
  const c = ensureHoverCard();
  if(hoverTimer){ clearTimeout(hoverTimer); hoverTimer = null; }
  hoverKey = key;
  c.style.display = 'block';
  moveHoverCard(e);
  if(detailCache[key]){ renderHover(c, detailCache[key]); return; }
  c.innerHTML = '<div class="hover-title">加载中…</div>';
  hoverTimer = setTimeout(async ()=>{
    let res;
    try { res = await loader(); } catch(err){ console.error(err); return; }
    if(!res) return;
    detailCache[key] = res;
    if(hoverKey === key) renderHover(c, res);
  }, 120);
}
function hoverAttrs(key, fnName, args){
  const a = args.map(jsArg).join(',');
  return `onmouseenter="hoverShow(event,${jsArg(key)},()=>${fnName}(${a}))" onmousemove="moveHoverCard(event)" onmouseleave="hideHoverCard()"`;
}

// ================= 基础 =================
async function login(){
  const pwd = document.getElementById('pwdInput').value;
  let r;
  try{
    r = await fetch('/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pwd})});
  }catch(e){ document.getElementById('loginErr').innerText='网络错误'; return; }
  if(!r.ok){
    let msg = '密码错误';
    try{ const d = await r.json(); if(d && d.error) msg = d.error; }catch(e){}
    document.getElementById('loginErr').innerText = msg;
    return;
  }
  const d = await r.json();
  TOKEN = d.token;
  localStorage.setItem('admin_token', TOKEN);
  showDashboard();
}

function showDashboard(){
  document.getElementById('loginBox').style.display='none';
  document.getElementById('dashboard').style.display='block';
  loadCurrentModule();
}

async function api(path, method='GET', body=null){
  const headers = {'X-Admin-Token':TOKEN};
  if(body) headers['Content-Type'] = 'application/json';
  const opts = { method, headers };
  if(body) opts.body = JSON.stringify(body);
  let r;
  try { r = await fetch(path, opts); } catch(e){ console.error('网络错误', path, e); return null; }
  if(r.status===401){
    localStorage.removeItem('admin_token');
    location.reload();
    return null;
  }
  let data = null;
  try { data = await r.json(); } catch(e){ console.error('非 JSON 响应', path, r.status); return null; }
  if(!r.ok && method==='GET'){ console.error('接口错误', path, data); return null; }
  return data;
}

function switchModule(name){
  currentModule = name;
  ['Video','News','Finance'].forEach(n=>{
    const on = name === n.toLowerCase();
    document.getElementById('tab'+n).classList.toggle('active', on);
    document.getElementById('module'+n).classList.toggle('active', on);
  });
  loadCurrentModule();
}

function loadCurrentModule(){
  clearDetailCache();          // 刷新后悬浮/弹窗明细也重新拉取
  hideHoverCard();
  document.getElementById('updateTime').innerText = '更新于 '+new Date().toLocaleTimeString();
  if(currentModule==='video') loadVideoModule();
  else if(currentModule==='news') loadNewsModule();
  else loadFinanceModule();
}

// ================= 在线客服 =================
function switchSupportStatus(el, s, appName){
  setActiveTab(el);
  supportStatus[appName] = s;
  loadSupportThreads(appName);
}

async function loadSupportThreads(appName){
  const body = document.getElementById('supportBody' + appName);
  if(!body) return;
  const data = await api(`/admin/api/support/threads?app=${encodeURIComponent(appName)}&status=${supportStatus[appName]}`);
  if(!Array.isArray(data)) return;
  body.innerHTML = data.length === 0 ? emptyRow(9,'暂无咨询') : data.map((r,i)=>{
    const isDevice = (r.user_type==='device') || String(r.user_id||'').startsWith('dev_');
    let badge;
    if(r.unread_admin > 0)             badge = `<span class="pill pill-orange">待回复 ${Number(r.unread_admin)||0}</span>`;
    else if(r.status === 'resolved')   badge = '<span class="pill pill-green">已完结</span>';
    else if(r.last_sender === 'admin') badge = '<span class="pill pill-blue">已回复</span>';
    else                               badge = '<span class="pill pill-purple">进行中</span>';
    const title = r.title || '用户咨询';
    const who = r.last_sender === 'admin' ? '我' : '用户';
    return `<tr>
      <td>${i+1}</td>
      <td><strong>${esc(title)}</strong><br>
          <span style="font-size:11px;color:#94a3b8">${who}: ${cut(r.last_message,80)}</span></td>
      ${idCells(r.user_id, isDevice)}
      <td><span class="pill pill-blue">${Number(r.msg_count)||0}</span></td>
      <td style="font-size:11px;color:#94a3b8">${r.app_version ? 'v'+esc(r.app_version) : '-'}</td>
      <td style="color:#94a3b8;font-size:12px;white-space:nowrap">${fmtTime(r.updated_at,0,16)}</td>
      <td>${badge}</td>
      <td>
        <span class="clickable" onclick="openSupportChat(${jsArg(r.thread_key)},${jsArg(title)})">💬 对话</span>
        ${r.status!=='resolved' ? `<br><span class="clickable" style="color:#86efac" onclick="resolveSupportThread(${jsArg(r.thread_key)})">✓ 完结</span>` : ''}
      </td>
    </tr>`;
  }).join('');
}

async function resolveSupportThread(key){
  await api('/admin/api/support/resolve','POST',{ thread_key: key });
  loadCurrentModule();
}

// 统一的"聊天历史 + 回复"弹窗（寻片 / 举报 / 客服 共用）
async function openSupportChat(key, title){
  pendingSupportThread = key;
  const data = await api('/admin/api/support/messages?thread_key=' + encodeURIComponent(key));
  const msgs = (data && data.messages) || [];
  const bubbles = msgs.length === 0
    ? '<div style="color:#64748b;text-align:center;padding:20px">暂无历史消息</div>'
    : msgs.map(m=>{
        const t = fmtTime(m.created_at,5,16);
        const c = esc(m.content);
        return m.sender === 'admin'
          ? `<div class="msg admin"><div class="time">${t}</div><div class="bubble">${c}</div></div>`
          : `<div class="msg user"><div class="bubble">${c}</div><div class="time">${t}</div></div>`;
      }).join('');
  const th = data && data.thread;
  const meta = th
    ? `<div style="font-size:11px;color:#94a3b8;margin-bottom:8px">
         类型: ${esc(th.thread_type)} · 用户: <span style="font-family:monospace">${esc(th.user_id)}</span>
         ${th.subtitle ? ' · ' + esc(th.subtitle) : ''}
       </div>` : '';

  document.getElementById('modalTitle').innerText = '💬 ' + (title || '对话记录');
  document.getElementById('modalMsg').innerHTML = `
    <div style="text-align:left">
      ${meta}
      <div class="chat-wrap" id="chatWrap">${bubbles}</div>
      <p style="font-size:12px;color:#94a3b8;margin-bottom:6px">
        输入回复内容（用户下次打开 App 会在客服窗收到）。提示：<b style="color:#86efac">Shift + Enter</b> 直接发送。</p>
      <textarea id="supportReplyInput" rows="3"
        style="width:100%;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:10px;font-size:14px;"
        placeholder="输入回复…"></textarea>
    </div>`;
  const btn = document.getElementById('modalConfirmBtn');
  btn.innerText = '发送回复';
  btn.onclick = submitSupportReply;
  document.getElementById('confirmModal').style.display = 'flex';

  setTimeout(()=>{
    const wrap = document.getElementById('chatWrap');
    if(wrap) wrap.scrollTop = wrap.scrollHeight;
    const ta = document.getElementById('supportReplyInput');
    if(ta){
      ta.focus();
      ta.addEventListener('keydown', e=>{
        if(e.key === 'Enter' && e.shiftKey){ e.preventDefault(); submitSupportReply(); }
      });
    }
  }, 60);
}

async function submitSupportReply(){
  const el = document.getElementById('supportReplyInput');
  const reply = el ? el.value.trim() : '';
  if(!reply || !pendingSupportThread){ closeModal(); return; }
  const r = await api('/admin/api/support/reply','POST', { thread_key: pendingSupportThread, reply: reply });
  closeModal();
  if(r && r.status === 'success') loadCurrentModule();
  else setTimeout(()=>showInfoModal('❌ 发送失败', esc((r && r.error) || '未知错误')), 200);
}

// ================= 视频模块 =================
async function loadVideoModule(){
  // loadVideoOverview();   // 顶部总览卡片已隐藏（需求a），不再请求，省掉 9 条 COUNT 查询
  loadVideoTrend();
  loadTopVideos('play', playPeriod);
  loadTopVideos('download_complete', dlPeriod);
  loadTopUsers();
  loadSupportThreads('ONews');
  loadVideoReports();
  loadVideoWishes();
  loadOnewsInvites();
}

async function loadVideoOverview(){
  const d = await api('/admin/api/overview'); if(!d) return;
  const items = [
    ['总用户数', d.total_users], ['今日活跃', d.today_active_users],
    ['今日播放', d.today_play], ['今日下载', d.today_download],
    ['累计播放', d.total_play_events], ['累计下载', d.total_download_events],
    ['待处理举报', d.pending_reports],
  ];
  document.getElementById('statsBox').innerHTML = items.map(([l,v])=>
    `<div class="stat-card"><div class="label">${l}</div><div class="value">${Number(v)||0}</div></div>`).join('');
}

async function loadVideoTrend(){
  const data = await api('/admin/api/daily_trend'); if(!Array.isArray(data)) return;
  const days=[...new Set(data.map(r=>r.day))].sort();
  const pick = t => days.map(d=>{const r=data.find(x=>x.day===d&&x.event_type===t);return r?r.cnt:0});
  if(trendChart) trendChart.destroy();
  trendChart = new Chart(document.getElementById('trendChart'),{
    type:'line',
    data:{labels:days,datasets:[
      {label:'播放',data:pick('play'),borderColor:'#60a5fa',backgroundColor:'rgba(96,165,250,.15)',tension:.3,fill:true},
      {label:'下载',data:pick('download_complete'),borderColor:'#a78bfa',backgroundColor:'rgba(167,139,250,.15)',tension:.3,fill:true},
    ]},
    options:{responsive:true,plugins:{legend:{labels:{color:'#cbd5e1'}}},scales:{x:{ticks:{color:'#94a3b8'}},y:{ticks:{color:'#94a3b8'}}}}
  });
}

async function loadTopVideos(type, period){
  const data = await api(`/admin/api/top_videos?type=${type}&period=${period}&limit=15`);
  if(!Array.isArray(data)) return;
  const tbody = type==='play'?'topPlayBody':'topDlBody';
  document.getElementById(tbody).innerHTML = data.length===0 ? emptyRow(4,'暂无数据')
    : data.map((r,i)=>`<tr>
        <td>${i+1}</td>
        <td class="clickable" onclick="showVideoUsers(${jsArg(r.video_url)},${jsArg(type)})">${esc(r.video_title||r.video_url)}</td>
        <td><span class="pill pill-green">${Number(r.unique_users)||0}</span></td>
        <td>${Number(r.total_count)||0}</td>
      </tr>`).join('');
}

async function loadTopUsers(){
  const data = await api('/admin/api/top_users');
  if(!Array.isArray(data)) return;
  cachedVideoUsers = data;
  renderVideoUsers();
}

function renderVideoUsers(){
  document.getElementById('vsort_total_actions').innerText = sortArrow('total_actions', videoUserSortField, videoUserSortOrder);
  document.getElementById('vsort_last_active').innerText = sortArrow('last_active', videoUserSortField, videoUserSortOrder);
  const sorted = sortRows(cachedVideoUsers, videoUserSortField, videoUserSortOrder);
  document.getElementById('topUsersBody').innerHTML = sorted.length === 0 ? emptyRow(10,'暂无数据')
    : sorted.map((r, i) => {
        const uid = String(r.user_id||'');
        const isDevice = uid.startsWith('dev_');
        const cell = (type, val, color) => `<td><span class="clickable" style="font-weight:bold;${color||''}"
            onclick="showUserVideoDetails(${jsArg(uid)},${jsArg(type)})"
            ${hoverAttrs('v|'+uid+'|'+type, 'videoDetailPayload', [uid, type])}>${Number(val)||0}</span></td>`;
        return `<tr>
          <td>${i+1}</td>
          ${idCells(uid, isDevice)}
          ${cell('online',  r.online_play)}
          ${cell('offline', r.offline_play)}
          ${cell('download_complete', r.download_videos)}
          ${cell('vip',    r.vip_play,    'color:#86efac;')}
          ${cell('points', r.points_play, 'color:#fdba74;')}
          <td><strong>${Number(r.total_actions)||0}</strong></td>
          <td style="color:#94a3b8;font-size:12px">${fmtTime(r.last_active,0,19)}</td>
        </tr>`;
      }).join('');
}

// 只负责生成内容；点击弹窗和悬浮卡共用。返回 {title: 纯文本, html: 已转义}
async function videoDetailPayload(userId, type){
  const cfg = VIDEO_DETAIL_TYPES[type];
  const sqlType = cfg ? 'play' : type;
  const typeText = cfg ? cfg.text : (type==='play' ? '播放' : '下载');
  const showSource = cfg ? cfg.showSource : false;
  const filter = cfg ? cfg.filter : '';
  const title = `👤 用户 [${userId.substring(0,10)}...] 的${typeText}历史`;
  const data = await api(`/admin/api/video/user_details?user_id=${encodeURIComponent(userId)}&type=${encodeURIComponent(sqlType)}&filter=${encodeURIComponent(filter)}`);
  if(!Array.isArray(data) || data.length === 0) return { title, html:'<div style="color:#64748b">暂无记录</div>' };

  const groups = {};
  data.forEach(item => { const d = item.day || '未知日期'; (groups[d] = groups[d] || []).push(item); });
  let html = `<div style="text-align:left;max-height:56vh;overflow-y:auto;font-size:13px;color:#cbd5e1;">`;
  Object.keys(groups).sort((a,b)=>b.localeCompare(a)).forEach(day => {
    const dayVers = [...new Set(groups[day].flatMap(x => String(x.versions||'').split(',')).filter(Boolean))];
    html += `<div style="margin-bottom:14px;border-bottom:1px solid #334155;padding-bottom:8px;">
      <div style="font-weight:bold;color:#60a5fa;font-size:14px;margin-bottom:6px;">📅 ${esc(day)}${dayVers.length ? ' '+verBadge(dayVers.join(',')) : ''}</div>
      <ul style="list-style:none;padding-left:4px;">`;
    groups[day].forEach(item => {
      const countBadge = item.click_count > 1 ? `<span class="pill pill-blue" style="margin-left:6px;font-size:10px;">${Number(item.click_count)}次</span>` : '';
      let sourceLine = '';
      if (showSource && item.sources) {
        const labels = String(item.sources).split(',').map(s => PLAY_SOURCE_MAP[s] || (s||'未知来源')).join(' / ');
        sourceLine = `<br><span class="pill pill-purple" style="font-size:10px;">📍 ${esc(labels)}</span>`;
      }
      html += `<li style="margin-bottom:8px;line-height:1.4;">
        <strong>${esc(item.video_title || '无标题')}</strong>${countBadge}${accessBadges(item.access_types)}<br>
        <span style="font-size:11px;color:#64748b;word-break:break-all;">${esc(item.video_url)}</span>${sourceLine}<br>
        <span style="font-size:11px;color:#94a3b8;">🕐 ${fmtTime(item.last_time,11,16)}</span></li>`;
    });
    html += `</ul></div>`;
  });
  html += `</div>`;
  return { title, html };
}

function sortVideoUsers(field){
  if (videoUserSortField === field) videoUserSortOrder = videoUserSortOrder === 'desc' ? 'asc' : 'desc';
  else { videoUserSortField = field; videoUserSortOrder = 'desc'; }
  renderVideoUsers();
}

async function showUserVideoDetails(userId, type){
  const key = 'v|'+userId+'|'+type;
  const p = detailCache[key] || await videoDetailPayload(userId, type);
  detailCache[key] = p;
  hideHoverCard();
  showInfoModal(p.title, p.html);
}

async function showVideoUsers(url, type){
  const data = await api(`/admin/api/video_users?video_url=${encodeURIComponent(url)}&type=${encodeURIComponent(type)}`);
  if(!Array.isArray(data)) return;
  const html = data.slice(0,50).map(u=>`• ${cut(u.user_id,25)}... (${Number(u.count)||0}次, 最后:${fmtTime(u.last_at,0,16)})`).join('<br>');
  showInfoModal(`👥 观看此视频的用户 (${data.length} 人)`, html || '暂无');
}

function switchPlayPeriod(el,p){ setActiveTab(el); playPeriod=p; loadTopVideos('play',p); }
function switchDlPeriod(el,p){ setActiveTab(el); dlPeriod=p; loadTopVideos('download_complete',p); }

// ---- 举报 ----
async function loadVideoReports(){
  const data = await api(`/admin/api/video_reports?status=${reportStatus}`);
  if(!Array.isArray(data)) return;
  document.getElementById('reportBody').innerHTML = data.length===0 ? emptyRow(9,'暂无举报')
    : data.map((r,i)=>{
        const typeName = REPORT_TYPE_MAP[r.report_type] || r.report_type;
        const channel = r.channel_name ? `<span class="pill pill-blue">${esc(r.channel_name)}</span>` : '<span style="color:#64748b">-</span>';
        const episode = r.episode_name ? `<span class="pill pill-purple">${esc(r.episode_name)}</span>` : '<span style="color:#64748b">-</span>';
        const href = safeUrl(r.real_url);    // 只允许 http(s)，挡掉 javascript: 链接
        const realLink = href ? `<a href="${href}" target="_blank" rel="noopener noreferrer" style="color:#60a5fa">打开</a>` : '-';
        const noteLine = r.notes ? `<br><span style="font-size:11px;color:#fdba74">📝 ${esc(r.notes)}</span>` : '';
        const replyLine = r.admin_reply ? `<br><span style="font-size:11px;color:#86efac">↳ 回复: ${esc(r.admin_reply)}</span>` : '';
        let stateBadge = '';
        if(r.reply_status==='unread')    stateBadge = ' <span class="pill pill-orange">已回复·待读</span>';
        else if(r.reply_status==='read') stateBadge = ' <span class="pill pill-green">已读</span>';
        const title = r.video_title || '';
        // 与服务器 support_thread_key 的 str(episode_url) 保持一致
        const epForKey = (r.episode_url == null) ? 'None' : r.episode_url;
        const ids = String(r.user_ids || '').split(',').filter(Boolean);
        const idChips = ids.map(u=>{
          const isDev = u.startsWith('dev_');
          const tk = `ONews|report|${u}|${epForKey}`;
          return `<div style="margin-bottom:4px">
            <span class="pill ${isDev?'pill-purple':'pill-blue'}" style="font-size:10px">${isDev?'device':'apple'}</span>
            <span class="clickable" style="font-family:monospace;font-size:10px"
                  onclick="openSupportChat(${jsArg(tk)},${jsArg(title)})">${cut(u,18)}…</span>
          </div>`;
        }).join('') || '<span style="color:#64748b">-</span>';

        return `<tr>
          <td>${i+1}</td>
          <td><strong>${esc(r.video_title||'(未知)')}</strong>${noteLine}${replyLine}<br>
              <span style="font-size:11px;color:#64748b">${cut(r.episode_url,48)}</span></td>
          <td>${channel}</td>
          <td>${episode}</td>
          <td><span class="pill pill-orange">${esc(typeName)}</span>${stateBadge}</td>
          <td>${idChips}<span class="pill pill-green">共 ${Number(r.unique_users)||0} 人</span></td>
          <td>${Number(r.total_count)||0}</td>
          <td>${realLink}</td>
          <td><span class="clickable" onclick="openReportReply(${jsArg(r.episode_url||'')},${jsArg(title)})">✓ 处理/回复</span></td>
        </tr>`;
      }).join('');
}

function switchReportStatus(el,s){ setActiveTab(el); reportStatus=s; loadVideoReports(); }

function openReportReply(episodeUrl, title){
  pendingReportEpisodeUrl = episodeUrl;
  document.getElementById('modalTitle').innerText = "💬 处理举报";
  document.getElementById('modalMsg').innerHTML = `
    <div style="text-align:left">
      <p style="margin-bottom:8px">视频：<strong style="color:#60a5fa">${esc(title)}</strong></p>
      <p style="font-size:12px;color:#94a3b8;margin-bottom:8px">
        可填写回复留言（举报该集的用户下次打开 App 会在首页收到；留空则仅标记"已处理"）。
        <br>提示：按 <b style="color:#86efac">Shift + Enter</b> 可直接提交。</p>
      <textarea id="reportReplyInput" rows="3"
        style="width:100%;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:10px;font-size:14px;"
        placeholder="例如：链接已修复，请重试 / 已更换播放源"></textarea>
    </div>`;
  const btn = document.getElementById('modalConfirmBtn');
  btn.innerText = "提交并标记已处理";
  btn.onclick = submitReportReply;
  document.getElementById('confirmModal').style.display = 'flex';
  setTimeout(() => {
    const ta = document.getElementById('reportReplyInput');
    if (ta) {
      ta.focus();
      ta.addEventListener('keydown', e=>{
        if (e.key === 'Enter' && e.shiftKey) { e.preventDefault(); submitReportReply(); }
      });
    }
  }, 50);
}

async function submitReportReply(){
  const el = document.getElementById('reportReplyInput');
  const reply = el ? el.value : '';
  const r = await api('/admin/api/resolve_report','POST', { episode_url: pendingReportEpisodeUrl, reply: reply });
  closeModal();
  if(r && r.status==='success') loadVideoReports();
}

// ---- 寻片 ----
async function loadVideoWishes(){
  const data = await api(`/admin/api/video_wishes?status=${wishStatus}`);
  if(!Array.isArray(data)) return;
  document.getElementById('wishBody').innerHTML = data.length===0 ? emptyRow(8,'暂无请求')
    : data.map((r,i)=>{
        const isDevice = (r.user_type==='device') || String(r.user_id||'').startsWith('dev_');
        let stateBadge;
        if(r.reply_status==='unread')      stateBadge = '<span class="pill pill-orange">已回复·待读</span>';
        else if(r.reply_status==='read')   stateBadge = '<span class="pill pill-green">已读</span>';
        else if(r.status==='resolved')     stateBadge = '<span class="pill pill-blue">已处理</span>';
        else                               stateBadge = '<span class="pill pill-purple">待处理</span>';
        const wid = Number(r.id)||0;
        const content = r.wish_content || '';
        const actionBtn = `
          ${r.status==='pending' ? `<span class="clickable" onclick="openWishReply(${wid},${jsArg(content)})">✓ 处理/回复</span><br>` : ''}
          <span class="clickable" style="color:#a78bfa"
                onclick="openSupportChat(${jsArg('ONews|wish|'+wid)},${jsArg(content)})">🕘 历史对话</span>`;
        const replyLine = r.admin_reply ? `<br><span style="font-size:11px;color:#86efac">↳ 回复: ${esc(r.admin_reply)}</span>` : '';
        return `<tr>
          <td>${i+1}</td>
          <td><strong>${esc(content)}</strong>${replyLine}</td>
          <td><span style="font-size:12px;color:#94a3b8">${esc(r.keyword||'-')}</span></td>
          <td><span class="pill ${isDevice?'pill-purple':'pill-blue'}">${isDevice?'device':'apple'}</span><br>
              <span style="font-family:monospace;font-size:10px">${cut(r.user_id,16)}...</span></td>
          <td>${Number(r.count)||0}</td>
          <td style="color:#94a3b8;font-size:12px">${fmtTime(r.last_at,0,16)}</td>
          <td>${stateBadge}</td>
          <td>${actionBtn}</td>
        </tr>`;
      }).join('');
}

function switchWishStatus(el,s){ setActiveTab(el); wishStatus=s; loadVideoWishes(); }

function openWishReply(id, content){
  pendingWishId = id;
  document.getElementById('modalTitle').innerText = "💬 处理寻片请求";
  document.getElementById('modalMsg').innerHTML = `
    <div style="text-align:left">
      <p style="margin-bottom:8px">用户想看：<strong style="color:#60a5fa">${esc(content)}</strong></p>
      <p style="font-size:12px;color:#94a3b8;margin-bottom:8px">
        可填写回复留言（用户下次打开 App 会在首页收到；留空则仅标记“已处理”）。
        <br>提示：按 <b style="color:#86efac">Shift + Enter</b> 可直接提交。</p>
      <textarea id="wishReplyInput" rows="3"
        style="width:100%;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:10px;font-size:14px;"
        placeholder="例如：视频已找到，请享用 / 该剧下月上线"></textarea>
    </div>`;
  const btn = document.getElementById('modalConfirmBtn');
  btn.innerText = "提交并标记已处理";
  btn.onclick = submitWishReply;
  document.getElementById('confirmModal').style.display = 'flex';
  setTimeout(() => {
    const ta = document.getElementById('wishReplyInput');
    if (ta) {
      ta.focus();
      ta.addEventListener('keydown', e=>{
        if (e.key === 'Enter' && e.shiftKey) { e.preventDefault(); submitWishReply(); }
      });
    }
  }, 50);
}

async function submitWishReply(){
  const el = document.getElementById('wishReplyInput');
  const reply = el ? el.value : '';
  const r = await api('/admin/api/resolve_wish','POST',{ id: pendingWishId, reply: reply });
  closeModal();
  if(r && r.status==='success') loadVideoWishes();
}

// ---- 新闻/视频邀请 ----
function statCards(boxId, items){
  document.getElementById(boxId).innerHTML = items.map(([l,v])=>
    `<div class="stat-card"><div class="label">${l}</div><div class="value">${Number(v)||0}</div></div>`).join('');
}
function renderInviters(bodyId, rows, totalField){
  document.getElementById(bodyId).innerHTML = rows.length===0 ? emptyRow(6,'暂无数据')
    : rows.map((r,i)=>`<tr>
        <td>${i+1}</td>
        <td><span style="font-family:monospace;font-size:11px">${cut(r.inviter_id,20)}...</span></td>
        <td><span class="pill pill-purple">${esc(r.inviter_code||'-')}</span></td>
        <td><span class="pill pill-green">${Number(r.invite_count)||0}</span></td>
        <td>${Number(r[totalField])||0} 点</td>
        <td style="color:#94a3b8;font-size:12px;white-space:nowrap">${fmtTime(r.last_time,0,16)}</td>
      </tr>`).join('');
}
function renderInviteLogs(bodyId, rows, rewardField){
  document.getElementById(bodyId).innerHTML = rows.length===0 ? emptyRow(6,'暂无记录')
    : rows.map((r,i)=>`<tr>
        <td>${i+1}</td>
        <td><span style="font-family:monospace;font-size:10px">${cut(r.inviter_id,16)}...</span></td>
        <td><span class="pill pill-purple">${esc(r.inviter_code||'-')}</span></td>
        <td><span style="font-family:monospace;font-size:10px">${cut(r.invitee_id,16)}...</span></td>
        <td><span class="pill pill-green">+${Number(r[rewardField])||0}点</span></td>
        <td style="color:#94a3b8;font-size:12px;white-space:nowrap">${fmtTime(r.created_at,0,16)}</td>
      </tr>`).join('');
}

async function loadOnewsInvites(){
  const o = await api('/admin/api/onews/invite_overview');
  if(o) statCards('onewsInviteStatsBox', [
    ['累计邀请成功', o.total_invites], ['今日邀请', o.today_invites],
    ['参与邀请人数', o.unique_inviters], ['累计赠送点数', o.total_reward_points],
  ]);
  const top = await api('/admin/api/onews/top_inviters');
  if(Array.isArray(top)) renderInviters('onewsTopInvitersBody', top, 'total_points');
  const logs = await api('/admin/api/onews/invite_logs');
  if(Array.isArray(logs)) renderInviteLogs('onewsInviteLogsBody', logs, 'reward_points');
}

// ================= 新闻模块 =================
async function loadNewsModule(){
  // loadNewsOverview();   // 总览卡片已隐藏，不再请求
  loadNewsTrend();
  loadTopSources(sourcePeriod);
  loadTopArticles(articleType, articlePeriod);
  await loadTopNewsUsers();
}

async function loadNewsOverview(){
  const d = await api('/admin/api/news/overview'); if(!d) return;
  statCards('newsStatsBox', [
    ['总读者数', d.total_users], ['今日活跃', d.today_active],
    ['今日朗读', d.today_listen], ['今日曝光', d.today_view],
    ['累计朗读', d.total_listen], ['累计曝光', d.total_view],
  ]);
}

async function loadNewsTrend(){
  const data = await api('/admin/api/news/daily_trend'); if(!Array.isArray(data)) return;
  const days = [...new Set(data.map(r=>r.day))].sort();
  const pick = t => days.map(d=>{const r=data.find(x=>x.day===d&&x.event_type===t);return r?r.cnt:0});
  if(newsTrendChart) newsTrendChart.destroy();
  newsTrendChart = new Chart(document.getElementById('newsTrendChart'),{
    type:'line',
    data:{labels:days,datasets:[
      {label:'朗读',data:pick('listen'),borderColor:'#a78bfa',backgroundColor:'rgba(167,139,250,.15)',tension:.3,fill:true},
      {label:'曝光',data:pick('view'),borderColor:'#fb923c',backgroundColor:'rgba(251,146,60,.10)',tension:.3,fill:true},
    ]},
    options:{responsive:true,plugins:{legend:{labels:{color:'#cbd5e1'}}},scales:{x:{ticks:{color:'#94a3b8'}},y:{ticks:{color:'#94a3b8'}}}}
  });
}

async function loadTopSources(period){
  const data = await api(`/admin/api/news/top_sources?period=${period}`); if(!Array.isArray(data)) return;
  document.getElementById('topSourcesBody').innerHTML = data.length===0 ? emptyRow(5,'暂无数据')
    : data.map((r,i)=>`<tr>
        <td>${i+1}</td>
        <td><strong>${esc(r.source_id||'(未知)')}</strong></td>
        <td><span class="pill pill-green">${Number(r.unique_users)||0}</span></td>
        <td><span class="pill pill-blue">${Number(r.unique_articles)||0}</span></td>
        <td>${Number(r.total_reads)||0}</td>
      </tr>`).join('');
}

async function loadTopArticles(type, period){
  const data = await api(`/admin/api/news/top_articles?type=${type}&period=${period}`); if(!Array.isArray(data)) return;
  document.getElementById('topArticlesBody').innerHTML = data.length===0 ? emptyRow(5,'暂无数据')
    : data.map((r,i)=>{
        const title = String(r.article_topic || r.article_key || '');
        const shortTitle = title.length > 40 ? title.substring(0,40)+'...' : title;
        return `<tr>
          <td>${i+1}</td>
          <td class="clickable" onclick="showArticleUsers(${jsArg(r.article_key)},${jsArg(type)})" title="${esc(title)}">${esc(shortTitle)}</td>
          <td><span class="pill pill-orange">${esc(r.source_id||'(未知)')}</span></td>
          <td><span class="pill pill-green">${Number(r.unique_users)||0}</span></td>
          <td>${Number(r.total_count)||0}</td>
        </tr>`;
      }).join('');
}

async function loadTopNewsUsers(){
  const data = await api('/admin/api/news/top_users'); if(!Array.isArray(data)) return;
  cachedNewsUsers = data;
  renderNewsUsers();
}

function renderNewsUsers(){
  document.getElementById('sort_unique_articles').innerText = sortArrow('unique_articles', newsUserSortField, newsUserSortOrder);
  document.getElementById('sort_last_active').innerText = sortArrow('last_active', newsUserSortField, newsUserSortOrder);
  const sorted = sortRows(cachedNewsUsers, newsUserSortField, newsUserSortOrder);
  document.getElementById('topNewsUsersBody').innerHTML = sorted.length===0 ? emptyRow(7,'暂无数据')
    : sorted.map((r,i)=>{
        const uid = String(r.user_id||'');
        const isDevice = r.user_type === 'device' || uid.startsWith('dev_');
        const cell = (type, val) => `<td><span class="clickable" style="font-weight:bold;"
            onclick="showUserNewsDetails(${jsArg(uid)},${jsArg(type)})"
            ${hoverAttrs('n|'+uid+'|'+type, 'newsDetailPayload', [uid, type])}>${Number(val)||0}</span></td>`;
        return `<tr>
          <td>${i+1}</td>
          ${idCells(uid, isDevice)}
          <td><strong>${Number(r.unique_articles)||0}</strong></td>
          ${cell('listen', r.listen_count)}
          ${cell('view',   r.view_count)}
          <td style="color:#94a3b8;font-size:12px">${fmtTime(r.last_active,0,19)}</td>
        </tr>`;
      }).join('');
}

async function newsDetailPayload(userId, type){
  const typeText = type === 'listen' ? '朗读' : '曝光';
  const title = `👥 用户 [${userId.substring(0,10)}...] 的${typeText}历史`;
  const data = await api(`/admin/api/news/user_details?user_id=${encodeURIComponent(userId)}&type=${encodeURIComponent(type)}`);
  if(!Array.isArray(data) || data.length === 0) return { title, html:'<div style="color:#64748b">暂无记录</div>' };
  const groups = {};
  data.forEach(item => {
    let d = String(item.article_date || '未知日期');
    if (/^\d{6}$/.test(d)) d = `20${d.substring(0,2)}-${d.substring(2,4)}-${d.substring(4,6)}`;
    (groups[d] = groups[d] || []).push(item);
  });
  let html = `<div style="text-align:left;max-height:56vh;overflow-y:auto;font-size:13px;color:#cbd5e1;">`;
  Object.keys(groups).sort((a,b)=>b.localeCompare(a)).forEach(date=>{
    html += `<div style="margin-bottom:16px;border-bottom:1px solid #334155;padding-bottom:8px;">
      <div style="font-weight:bold;color:#60a5fa;font-size:14px;margin-bottom:6px;">📅 ${esc(date)}</div><ul style="list-style:none;padding-left:4px;">`;
    groups[date].forEach(art=>{
      const sourceBadge = art.source_id ? `<span class="pill pill-orange" style="margin-right:6px;font-size:10px;padding:1px 4px;">${esc(art.source_id)}</span>` : '';
      const countBadge = art.click_count > 1 ? `<span class="pill pill-blue" style="margin-left:6px;font-size:10px;padding:1px 4px;">${Number(art.click_count)}次</span>` : '';
      html += `<li style="margin-bottom:6px;line-height:1.4;">${sourceBadge}<strong>${esc(art.article_topic || '无标题')}</strong>${countBadge}${verBadge(art.versions)}${accessBadges(art.access_types)}</li>`;
    });
    html += `</ul></div>`;
  });
  html += `</div>`;
  return { title, html };
}

function sortNewsUsers(field){
  if (newsUserSortField === field) newsUserSortOrder = newsUserSortOrder === 'desc' ? 'asc' : 'desc';
  else { newsUserSortField = field; newsUserSortOrder = 'desc'; }
  renderNewsUsers();
}

async function showArticleUsers(key, type){
  const data = await api(`/admin/api/news/article_users?article_key=${encodeURIComponent(key)}&type=${encodeURIComponent(type)}`);
  if(!Array.isArray(data)) return;
  const html = data.slice(0,50).map(u=>
    `• [${esc(u.user_type||'-')}] ${cut(u.user_id,25)}... (${Number(u.count)||0}次, 最后:${fmtTime(u.last_at,0,16)})`).join('<br>');
  showInfoModal(`👥 ${type==='listen'?'朗读':'曝光'}此文章的用户 (${data.length} 人)`, html || '暂无');
}

async function showUserNewsDetails(userId, type){
  const key = 'n|'+userId+'|'+type;
  const p = detailCache[key] || await newsDetailPayload(userId, type);
  detailCache[key] = p;
  hideHoverCard();
  showInfoModal(p.title, p.html);
}

function switchSourcePeriod(el,p){ setActiveTab(el); sourcePeriod=p; loadTopSources(p); }
function switchArticleType(el,t){ setActiveTab(el); articleType=t; loadTopArticles(t,articlePeriod); }
function switchArticlePeriod(el,p){ setActiveTab(el); articlePeriod=p; loadTopArticles(articleType,p); }

// ================= 美股模块 =================
async function loadFinanceModule(){
  // loadFinanceOverview();   // 总览卡片已隐藏，不再请求
  loadSupportThreads('Finance');
  loadFinanceTrend();
  loadFinanceTopCards(financePeriod);
  await loadFinanceUsers();
  loadFinanceInvites();
}

async function loadFinanceOverview(){
  const d = await api('/admin/api/finance/overview'); if(!d) return;
  statCards('financeStatsBox', [
    ['总用户数', d.total_users], ['今日活跃', d.today_active],
    ['今日点击', d.today_clicks], ['各模块总点击量', d.total_clicks],
  ]);
}

async function loadFinanceTrend(){
  const data = await api('/admin/api/finance/daily_trend'); if(!Array.isArray(data)) return;
  if(financeTrendChart) financeTrendChart.destroy();
  financeTrendChart = new Chart(document.getElementById('financeTrendChart'),{
    type:'line',
    data:{labels:data.map(r=>r.day),datasets:[
      {label:'点击量',data:data.map(r=>r.cnt),borderColor:'#60a5fa',backgroundColor:'rgba(96,165,250,.15)',tension:.3,fill:true},
      {label:'活跃用户',data:data.map(r=>r.uu),borderColor:'#34d399',backgroundColor:'rgba(52,211,153,.10)',tension:.3,fill:true},
    ]},
    options:{responsive:true,plugins:{legend:{labels:{color:'#cbd5e1'}}},scales:{x:{ticks:{color:'#94a3b8'}},y:{ticks:{color:'#94a3b8'}}}}
  });
}

async function loadFinanceTopCards(period){
  const data = await api(`/admin/api/finance/top_cards?period=${period}`); if(!Array.isArray(data)) return;
  document.getElementById('financeTopCardsBody').innerHTML = data.length===0 ? emptyRow(4,'暂无数据')
    : data.map((r,i)=>`<tr>
        <td>${i+1}</td>
        <td><strong>${esc(r.card_name||r.card_key)}</strong> <span style="font-size:11px;color:#64748b">${esc(r.card_key)}</span></td>
        <td><span class="pill pill-green">${Number(r.unique_users)||0}</span></td>
        <td>${Number(r.total_count)||0}</td>
      </tr>`).join('');
}

function switchFinancePeriod(el,p){ setActiveTab(el); financePeriod=p; loadFinanceTopCards(p); }

async function loadFinanceUsers(){
  const data = await api('/admin/api/finance/top_users'); if(!Array.isArray(data)) return;
  cachedFinanceUsers = data;
  renderFinanceUsers();
}

async function loadFinanceInvites(){
  const o = await api('/admin/api/finance/invite_overview');
  if(o) statCards('financeInviteStatsBox', [
    ['累计邀请成功', o.total_invites], ['今日邀请', o.today_invites],
    ['参与邀请人数', o.unique_inviters], ['累计赠送点数', o.total_reward_days],
  ]);
  const top = await api('/admin/api/finance/top_inviters');
  if(Array.isArray(top)) renderInviters('financeTopInvitersBody', top, 'total_days');
  const logs = await api('/admin/api/finance/invite_logs');
  if(Array.isArray(logs)) renderInviteLogs('financeInviteLogsBody', logs, 'reward_days');
}

function renderFinanceUsers(){
  ['total_clicks','last_active','points_spent'].forEach(f=>{
    const el = document.getElementById('fsort_'+f);
    if(el) el.innerText = sortArrow(f, financeUserSortField, financeUserSortOrder);
  });
  const sorted = sortRows(cachedFinanceUsers, financeUserSortField, financeUserSortOrder);
  document.getElementById('financeUsersBody').innerHTML = sorted.length===0 ? emptyRow(9,'暂无数据')
    : sorted.map((r,i)=>{
        const uid = String(r.user_id||'');
        const isDevice = (r.user_type==='device') || uid.startsWith('dev_');
        const vip = Number(r.vip_clicks)||0, pts = Number(r.points_clicks)||0;
        let idBadge;
        if(vip > 0 && pts === 0) idBadge = '<span class="pill pill-green">💎 付费用户</span>';
        else if(vip > 0)         idBadge = '<span class="pill pill-blue">🔄 曾付费/混合</span>';
        else if(pts > 0)         idBadge = '<span class="pill pill-orange">🎫 免费(扣点)</span>';
        else                     idBadge = '<span class="pill pill-purple">🆓 免费</span>';
        return `<tr>
          <td>${i+1}</td>
          ${idCells(uid, isDevice)}
          <td>${idBadge}</td>
          <td>${vip}</td>
          <td><span style="color:#fdba74;font-weight:bold">${pts}</span></td>
          <td><span style="color:#fdba74;font-weight:bold">${Number(r.points_spent)||0}</span> 点</td>
          <td><span class="clickable" style="font-weight:bold;"
              onclick="showFinanceUserDetails(${jsArg(uid)})"
              ${hoverAttrs('f|'+uid, 'financeDetailPayload', [uid])}>${Number(r.total_clicks)||0}</span></td>
          <td style="color:#94a3b8;font-size:12px">${fmtTime(r.last_active,0,19)}</td>
        </tr>`;
      }).join('');
}

async function financeDetailPayload(userId){
  const data = await api(`/admin/api/finance/user_details?user_id=${encodeURIComponent(userId)}`);
  const title = `👤 用户 [${userId.substring(0,10)}...] 的点击历史` + (Array.isArray(data) && data.length ? ` (${data.length} 个)` : '');
  if(!Array.isArray(data) || data.length===0) return { title, html:'<div style="color:#64748b">暂无记录</div>' };
  let html = `<div style="text-align:left;max-height:56vh;overflow-y:auto;font-size:13px;color:#cbd5e1;"><ul style="list-style:none;padding-left:4px;">`;
  data.forEach(item=>{
    const countBadge = item.click_count>1 ? `<span class="pill pill-blue" style="margin-left:6px;font-size:10px;">${Number(item.click_count)}次</span>`:'';
    const cost = Number(item.points_cost)||0;
    const costBadge = cost > 0
      ? `<span class="pill pill-orange" style="margin-left:6px;font-size:10px;">-${cost}点</span>`
      : `<span class="pill pill-green" style="margin-left:6px;font-size:10px;">0点</span>`;
    html += `<li style="margin-bottom:10px;line-height:1.4;border-bottom:1px solid #334155;padding-bottom:6px;">
      <strong>${esc(item.card_name||item.card_key)}</strong>${countBadge}${costBadge}${verBadge(item.versions)}${accessBadges(item.access_types)}
      <span style="font-size:11px;color:#64748b">(${esc(item.card_key)})</span><br>
      <span style="font-size:11px;color:#94a3b8;">🕐 ${fmtTime(item.last_time,0,16)}</span></li>`;
  });
  html += `</ul></div>`;
  return { title, html };
}

// 【修复】原来这里定义了两次，后一个把带"扣点/权限"标签的版本覆盖了，只保留这一个
async function showFinanceUserDetails(userId){
  const key = 'f|'+userId;
  const p = detailCache[key] || await financeDetailPayload(userId);
  detailCache[key] = p;
  hideHoverCard();
  showInfoModal(p.title, p.html);
}

function sortFinanceUsers(field){
  if(financeUserSortField===field) financeUserSortOrder = financeUserSortOrder==='desc'?'asc':'desc';
  else { financeUserSortField = field; financeUserSortOrder = 'desc'; }
  renderFinanceUsers();
}

// ================= 通用弹窗 =================
// title 是纯文本；htmlBody 必须是已转义的 HTML
function showInfoModal(title, htmlBody){
  document.getElementById('modalTitle').innerText = title;
  document.getElementById('modalMsg').innerHTML = htmlBody;
  const btn = document.getElementById('modalConfirmBtn');
  btn.innerText = "关闭";
  btn.onclick = closeModal;
  document.getElementById('confirmModal').style.display = 'flex';
}

function triggerClear(type) {
  pendingClearType = type;
  const names = { analytics:"【全部行为统计数据（视频+新闻+美股）】", users:"【所有注册用户账号及订阅权限数据】", all:"【全部数据】" };
  document.getElementById('modalTitle').innerText = "⚠️ 第一次安全确认";
  document.getElementById('modalMsg').innerText = `您正在尝试清空 ${names[type]||''}。此操作不可恢复！`;
  const btn = document.getElementById('modalConfirmBtn');
  btn.innerText = "继续下一步";
  btn.onclick = secondConfirm;
  document.getElementById('confirmModal').style.display = 'flex';
}

function secondConfirm() {
  document.getElementById('modalTitle').innerText = "🚨 终极核对确认";
  document.getElementById('modalMsg').innerText = `请再次确认！如果您十分确定，请点击下方按钮。`;
  const btn = document.getElementById('modalConfirmBtn');
  btn.innerText = "彻底清空并执行";
  btn.onclick = executeClear;
}

async function executeClear() {
  const result = await api('/admin/api/clear_db', 'POST', { type: pendingClearType });
  closeModal();
  if (result && result.status === 'success') {
    loadCurrentModule();
    setTimeout(()=>showInfoModal("✅ 操作成功", esc(result.message)), 300);
  } else {
    setTimeout(()=>showInfoModal("❌ 操作失败", esc((result && result.error) ? result.error : "未知错误")), 300);
  }
}

function closeModal() {
  document.getElementById('confirmModal').style.display = 'none';
}

document.getElementById('pwdInput').addEventListener('keydown', e=>{ if(e.key === 'Enter') login(); });
document.addEventListener('keydown', e=>{ if(e.key === 'Escape') closeModal(); });
if(TOKEN) showDashboard();
</script>
</body>
</html>
'''


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  §7 启动入口                                                          ║
# ╚══════════════════════════════════════════════════════════════════════╝

def init_all():
    # §1 公共
    init_user_db()
    init_anonymous_sub_db()
    init_analytics_base()
    # §2A 点数账本
    init_points_db()
    # 各模块表（必须在客服迁移之前：迁移要读 video_wish_requests / video_link_reports）
    init_ovideo_analytics_db()
    init_onews_analytics_db()
    init_finance_user_db()
    init_finance_analytics_db()
    # §2B 客服
    init_support_db()
    migrate_support_threads_once()
    # §3 OVideo.db
    ensure_video_db(force=True)


if __name__ == '__main__':
    init_all()
    # 启动时也做一次日常维护（清理过期流水 + 截断 WAL + 按需 VACUUM，后台线程执行）
    maybe_cleanup_old_unlocks()
    print("多应用服务器正在启动...")
    print(f"支持的应用: {', '.join(ALLOWED_APPS)}")
    print(f"资源目录被定位在: {BASE_RESOURCES_DIR}")
    host_ip = '0.0.0.0'
    port = 5001
    print("请确保您的手机和电脑连接到同一个Wi-Fi网络")
    print(f"在iOS App中请使用 http://{host_ip}:{port}/api/ONews/... 访问")
    app.run(host=host_ip, port=port, debug=False, threaded=True)
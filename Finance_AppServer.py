import os
import re
import threading
import json
import sqlite3
import traceback
from difflib import SequenceMatcher
from flask import Flask, jsonify, send_from_directory, request, g
from flask_cors import CORS
from flask_compress import Compress
from werkzeug.utils import safe_join
import secrets, hashlib
from functools import wraps
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
# 以北京时间作为"每日免费次数"的统一基准，不再依赖服务器系统时区
APP_TZ = timezone(timedelta(hours=8))
CORS(app)
_last_unlock_cleanup_date = None

# 【新增】初始化 Gzip 压缩
# 这会自动压缩 application/json, text/csv, text/plain 等响应
# 默认压缩级别为 6，足以大幅减小文本文件体积
Compress(app)

# --- 配置 ---
# 获取当前 app.py 所在的目录 (即 LocalServer)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 获取上级目录 (即 /root 或 /Users/yanzhang/Coding)
PARENT_DIR = os.path.dirname(CURRENT_DIR)

BASE_RESOURCES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')

# 活跃用户明细/流水仅保留最近 N 天（可配置）
ANALYTICS_LOG_KEEP_DAYS = 7

ALLOWED_APPS = ['ONews', 'Finance', 'Prediction', 'OVideo']
ALLOWED_EVENT_TYPES = {'play', 'download_complete'}
# 【修改】移除了 'read'，仅保留 view, listen
ALLOWED_NEWS_EVENT_TYPES = {'view', 'listen'}
ALLOWED_REPORT_TYPES = {'playback_failed', 'download_failed', 'media_error', 'content_mismatch', 'other'}
ALLOWED_FINANCE_EVENT_TYPES = {'click'}
report_last_time = {}  # 内存软限流: user_id -> 最近提交时间戳
wish_last_time = {}   # 内存软限流: user_id -> 最近提交时间戳

# 【新增】用户数据库路径
USER_DB_PATH = os.path.join(PARENT_DIR, 'user_data.db')
ANALYTICS_DB_PATH = os.path.join(PARENT_DIR, 'analytics.db')
FINANCE_DB_PATH = os.path.join(BASE_RESOURCES_DIR, 'Finance', 'Finance.db')

# ⚠️ 改成你自己的密码！
ADMIN_PASSWORD_HASH = hashlib.sha256("YourStrongPassword123!".encode()).hexdigest()
ADMIN_TOKENS = set()  # 内存存有效 token，重启失效（简单够用）

# 【新增】简单的邀请码配置 (实际生产中可以放在数据库里)
# 格式: "邀请码": "备注"
VALID_INVITE_CODES = {
    "ONEWS_FAMILY_2024": "Family Access",
    "VIP_FRIEND_888": "Friend Access",
    "DEV_TEST_KEY": "Developer Key"
}

# 视频模块黑名单：这些用户即使是永久 VIP 也看不到视频模块
VIDEO_MODULE_BLOCKED_USERS = {
    "001356.cdec6d350edb4646b0130f9363b6d37e.2149",
}

# Featured 首页「按上映日期」排序时:
# Drama 分类改用 (更新日期 − N 天) 作为排序键,N 可在此调整
FEATURED_DRAMA_DATE_OFFSET_DAYS = 2

# 邀请码字母表：去掉 0/O/1/I/L 等易混字符
INVITE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

def get_finance_config():
    """读取 Finance/version.json 中与点数/邀请相关的配置"""
    path = os.path.join(BASE_RESOURCES_DIR, 'Finance', 'version.json')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
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

# 使用分组独立扣点的动作
_SECTOR_OVERRIDE_ACTIONS = {'open_sector', 'open_special_list', 'view_big_orders'}

def finance_calc_cost(cfg, action, item_key):
    """服务器权威地计算单次扣点"""
    if item_key and action in _SECTOR_OVERRIDE_ACTIONS:
        ov = cfg['sector_cost_overrides'].get(item_key)
        if ov is not None:
            return int(ov)
    return int(cfg['cost_config'].get(action, 1))

def _gen_invite_code(cursor, length=6):
    for _ in range(30):
        code = ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length))
        if not cursor.execute("SELECT 1 FROM finance_points WHERE invite_code=?", (code,)).fetchone():
            return code
    return ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length + 2))

def _ensure_finance_points(c, user_id):
    """确保该用户有点数行；不存在则创建并发放一次性赠送点数；跨天则重置每日额度。"""
    cfg = get_finance_config()
    today = today_str()
    row = c.execute("SELECT * FROM finance_points WHERE user_id=?", (user_id,)).fetchone()
    if row is None:
        code = _gen_invite_code(c)
        bonus = cfg['bonus_points']
        c.execute('''INSERT INTO finance_points
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
    """给某用户一次性发放赠送点数（bonus_remaining 与 bonus_total 同步累加）"""
    _ensure_finance_points(c, user_id)   # 确保点数行存在
    c.execute("""UPDATE finance_points
                 SET bonus_remaining = bonus_remaining + ?,
                     bonus_total     = bonus_total + ?
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

def is_real_login_user(user_id):
    """只有 Apple 登录用户(稳定 Apple ID)才享受免费次数。
       dev_ 开头是设备标识(可被重置)，guest_user 是兜底，都不给。"""
    return bool(user_id) and not user_id.startswith('dev_') and user_id != 'guest_user'

def analytics_cutoff_iso(days=ANALYTICS_LOG_KEEP_DAYS):
    """返回北京时间 N 天前的 naive ISO 字符串，用于与 created_at 比较"""
    return (datetime.now(APP_TZ) - timedelta(days=days)).replace(tzinfo=None).isoformat()


def today_str():
    """统一的"自然日"字符串，永远按北京时间 00:00 切分"""
    return datetime.now(APP_TZ).strftime('%Y-%m-%d')

def now_iso():
    """统一时间戳：北京时间，且不带 +08:00 后缀（naive）。
       这样 SQLite 的 date()/datetime() 不会再把它换算成 UTC，
       date(created_at) 得到的就是北京自然日。"""
    return datetime.now(APP_TZ).replace(tzinfo=None).isoformat()

# --- 数据库连接辅助函数 ---
def require_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = request.headers.get('X-Admin-Token') or request.args.get('token')
        if token not in ADMIN_TOKENS:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

def get_finance_db():
    db = getattr(g, '_finance_database', None)
    if db is None:
        if os.path.exists(FINANCE_DB_PATH):
            db = g._finance_database = sqlite3.connect(FINANCE_DB_PATH, timeout=60.0)
            db.row_factory = sqlite3.Row
        else:
            return None
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_finance_database', None)
    if db is not None:
        db.close()

# --- 用户数据库初始化 (通用) ---
def init_user_db():
    print(f"检查用户数据库: {USER_DB_PATH}")
    # 确保存储目录存在
    os.makedirs(os.path.dirname(USER_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    # 【关键修复】同样开启 WAL，避免额度/登录写入阻塞读取
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    
    # 【核心修改】新的表结构, ，添加了 device_id
    # finance_expire_at: Finance 付费过期时间
    # finance_is_permanent: Finance 永久/亲友 VIP 标记 (0或1)
    # onews_expire_at: ONews 付费过期时间
    # onews_is_permanent: ONews 永久/亲友 VIP 标记 (0或1)
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
    
    # 2. 数据库升级逻辑：针对已经有旧数据库，需要补充新字段的老环境
    # 尝试添加 device_id 列
    try:
        c.execute('ALTER TABLE users ADD COLUMN device_id TEXT')
    except sqlite3.OperationalError:
        pass 

    # 尝试添加 prediction_expire_at 列
    try:
        c.execute('ALTER TABLE users ADD COLUMN prediction_expire_at TIMESTAMP')
    except sqlite3.OperationalError:
        # 如果捕获到 OperationalError，说明这列已经存在了，直接跳过即可
        pass 

    # 尝试添加 prediction_is_permanent 列
    try:
        c.execute('ALTER TABLE users ADD COLUMN prediction_is_permanent INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass

    # 【新增】Finance 点数账本（服务器权威，绑定 Apple ID）
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
    # 【新增】Finance 当日已解锁项（同一项当天再次访问免费，与旧客户端逻辑一致）
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
    print("用户数据库已准备就绪。")



def _gen_points_code(cursor, table, length=6):
    for _ in range(30):
        code = ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length))
        if not cursor.execute(f"SELECT 1 FROM {table} WHERE invite_code=?", (code,)).fetchone():
            return code
    return ''.join(secrets.choice(INVITE_ALPHABET) for _ in range(length + 2))

def _ensure_points(c, table, user_id, cfg, migrate_from=None):
    """确保点数行存在；返回 (row, just_granted)。
       news_points 首次创建时可从旧 onews_points 迁移，避免老用户丢失/重复发放。"""
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
            c.execute(f'''INSERT INTO {table}
                (user_id, invite_code, bonus_remaining, bonus_total, invited_by_code,
                 invite_reward_count, first_login_bonus_granted, created_at)
                VALUES (?,?,?,?,?,?,1,?)''',
                (user_id, code, migrated['bonus_remaining'], migrated['bonus_total'],
                 migrated['invited_by_code'], migrated['invite_reward_count'], now_iso()))
        else:
            bonus = cfg['first_login_bonus']
            granted = 1 if bonus > 0 else 0
            just_granted = bonus > 0
            c.execute(f'''INSERT INTO {table}
                (user_id, invite_code, bonus_remaining, bonus_total, invited_by_code,
                 invite_reward_count, first_login_bonus_granted, created_at)
                VALUES (?,?,?,?,NULL,0,?,?)''',
                (user_id, code, bonus, bonus, granted, now_iso()))
        row = c.execute(f"SELECT * FROM {table} WHERE user_id=?", (user_id,)).fetchone()
    return row, just_granted

def _grant_points_bonus(c, table, user_id, points, cfg):
    migrate = 'onews_points' if table == 'news_points' else None
    _ensure_points(c, table, user_id, cfg, migrate_from=migrate)
    c.execute(f"UPDATE {table} SET bonus_remaining=bonus_remaining+?, bonus_total=bonus_total+? WHERE user_id=?",
              (points, points, user_id))


    
def init_analytics_db():
    print(f"检查行为数据库: {ANALYTICS_DB_PATH}")
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    # 【关键修复】开启 WAL：读写互不阻塞，彻底解决“活跃用户榜”被客户端写入拖死的问题
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
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
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_time ON event_logs(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_type ON event_logs(event_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_events_url ON user_video_events(video_url)')

    c.execute('''
        CREATE TABLE IF NOT EXISTS user_news_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',   -- apple / device
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
    # 【新增】新闻流水表
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
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_time ON news_event_logs(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_source ON news_event_logs(source_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_type ON news_event_logs(event_type)')

    # 【新增】视频免费次数解锁表
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_free_unlocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            episode_key TEXT NOT NULL,
            unlock_date TEXT NOT NULL,      -- 服务器本地日期 YYYY-MM-DD
            video_title TEXT,
            created_at TIMESTAMP NOT NULL,
            UNIQUE(user_id, episode_key, unlock_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_unlock_user_date ON video_free_unlocks(user_id, unlock_date)')

    # 【新增】一次性赠送点数表（新人首登发放，跨天保留，优先消耗）
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_bonus_quota (
            user_id TEXT PRIMARY KEY,
            bonus_total INTEGER NOT NULL,
            bonus_remaining INTEGER NOT NULL,
            granted_at TIMESTAMP NOT NULL
        )
    ''')
    # 【新增】解锁记录标注来源：bonus=一次性赠送 / daily=每日免费（老库默认 daily）
    try:
        c.execute("ALTER TABLE video_free_unlocks ADD COLUMN source TEXT DEFAULT 'daily'")
    except sqlite3.OperationalError:
        pass
    
    # 【新增】给举报表补充回复字段（兼容老库）
    for ddl in [
        "ALTER TABLE video_link_reports ADD COLUMN admin_reply TEXT",
        "ALTER TABLE video_link_reports ADD COLUMN reply_status TEXT DEFAULT 'none'",
        "ALTER TABLE video_link_reports ADD COLUMN replied_at TIMESTAMP",
    ]:
        try:
            c.execute(ddl)
        except sqlite3.OperationalError:
            pass
        
    #【新增】错误链接举报表（补充回复字段，与 wish 一致）
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
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_status ON video_link_reports(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_ep ON video_link_reports(episode_url)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reports_reply ON video_link_reports(user_id, reply_status)')

    # 【新增】给视频统计表补充 user_type 字段（兼容老库）
    try:
        c.execute("ALTER TABLE event_logs ADD COLUMN user_type TEXT DEFAULT 'apple'")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE user_video_events ADD COLUMN user_type TEXT DEFAULT 'apple'")
    except sqlite3.OperationalError:
        pass
    # 【新增】给视频流水表补充 source 字段（播放来源；仅在线播放有值）
    try:
        c.execute("ALTER TABLE event_logs ADD COLUMN source TEXT")
    except sqlite3.OperationalError:
        pass
    
    # 【新增】用户寻片/许愿请求表（含第二阶段的管理员回复字段）
    c.execute('''
        CREATE TABLE IF NOT EXISTS video_wish_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_type TEXT DEFAULT 'apple',
            keyword TEXT,                 -- 用户当时搜索的关键词
            wish_content TEXT NOT NULL,   -- 用户想看的剧集名称等
            app_version TEXT,
            first_at TIMESTAMP NOT NULL,
            last_at TIMESTAMP NOT NULL,
            count INTEGER DEFAULT 1,
            status TEXT DEFAULT 'pending',      -- pending / resolved
            admin_reply TEXT,                   -- 第二阶段：管理员回复内容
            reply_status TEXT DEFAULT 'none',   -- none / unread / read
            replied_at TIMESTAMP,
            UNIQUE(user_id, wish_content)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wish_status ON video_wish_requests(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wish_reply ON video_wish_requests(user_id, reply_status)')
    
    # 【新增】美股(Finance)点击统计：明细表(去重聚合) + 流水表
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
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_time ON finance_event_logs(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_card ON finance_event_logs(card_key)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_logs_user ON finance_event_logs(user_id)')

    # 【新增】Finance 邀请拉新流水
    c.execute('''
        CREATE TABLE IF NOT EXISTS finance_invite_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inviter_id TEXT NOT NULL,
            inviter_code TEXT,
            invitee_id TEXT NOT NULL,
            reward_days INTEGER,
            created_at TIMESTAMP NOT NULL
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_invite_inviter ON finance_invite_logs(inviter_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fin_invite_time ON finance_invite_logs(created_at)')
    
    # 【新增】ONews/Video 共用点数账本（服务器权威，绑定 Apple ID）
    c.execute('''
        CREATE TABLE IF NOT EXISTS onews_points (
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
    # 【新增】新闻解锁表（永久解锁：同一篇解锁后永久免费；每日消耗按 unlock_date 计）
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
    # 【新增】ONews/Video 邀请拉新流水
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

    for tname in ('news_points', 'video_points'):
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

    # 【需求4】给三张流水表补充 app_version（兼容老库）
    for tbl in ('event_logs', 'news_event_logs', 'finance_event_logs'):
        try:
            c.execute(f"ALTER TABLE {tbl} ADD COLUMN app_version TEXT")
        except sqlite3.OperationalError:
            pass

    # 【新增】活跃用户榜是按 user_id 全表分组，加索引避免临时排序、加快聚合
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_user ON event_logs(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_news_logs_user ON news_event_logs(user_id)')
    # finance_event_logs 已有 idx_fin_logs_user

    conn.commit()
    conn.close()
    print("行为数据库已就绪。")




# 概览：今日 / 总计
@app.route('/admin/api/overview', methods=['GET'])
@require_admin
def admin_overview():
    today = today_str()    # ⭐ 北京时间今天
    return jsonify({
        "total_users":          _query_analytics("SELECT COUNT(DISTINCT user_id) AS c FROM event_logs")[0]['c'],
        "total_play_events":    _query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play'")[0]['c'],
        "total_download_events":_query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='download_complete'")[0]['c'],
        "today_active_users":   _query_analytics("SELECT COUNT(DISTINCT user_id) AS c FROM event_logs WHERE date(created_at)=?", (today,))[0]['c'],
        "today_play":           _query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play' AND date(created_at)=?", (today,))[0]['c'],
        "today_download":       _query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='download_complete' AND date(created_at)=?", (today,))[0]['c'],
        "pending_reports":      _query_analytics("SELECT COUNT(DISTINCT episode_url) AS c FROM video_link_reports WHERE status='pending'")[0]['c'],
    })



# 活跃用户排行
@app.route('/admin/api/top_users', methods=['GET'])
@require_admin
def admin_top_users():
    rows = _query_analytics('''
        SELECT user_id,
               MAX(user_type) AS user_type,
               COUNT(DISTINCT CASE WHEN event_type='play' THEN video_url END) AS play_videos,
               COUNT(DISTINCT CASE WHEN event_type='download_complete' THEN video_url END) AS download_videos,
               COUNT(DISTINCT CASE WHEN event_type='play' AND video_url NOT LIKE '%.m3u8' THEN video_url END) AS online_play,
               COUNT(DISTINCT CASE WHEN event_type='play' AND video_url LIKE '%.m3u8' THEN video_url END) AS offline_play,
               COUNT(*) AS total_actions,
               MAX(created_at) AS last_active
        FROM event_logs
        GROUP BY user_id
        ORDER BY total_actions DESC
        LIMIT 50
    ''')
    return jsonify(rows)



# --- ONews API 路由 ---
@app.route('/api/<app_name>/check_version', methods=['GET'])
def check_version(app_name):
    print(f"收到来自应用 '{app_name}' 的版本检查请求")
    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    
    # 获取服务器当前的日期，格式与你的 json 文件一致 (yyMMdd)
    server_now = datetime.now()
    server_date_str = server_now.strftime('%y%m%d')
    
    # 获取原始的 version.json 内容
    version_file_path = os.path.join(BASE_RESOURCES_DIR, app_name, 'version.json')
    if os.path.exists(version_file_path):
        with open(version_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 【关键】动态注入服务器当前日期
        data['server_date'] = server_date_str
        return jsonify(data)
    else:
        return jsonify({"error": "Version file not found"}), 404

# --- 在 AppServer.py 中添加删除账号路由 ---
@app.route('/api/<app_name>/user/delete', methods=['POST'])
def delete_user(app_name):
    data = request.get_json()
    user_id = data.get('user_id')
    
    if not user_id: 
        return jsonify({"error": "Missing user_id"}), 400
        
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    try:
        # 从数据库中永久删除该用户
        c.execute("DELETE FROM users WHERE apple_user_id = ?", (user_id,))
        if c.rowcount == 0:
            return jsonify({"error": "User not found"}), 404
        conn.commit()
        print(f"[{app_name}] 用户 {user_id} 已成功删除账号。")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"删除账号失败: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/<app_name>/download', methods=['GET'])
def download_file(app_name):
    # filename 参数现在可能是 "some.json" 或 "some_dir/some_image.jpg"
    filename = request.args.get('filename')
    print(f"收到来自应用 '{app_name}' 的文件下载请求: {filename}")

    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    if not filename:
        return jsonify({"error": "缺少文件名参数"}), 400

    # --- 核心修改：使用 werkzeug.utils.safe_join 来构建安全路径 ---
    # safe_join 是 Flask/Werkzeug 推荐的、更安全的方式来防止目录遍历攻击
    try:
        # safe_join 会自动处理路径规范化和安全检查
        full_path = safe_join(BASE_RESOURCES_DIR, app_name, filename)
    except Exception:
        # 如果路径包含 '..' 或其他不安全部分，safe_join 会抛出异常
        print(f"错误: 请求的路径不安全: {filename}")
        return jsonify({"error": "无效的路径"}), 400
        
    if not os.path.isfile(full_path):
        print(f"错误: 请求的文件不存在: {full_path}")
        return jsonify({"error": "文件未找到"}), 404

    try:
        # send_from_directory 需要目录和文件名作为分离的参数
        directory, file = os.path.split(full_path)
        print(f"正在发送文件 '{file}' 从目录 '{directory}'")
        return send_from_directory(directory, file, as_attachment=True)
    except Exception as e:
        print(f"发生错误: {e}")
        return jsonify({"error": str(e)}), 500

# --- 用户认证与权限核心逻辑 ---
def check_user_subscription_status(user_row, app_name):
    """
    检查用户权限。
    逻辑：
    1. 先检查该 App 的 is_permanent (亲友/后门)。如果是 1，直接返回 2099年。
    2. 再检查该 App 的 expire_at (付费)。如果时间还没到，返回该时间。
    3. 否则返回 False。
    """
    now = datetime.utcnow()
    
    # 根据传入的 app_name 决定查哪些字段
    # 比如 app_name="Finance" -> prefix="finance"
    prefix = app_name.lower() 
    perm_col = f"{prefix}_is_permanent"
    expire_col = f"{prefix}_expire_at"
    
    # 1. 【优先】检查永久 VIP (亲友/后门)
    # 数据库里取出来可能是 1 或 True，做个兼容
    if user_row[perm_col] == 1:
        # 对于亲友，我们返回一个极远的未来时间，让前端显示“长期有效”或类似效果
        return True, "2099-12-31T23:59:59"
        
    # 2. 检查付费订阅的过期时间
    if user_row[expire_col]:
        try:
            # 数据库存的是字符串，转回 datetime
            expires_at = datetime.fromisoformat(str(user_row[expire_col]))
            if expires_at > now:
                return True, user_row[expire_col]
            else:
                # 【优化】如果已经过期，虽然逻辑上返回 False，
                # 但可以在这里记录一下，或者由 App 端下次登录时更新
                return False, user_row[expire_col]
        except:
            pass
            
    return False, None

# --- 用户认证相关 ---
def handle_auth(app_name):
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        device_id = data.get('device_id') # 【新增】接收客户端传来的设备ID
        
        if not user_id: return jsonify({"error": "Missing user_id"}), 400
        conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,))
        user = c.fetchone()
        now = datetime.utcnow()
        is_subscribed = False
        expiration_date = None
        if user:
            # 老用户：更新登录时间，同时关联/更新最新的 device_id
            c.execute(
                "UPDATE users SET last_login_at = ?, device_id = ? WHERE apple_user_id = ?", 
                (now, device_id, user_id)
            )
            # 检查权限 (传入 app_name)
            is_subscribed, expiration_date = check_user_subscription_status(user, app_name)
        else:
            # 新用户：插入记录，同时写入 device_id
            c.execute(
                "INSERT INTO users (apple_user_id, device_id, created_at, last_login_at) VALUES (?, ?, ?, ?)",
                (user_id, device_id, now, now)
            )
            # 新用户肯定没订阅且不是VIP
        
        conn.commit()
        conn.close()
        return jsonify({
            "status": "success", 
            "is_subscribed": is_subscribed,
            "subscription_expires_at": expiration_date,
            "video_module_blocked": user_id in VIDEO_MODULE_BLOCKED_USERS   # 【新增】
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def handle_status_check(app_name):
    user_id = request.args.get('user_id')
    if not user_id: return jsonify({"error": "Missing user_id"}), 400
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,))
        row = c.fetchone()
        is_subscribed = False
        expires_at_str = None
        if row:
            is_subscribed, expires_at_str = check_user_subscription_status(row, app_name)
        return jsonify({
            "is_subscribed": is_subscribed, 
            "subscription_expires_at": expires_at_str,
            "video_module_blocked": user_id in VIDEO_MODULE_BLOCKED_USERS   # 【新增】
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# 【新增】处理邀请码兑换
def handle_redeem_invite(app_name):
    data = request.get_json()
    user_id = data.get('user_id')
    invite_code = data.get('invite_code')
    if not user_id or not invite_code:
        return jsonify({"error": "缺少参数"}), 400
        
    # 验证邀请码
    if invite_code not in VALID_INVITE_CODES:
        return jsonify({"error": "无效的邀请码"}), 403
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    c = conn.cursor()
    try:
        # 确定要更新哪个字段
        perm_col = f"{app_name.lower()}_is_permanent"
        
        # 设置永久 VIP 标记为 1
        query = f"UPDATE users SET {perm_col} = 1 WHERE apple_user_id = ?"
        c.execute(query, (user_id,))
        if c.rowcount == 0:
            return jsonify({"error": "用户不存在，请先登录"}), 404
        conn.commit()
        print(f"[{app_name}] 用户 {user_id} 使用邀请码 {invite_code} 升级为永久 VIP")
        
        return jsonify({
            "status": "success",
            "is_subscribed": True,
            "subscription_expires_at": "2099-12-31T23:59:59"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

def handle_payment(app_name):
    data = request.get_json()
    user_id = data.get('user_id')
    days = data.get('days', 30) # 保持默认值用于兼容旧版本或手动充值
    # 【新增】接收客户端传来的真实过期时间字符串 (ISO 8601 格式)
    explicit_expiry = data.get('explicit_expiry') 
    if not user_id: return jsonify({"error": "Missing user_id"}), 400
    conn = sqlite3.connect(USER_DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,))
        row = c.fetchone()
        if not row: return jsonify({"error": "User not found"}), 404
        now = datetime.utcnow()
        
        # 确定要更新哪个字段
        expire_col = f"{app_name.lower()}_expire_at"
        new_expiry_str = ""

        # 【核心修改】逻辑分支
        if explicit_expiry:
            # 方案 A: 客户端传了真实的 Apple 过期时间，直接使用
            # 这样就实现了"同步"，而不是"充值"
            print(f"[{app_name}] 同步用户 {user_id} 订阅时间至: {explicit_expiry}")
            new_expiry_str = explicit_expiry
        else:
            # 方案 B: 旧逻辑 (充值模式) - 依然保留以备不时之需
            current_expiry_str = row[expire_col]
            new_expiry = now + timedelta(days=days) 
            
            if current_expiry_str:
                try:
                    current_expiry = datetime.fromisoformat(current_expiry_str)
                    if current_expiry > now:
                        new_expiry = current_expiry + timedelta(days=days)
                except: pass
            new_expiry_str = new_expiry.isoformat()
        
        # 执行更新
        query = f"UPDATE users SET {expire_col} = ? WHERE apple_user_id = ?"
        c.execute(query, (new_expiry_str, user_id))
        conn.commit()
        return jsonify({
            "status": "success", 
            "is_subscribed": True, 
            "subscription_expires_at": new_expiry_str
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


    
@app.route('/admin/login', methods=['POST'])
def admin_login():
    pwd = request.get_json().get('password', '')
    if hashlib.sha256(pwd.encode()).hexdigest() == ADMIN_PASSWORD_HASH:
        token = secrets.token_urlsafe(32)
        ADMIN_TOKENS.add(token)
        return jsonify({"token": token})
    return jsonify({"error": "密码错误"}), 401

def _query_analytics(sql, params=()):
    import time
    last_err = None
    for _ in range(3):                     # 撞锁时最多重试 3 次
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



def _do_invite_redeem(table, cfg_getter):
    data = request.get_json() or {}
    invitee_id = data.get('user_id')
    code = (data.get('invite_code') or '').strip().upper()
    if not is_real_login_user(invitee_id):
        return jsonify({"error": "请先登录后再使用邀请码"}), 401
    if not code:
        return jsonify({"error": "请输入邀请码"}), 400
    cfg = cfg_getter(); reward = cfg['invite_reward_points']
    migrate = 'onews_points' if table == 'news_points' else None
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    inviter_id = None; invitee_bonus = 0; invitee_total = 0
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

        inv_row = c.execute(f"SELECT bonus_remaining FROM {table} WHERE user_id=?", (invitee_id,)).fetchone()
        invitee_bonus = inv_row['bonus_remaining']
        today = today_str()
        du = _news_daily_used(c, invitee_id, today) if table == 'news_points' else _video_daily_used(c, invitee_id, today)
        dr = max(0, cfg['daily_quota'] - du)
        invitee_total = invitee_bonus + dr
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



# --- Finance 路由 ---
@app.route('/api/Finance/auth/apple', methods=['POST'])
def finance_auth(): return handle_auth('Finance')

@app.route('/api/Finance/payment/subscribe', methods=['POST'])
def finance_pay(): return handle_payment('Finance')

@app.route('/api/Finance/user/status', methods=['GET'])
def finance_status(): return handle_status_check('Finance')

# 注册 Finance 的兑换路由！！！
@app.route('/api/Finance/user/redeem', methods=['POST'])
def finance_redeem(): return handle_redeem_invite('Finance')

# Finance 点数账本（服务器权威）
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
        today = today_str()
        keys = [r['item_key'] for r in c.execute(
            "SELECT item_key FROM finance_daily_unlocks WHERE user_id=? AND unlock_date=?",
            (user_id, today)).fetchall()]
        conn.commit()
        daily_remaining = max(0, cfg['daily_free_limit'] - row['daily_used'])
        total = row['bonus_remaining'] + daily_remaining
        return jsonify({
            "logged_in": True,
            "daily_limit": cfg['daily_free_limit'],
            "daily_used": row['daily_used'],
            "bonus_remaining": row['bonus_remaining'],
            "remaining_total": total,
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
    data = request.get_json() or {}
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

        if exists or cost <= 0:
            c.execute("COMMIT")
            return jsonify({"status": "already_unlocked" if exists else "free",
                            "cost": cost, "remaining_total": total,
                            "bonus_remaining": row['bonus_remaining'],
                            "daily_used": row['daily_used'],
                            "daily_limit": cfg['daily_free_limit']})

        if total < cost:
            c.execute("COMMIT")
            return jsonify({"status": "insufficient", "cost": cost, "remaining_total": total,
                            "bonus_remaining": row['bonus_remaining'],
                            "daily_used": row['daily_used'],
                            "daily_limit": cfg['daily_free_limit']})

        remaining_cost = cost
        bonus = row['bonus_remaining']
        use_bonus = min(bonus, remaining_cost)
        bonus -= use_bonus
        remaining_cost -= use_bonus
        daily_used = row['daily_used'] + remaining_cost

        c.execute("UPDATE finance_points SET bonus_remaining=?, daily_used=?, last_date=? WHERE user_id=?",
                  (bonus, daily_used, today, user_id))
        c.execute('''INSERT OR IGNORE INTO finance_daily_unlocks (user_id, item_key, unlock_date, created_at)
                     VALUES (?,?,?,?)''', (user_id, unlock_key, today, now_iso()))
        c.execute("COMMIT")

        daily_remaining = max(0, cfg['daily_free_limit'] - daily_used)
        total = bonus + daily_remaining
        return jsonify({"status": "success", "cost": cost, "remaining_total": total,
                        "bonus_remaining": bonus, "daily_used": daily_used,
                        "daily_limit": cfg['daily_free_limit']})
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        conn.close()

# Finance 邀请拉新
@app.route('/api/Finance/invite/redeem', methods=['POST'])
def finance_invite_redeem():
    data = request.get_json() or {}
    invitee_id = data.get('user_id')
    code = (data.get('invite_code') or '').strip().upper()
    if not is_real_login_user(invitee_id):
        return jsonify({"error": "请先登录后再使用邀请码"}), 401
    if not code:
        return jsonify({"error": "请输入邀请码"}), 400

    cfg = get_finance_config()
    reward_points = cfg['invite_reward_points']

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

        # 【新增】互邀检测：如果对方（inviter）之前正是用「我」的邀请码兑换过，
        # 说明这对好友已经领过一次奖励，禁止反向再领
        if inviter['invited_by_code'] and invitee_row['invite_code'] \
           and inviter['invited_by_code'] == invitee_row['invite_code']:
            c.execute("COMMIT")
            return jsonify({"error": "你们已经互相邀请过啦，每对好友仅能领取一次奖励"}), 403
        
        # 双方各一次性发放 reward_points 赠送点数
        _grant_finance_bonus(c, invitee_id, reward_points)
        _grant_finance_bonus(c, inviter_id, reward_points)
        c.execute("UPDATE finance_points SET invited_by_code=? WHERE user_id=?", (code, invitee_id))
        c.execute("UPDATE finance_points SET invite_reward_count=invite_reward_count+1 WHERE user_id=?", (inviter_id,))

        # 读取被邀请人最新点数，用于返回给客户端即时显示
        inv_row = c.execute("SELECT bonus_remaining, daily_used FROM finance_points WHERE user_id=?",
                            (invitee_id,)).fetchone()
        invitee_bonus = inv_row['bonus_remaining']
        daily_remaining = max(0, cfg['daily_free_limit'] - inv_row['daily_used'])
        invitee_total = invitee_bonus + daily_remaining

        c.execute("COMMIT")
    except Exception as e:
        try: c.execute("ROLLBACK")
        except Exception: pass
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    _log_finance_invite(inviter_id, code, invitee_id, reward_points)
    return jsonify({
        "status": "success",
        "reward_points": reward_points,
        "bonus_remaining": invitee_bonus,
        "remaining_total": invitee_total
    })

# Finance 点击行为上报
@app.route('/api/Finance/track', methods=['POST'])
def track_finance_event():
    try:
        data = request.get_json()
        user_id    = data.get('user_id')
        user_type  = data.get('user_type', 'apple')
        card_key   = data.get('card_key')
        card_name  = data.get('card_name', '')
        event_type = data.get('event_type', 'click')
        app_version = data.get('app_version', '')       # 【新增】
        if not user_id or not card_key or event_type not in ALLOWED_FINANCE_EVENT_TYPES:
            return jsonify({"error": "Invalid params"}), 400
        now = now_iso()   # 北京时间
        conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
        c = conn.cursor()
        c.execute('''
            INSERT INTO user_finance_events
                (user_id, user_type, card_key, card_name, event_type, first_at, last_at, count)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(user_id, card_key, event_type)
            DO UPDATE SET last_at = ?, count = count + 1
        ''', (user_id, user_type, card_key, card_name, event_type, now, now, now))
        c.execute('''
            INSERT INTO finance_event_logs
                (user_id, user_type, card_key, card_name, event_type, created_at, app_version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, user_type, card_key, card_name, event_type, now, app_version))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# 新增：Finance 数据查询 API (替代本地 SQL)

# 1. 获取所有市值数据
@app.route('/api/Finance/query/market_cap', methods=['GET'])
def query_market_cap():
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    try:
        # 对应 fetchAllMarketCapData
        cur = db.execute('SELECT symbol, marketcap, pe_ratio, pb FROM "MNSPP"')
        rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                "symbol": row["symbol"],
                "marketCap": row["marketcap"],
                "peRatio": row["pe_ratio"],
                "pb": row["pb"]
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 2. 获取历史价格数据
@app.route('/api/Finance/query/historical', methods=['GET'])
def query_historical():
    symbol = request.args.get('symbol')
    table_name = request.args.get('table')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    if not all([symbol, table_name, start_date, end_date]):
        return jsonify({"error": "Missing parameters"}), 400
        
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    
    try:
        # 【修改】查询不再包含 id，改为返回所有可能的字段
        # 先检查表结构
        cur = db.execute(f'PRAGMA table_info("{table_name}")')
        columns = [row['name'].lower() for row in cur.fetchall()]
        
        # 构建动态 SELECT 语句
        select_fields = ["date", "price"]
        if "volume" in columns:
            select_fields.append("volume")
        if "open" in columns:
            select_fields.append("open")
        if "high" in columns:
            select_fields.append("high")
        if "low" in columns:
            select_fields.append("low")
        
        select_clause = ", ".join(select_fields)
        
        query = f'''
            SELECT {select_clause} 
            FROM "{table_name}" 
            WHERE name = ? AND date BETWEEN ? AND ? 
            ORDER BY date ASC
        '''
        cur = db.execute(query, (symbol, start_date, end_date))
        rows = cur.fetchall()
        result = []
        for row in rows:
            item = {
                "date": row["date"],
                "price": row["price"]
            }
            # 动态添加存在的字段
            if "volume" in columns and row["volume"] is not None:
                item["volume"] = row["volume"]
            if "open" in columns and row["open"] is not None:
                item["open"] = row["open"]
            if "high" in columns and row["high"] is not None:
                item["high"] = row["high"]
            if "low" in columns and row["low"] is not None:
                item["low"] = row["low"]
            result.append(item)
        return jsonify(result)
    except Exception as e:
        print(f"Error querying historical: {e}")
        return jsonify({"error": str(e)}), 500

# 3. 获取财报数据
@app.route('/api/Finance/query/earning', methods=['GET'])
def query_earning():
    symbol = request.args.get('symbol')
    if not symbol: return jsonify({"error": "Missing symbol"}), 400
    
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    
    try:
        cur = db.execute('SELECT date, price FROM Earning WHERE name = ?', (symbol,))
        rows = cur.fetchall()
        result = [{"date": row["date"], "price": row["price"]} for row in rows]
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 4. 获取单日收盘价
@app.route('/api/Finance/query/closing_price', methods=['GET'])
def query_closing_price():
    symbol = request.args.get('symbol')
    date = request.args.get('date')
    table_name = request.args.get('table')
    if not all([symbol, date, table_name]):
        return jsonify({"error": "Missing parameters"}), 400
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    try:
        query = f'SELECT price FROM "{table_name}" WHERE name = ? AND date = ? LIMIT 1'
        cur = db.execute(query, (symbol, date))
        row = cur.fetchone()
        if row:
            return jsonify({"price": row["price"]})
        else:
            return jsonify({"price": None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 5. 获取最新成交量
@app.route('/api/Finance/query/latest_volume', methods=['GET'])
def query_latest_volume():
    symbol = request.args.get('symbol')
    table_name = request.args.get('table')
    if not all([symbol, table_name]): return jsonify({"error": "Missing parameters"}), 400
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    try:
        # 先检查是否有 volume 列，避免报错
        cur = db.execute(f'PRAGMA table_info("{table_name}")')
        columns = [row['name'].lower() for row in cur.fetchall()]
        if 'volume' not in columns:
             return jsonify({"volume": None})
        query = f'SELECT volume FROM "{table_name}" WHERE name = ? ORDER BY date DESC LIMIT 1'
        cur = db.execute(query, (symbol,))
        row = cur.fetchone()
        if row:
            return jsonify({"volume": row["volume"]})
        else:
            return jsonify({"volume": None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 6. 获取期权 Call/Put 汇总数据 (修改版：支持单体 symbol 或 批量 symbols)
@app.route('/api/Finance/query/options_summary', methods=['GET'])
def query_options_summary():
    # 允许传单个 'symbol' 或 逗号分隔的 'symbols'
    symbol_param = request.args.get('symbol')
    symbols_param = request.args.get('symbols')
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500

    # 统一构建待查询列表
    target_symbols = []
    if symbols_param:
        target_symbols = [s.strip() for s in symbols_param.split(',') if s.strip()]
    elif symbol_param:
        target_symbols = [symbol_param]
    if not target_symbols:
        return jsonify({"error": "Missing parameters"}), 400
    try:
        results = {}
        
        # 遍历查询 (虽然是循环，但比 HTTP 开销小得多，且本地 SQLite 很快)
        # 如果追求极致性能可以用 SQL 的 IN 查询，但这里为了逻辑复用，循环足够了
        for sym in target_symbols:
            query = '''
                SELECT call, put, price, change, iv, date 
                FROM "Options" 
                WHERE name = ? 
                ORDER BY date DESC 
                LIMIT 2
            '''
            cur = db.execute(query, (sym,))
            rows = cur.fetchall()
            if rows:
                latest_row = rows[0]
                prev_row = rows[1] if len(rows) > 1 else None
                results[sym] = {
                    "call": latest_row["call"],
                    "put": latest_row["put"],
                    "price": latest_row["price"],
                    "change": latest_row["change"],
                    "iv": latest_row["iv"],
                    "date": latest_row["date"],
                    "prev_iv": prev_row["iv"] if prev_row else None,
                    "prev_price": prev_row["price"] if prev_row else None,
                    "prev_change": prev_row["change"] if prev_row else None
                }
            else:
                # 没数据就不放进结果，或者放个 None
                pass

        # 如果是单查，为了兼容旧逻辑，直接返回对象；如果是批量，返回字典
        if symbols_param:
            return jsonify(results)
        else:
            # 保持兼容旧 API 的返回格式
            if target_symbols[0] in results:
                return jsonify(results[target_symbols[0]])
            else:
                return jsonify({
                    "call": None, "put": None, 
                    "price": None, "change": None, 
                    "iv": None, "date": None,
                    "prev_iv": None, "prev_price": None, "prev_change": None
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 7. 获取期权历史价格走势 (新增)
@app.route('/api/Finance/query/options_price_history', methods=['GET'])
def query_options_price_history():
    symbol = request.args.get('symbol')
    
    if not symbol: return jsonify({"error": "Missing parameters"}), 400
    
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    
    try:
        # 【修改点】增加了 iv 字段的查询
        query = 'SELECT date, price, iv FROM "Options" WHERE name = ? ORDER BY date DESC'
        cur = db.execute(query, (symbol,))
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            result.append({
                "date": row["date"],
                "price": row["price"],
                "iv": row["iv"] # 新增返回 IV (字符串格式, 如 "50.5%")
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 8. 获取期权榜单 (修改 - Options Rank)
# 逻辑：利用数据库 change 字段，移除 Self-Join，极大提高性能
@app.route('/api/Finance/query/options_rank', methods=['GET'])
def query_options_rank():
    # 获取客户端传来的市值阀值，如果没有传则默认 500亿
    limit = request.args.get('limit', default=50000000000, type=float)
    db = get_finance_db()
    if not db: return jsonify({"error": "Database not found"}), 500
    
    try:
        # 1. 找到 Options 表中最新的两个日期
        cur = db.execute('SELECT DISTINCT date FROM "Options" ORDER BY date DESC LIMIT 2')
        date_rows = cur.fetchall()
        if not date_rows:
             return jsonify({"rank_up": [], "rank_down": []})
        
        latest_date = date_rows[0]['date']
        # 如果有次新日期则获取，否则为 None
        prev_date = date_rows[1]['date'] if len(date_rows) > 1 else None
        
        # 2. SQL 查询：Join 自身获取 Latest 和 Prev 的 IV 以及 价格数据
        # 【修改点】增加了 t1.price, t1.change, t2.price, t2.change
        sql = '''
            SELECT 
                t1.name as symbol, 
                t1.iv as iv_latest,
                t1.price as price_latest,
                t1.change as change_latest,
                t2.iv as iv_prev,
                t2.price as price_prev,
                t2.change as change_prev,
                m.marketcap
            FROM "Options" t1
            LEFT JOIN "Options" t2 ON t1.name = t2.name AND t2.date = ?
            JOIN "MNSPP" m ON t1.name = m.symbol
            WHERE t1.date = ? 
              AND m.marketcap > ?
              AND t1.iv IS NOT NULL
        '''
        
        # 注意参数顺序：prev_date, latest_date, limit
        cur = db.execute(sql, (prev_date, latest_date, limit))
        rows = cur.fetchall()
        
        all_results = []
        for r in rows:
            # 解析 IV 字符串为浮点数用于排序 (去除 % 号)
            raw_iv_latest = r["iv_latest"]
            sort_val = 0.0
            
            if raw_iv_latest:
                try:
                    clean_str = raw_iv_latest.replace('%', '').strip()
                    sort_val = float(clean_str)
                except:
                    sort_val = 0.0
            
            all_results.append({
                "symbol": r["symbol"],
                "iv": raw_iv_latest,       # 第一项显示 (Latest IV)
                "prev_iv": r["iv_prev"],   # 第二项显示 (Prev IV)
                
                # 【新增】返回价格数据
                "price": r["price_latest"],
                "change": r["change_latest"],
                "prev_price": r["price_prev"],
                "prev_change": r["change_prev"],

                "sort_val": sort_val       # 用于后端排序
            })
            
        # 3. 排序规则：按照 Latest IV (sort_val) 降序排列
        all_results.sort(key=lambda x: x["sort_val"], reverse=True)
            
        if not all_results:
             return jsonify({"rank_up": [], "rank_down": []})
             
        # 截取前20
        rank_up = all_results[:20]
        
        # 截取后20 (IV 最小的)
        rank_down = all_results[-20:]
        rank_down.reverse() 
        
        # 清理掉 sort_val 字段再返回
        for item in rank_up + rank_down:
            item.pop("sort_val", None)

        return jsonify({
            "rank_up": rank_up,
            "rank_down": rank_down
        })
        
    except Exception as e:
        print(f"Error querying options rank: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================

# 美股 - 总览
@app.route('/admin/api/finance/overview', methods=['GET'])
@require_admin
def admin_finance_overview():
    today = today_str()
    return jsonify({
        "total_users":  _query_analytics("SELECT COUNT(DISTINCT user_id) c FROM finance_event_logs")[0]['c'],
        "today_active": _query_analytics("SELECT COUNT(DISTINCT user_id) c FROM finance_event_logs WHERE date(created_at)=?", (today,))[0]['c'],
        "today_clicks": _query_analytics("SELECT COUNT(*) c FROM finance_event_logs WHERE date(created_at)=?", (today,))[0]['c'],
        "total_clicks": _query_analytics("SELECT COUNT(*) c FROM finance_event_logs")[0]['c'],
    })

# 美股 - 活跃用户榜
@app.route('/admin/api/finance/top_users', methods=['GET'])
@require_admin
def admin_finance_top_users():
    rows = _query_analytics('''
        SELECT user_id,
               MAX(user_type) AS user_type,
               COUNT(DISTINCT card_key) AS unique_cards,
               COUNT(*) AS total_clicks,
               MAX(created_at) AS last_active
        FROM finance_event_logs
        GROUP BY user_id
        ORDER BY total_clicks DESC
        LIMIT 50
    ''')
    return jsonify(rows)

# 美股 - 某用户点击明细
@app.route('/admin/api/finance/user_details', methods=['GET'])
@require_admin
def admin_finance_user_details():
    maybe_cleanup_old_unlocks()                      # 【新增】
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400
    cutoff = analytics_cutoff_iso()                  # 【新增】
    sql = '''
        SELECT card_key, MAX(card_name) AS card_name,
               MAX(created_at) AS last_time,
               COUNT(*) AS click_count,
               GROUP_CONCAT(DISTINCT NULLIF(app_version,'')) AS versions
        FROM finance_event_logs
        WHERE user_id = ? AND created_at >= ?
        GROUP BY card_key
        ORDER BY last_time DESC
    '''
    return jsonify(_query_analytics(sql, (user_id, cutoff)))

# 美股 - 30天趋势
@app.route('/admin/api/finance/daily_trend', methods=['GET'])
@require_admin
def admin_finance_daily_trend():
    rows = _query_analytics('''
        SELECT date(created_at) AS day,
               COUNT(*) AS cnt,
               COUNT(DISTINCT user_id) AS uu
        FROM finance_event_logs
        WHERE created_at >= datetime('now', '+8 hours', '-30 days')
        GROUP BY day
        ORDER BY day ASC
    ''')
    return jsonify(rows)

# 美股 - 模块热度榜
@app.route('/admin/api/finance/top_cards', methods=['GET'])
@require_admin
def admin_finance_top_cards():
    period = request.args.get('period', '7d')
    where = ""
    if period == 'today':
        where = "AND date(created_at) = date('now', '+8 hours')"
    elif period == '7d':
        where = "AND created_at >= datetime('now', '+8 hours', '-7 days')"
    sql = f'''
        SELECT card_key, MAX(card_name) AS card_name,
               COUNT(DISTINCT user_id) AS unique_users,
               COUNT(*) AS total_count
        FROM finance_event_logs
        WHERE 1=1 {where}
        GROUP BY card_key
        ORDER BY total_count DESC
        LIMIT 60
    '''
    return jsonify(_query_analytics(sql))

@app.route('/admin/api/finance/invite_overview', methods=['GET'])
@require_admin
def admin_finance_invite_overview():
    today = today_str()
    return jsonify({
        "total_invites":   _query_analytics("SELECT COUNT(*) c FROM finance_invite_logs")[0]['c'],
        "today_invites":   _query_analytics("SELECT COUNT(*) c FROM finance_invite_logs WHERE date(created_at)=?", (today,))[0]['c'],
        "unique_inviters": _query_analytics("SELECT COUNT(DISTINCT inviter_id) c FROM finance_invite_logs")[0]['c'],
        "total_reward_days": _query_analytics("SELECT COALESCE(SUM(reward_days),0) c FROM finance_invite_logs")[0]['c'],
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



# --- 服务器启动 ---
if __name__ == '__main__':
    # 【新增】在启动时初始化数据库
    init_user_db()
    init_analytics_db()
    supported_apps_str = ", ".join(ALLOWED_APPS)
    print("多应用服务器正在启动...")
    print(f"支持的应用: {supported_apps_str}")
    print(f"资源目录被定位在: {BASE_RESOURCES_DIR}")
    host_ip = '0.0.0.0'
    port = 5001
    print("请确保您的手机和电脑连接到同一个Wi-Fi网络")
    print(f"在iOS App中请使用 http://{host_ip}:{port}/api/ONews/... 访问")
    app.run(host=host_ip, port=port, debug=False, threaded=True)
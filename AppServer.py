import os
import json
import sqlite3
import traceback
from flask import Flask, jsonify, send_from_directory, request, g
from flask_cors import CORS
from flask_compress import Compress
from werkzeug.utils import safe_join
from datetime import datetime, timedelta
import secrets, hashlib
from functools import wraps

app = Flask(__name__)
CORS(app)

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
ALLOWED_APPS = ['ONews', 'Finance', 'Prediction', 'OVideo']
ALLOWED_EVENT_TYPES = {'play', 'download_complete'}

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
    
    # 【核心修改】新的表结构
    # finance_expire_at: Finance 付费过期时间
    # finance_is_permanent: Finance 永久/亲友 VIP 标记 (0或1)
    # onews_expire_at: ONews 付费过期时间
    # onews_is_permanent: ONews 永久/亲友 VIP 标记 (0或1)
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apple_user_id TEXT NOT NULL UNIQUE,
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

    conn.commit()
    conn.close()
    print("用户数据库（新结构）已准备就绪。")

def init_analytics_db():
    print(f"检查行为数据库: {ANALYTICS_DB_PATH}")
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=60.0)
    c = conn.cursor()
    # 移除了 category 字段
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
    conn.commit()
    conn.close()
    print("行为数据库已就绪。")

# --- API 路由 ---
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

# ==========================================
# 新增：Finance 数据查询 API (替代本地 SQL)
# ==========================================

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
        # email 和 full_name 我们不再获取也不再存储
        
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
            # 老用户：更新登录时间
            c.execute("UPDATE users SET last_login_at = ? WHERE apple_user_id = ?", (now, user_id))
            # 检查权限 (传入 app_name)
            is_subscribed, expiration_date = check_user_subscription_status(user, app_name)
        else:
            # 新用户：插入记录。注意这里不需要记录 app_source 了，因为 apple_id 唯一
            c.execute(
                "INSERT INTO users (apple_user_id, created_at, last_login_at) VALUES (?, ?, ?)",
                (user_id, now, now)
            )
            # 新用户肯定没订阅且不是VIP
        
        conn.commit()
        conn.close()
        return jsonify({
            "status": "success", 
            "is_subscribed": is_subscribed,
            "subscription_expires_at": expiration_date
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
            # 【修改】使用统一的检查逻辑
            is_subscribed, expires_at_str = check_user_subscription_status(row, app_name)
        return jsonify({"is_subscribed": is_subscribed, "subscription_expires_at": expires_at_str})
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

# --- ONews 路由 (保持兼容) ---
@app.route('/api/ONews/auth/apple', methods=['POST'])
def onews_auth(): return handle_auth('ONews')

@app.route('/api/ONews/payment/subscribe', methods=['POST'])
def onews_pay(): return handle_payment('ONews')

# 注意状态检查也要传 App 名，因为我们要看特定 App 的权限
@app.route('/api/ONews/user/status', methods=['GET'])
def onews_status(): 
    # 这里复用 handle_auth 里的 check 逻辑，稍微改写一下 handle_status_check
    return handle_status_check('ONews') 

# ONews 兑换路由
@app.route('/api/ONews/user/redeem', methods=['POST'])
def onews_redeem(): return handle_redeem_invite('ONews')

# --- Prediction 路由 ---
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


# ==========================================
# OVideo 视频模块 API
# ==========================================
OVIDEO_DIR = os.path.join(BASE_RESOURCES_DIR, 'OVideo')
OVIDEO_COVER_DIR = os.path.join(OVIDEO_DIR, 'cover_image')

# 1. 获取视频目录（保证分类顺序 Movie/Drama/Show/Anime ...）
# 【修改】只显示在 url_mapping.json 中存在真实播放链接的剧集
@app.route('/api/OVideo/videos', methods=['GET'])
def get_ovideos():
    video_file = os.path.join(OVIDEO_DIR, 'OVideos.json')
    mapping_file = os.path.join(OVIDEO_DIR, 'url_mapping.json')
    
    if not os.path.exists(video_file):
        return jsonify({"error": "Video file not found"}), 404
        
    try:
        # 1. 读取原始视频数据
        with open(video_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 2. 读取 url_mapping 数据，用于过滤
        valid_urls = set()
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f_map:
                mappings = json.load(f_map)
                # 只有 mapping 中值不为空的 URL 才是有效的
                valid_urls = set(mappings.keys())

        # 3. 将 dict 转为有序列表，并在转换过程中过滤 playlist
        categories = []
        for key, value in data.items():
            filtered_items = []
            for item in value:
                # 深拷贝或重建 item，避免修改内存中的原始 dict 缓存
                new_item = dict(item)
                filtered_playlist = []
                
                if 'playlist' in item:
                    for channel in item['playlist']:
                        # 【核心修改】：如果 url 在 mapping 中，或者 url 本身包含 .m3u8，都视作有效
                        filtered_episodes = {
                            ep_name: ep_url 
                            for ep_name, ep_url in channel.get('episodes', {}).items()
                            if ep_url in valid_urls or '.m3u8' in ep_url.lower()
                        }
                        
                        # 如果过滤后该播放源还有剧集，则保留该播放源
                        if filtered_episodes:
                            new_channel = dict(channel)
                            new_channel['episodes'] = filtered_episodes
                            filtered_playlist.append(new_channel)
                
                # 更新 item 的播放列表
                new_item['playlist'] = filtered_playlist
                
                # 选项：如果你希望“没有任何可用播放源”的视频直接不显示，可以加判断：
                # if filtered_playlist: filtered_items.append(new_item)
                # 否则保留视频，详情页会显示“暂无可用资源”
                filtered_items.append(new_item)
                
            categories.append({"name": key, "items": filtered_items})
            
        return jsonify({"categories": categories})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# 2. 获取封面图片
@app.route('/api/OVideo/cover/<path:filename>', methods=['GET'])
def get_ovideo_cover(filename):
    try:
        safe_path = safe_join(OVIDEO_COVER_DIR, filename)
    except Exception:
        return jsonify({"error": "Invalid path"}), 400
    if not safe_path or not os.path.isfile(safe_path):
        return jsonify({"error": "Image not found"}), 404
    directory, file = os.path.split(safe_path)
    # 加个缓存头，减少 App 反复拉图片
    response = send_from_directory(directory, file)
    response.headers['Cache-Control'] = 'public, max-age=604800'  # 7天
    return response

# 3. 解析页面 URL -> 真实 m3u8（同时做黑名单拦截）
@app.route('/api/OVideo/resolve', methods=['POST'])
def resolve_ovideo_url():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing body"}), 400
    episode_url = data.get('url')
    if not episode_url:
        return jsonify({"error": "Missing url"}), 400

    # 【核心修改】：如果是直接写在 json 里的 m3u8 链接，直接返回它自己，跳过 mapping 检索
    if '.m3u8' in episode_url.lower():
        return jsonify({
            "real_url": episode_url,
            "title": ""
        })

    blacklist_file = os.path.join(OVIDEO_DIR, 'blacklist_url.json')
    if os.path.exists(blacklist_file):
        try:
            with open(blacklist_file, 'r', encoding='utf-8') as f:
                blacklist = json.load(f)
            if episode_url in blacklist:
                return jsonify({"error": "Blacklisted", "reason": "该视频暂不可用"}), 403
        except Exception as e:
            print(f"黑名单读取失败: {e}")

    # 映射表
    mapping_file = os.path.join(OVIDEO_DIR, 'url_mapping.json')
    if not os.path.exists(mapping_file):
        return jsonify({"error": "Mapping file not found"}), 404

    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
        if episode_url in mappings:
            mapping_data = mappings[episode_url]
            if isinstance(mapping_data, list) and len(mapping_data) > 0:
                return jsonify({
                    "real_url": mapping_data[0],
                    "title": mapping_data[1] if len(mapping_data) > 1 else ""
                })
        return jsonify({"error": "URL not found in mapping"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 4. 服务端搜索（可选，客户端也可以自己搜）
@app.route('/api/OVideo/search', methods=['GET'])
def search_ovideo():
    keyword = request.args.get('q', '').strip().lower()
    if not keyword:
        return jsonify({"results": []})
    video_file = os.path.join(OVIDEO_DIR, 'OVideos.json')
    if not os.path.exists(video_file):
        return jsonify({"results": []})
    try:
        with open(video_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        results = []
        for category_name, items in data.items():
            for item in items:
                name = item.get('name', '').lower()
                director = (item.get('导演') or '').lower()
                cast = ' '.join(item.get('主演') or []).lower()
                intro = (item.get('intro') or '').lower()
                if (keyword in name or keyword in director
                        or keyword in cast or keyword in intro):
                    result_item = dict(item)
                    result_item['category'] = category_name
                    results.append(result_item)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/api/OVideo/track', methods=['POST'])
def track_event():
    try:
        data = request.get_json()
        user_id     = data.get('user_id')
        video_url   = data.get('video_url')
        video_title = data.get('video_title', '')
        event_type  = data.get('event_type')  # 移除了 category

        if not user_id or not video_url or event_type not in ALLOWED_EVENT_TYPES:
            return jsonify({"error": "Invalid params"}), 400

        now = datetime.utcnow().isoformat()
        conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
        c = conn.cursor()

        # 移除了 category 字段的插入
        c.execute('''
            INSERT INTO user_video_events
                (user_id, video_url, video_title, event_type, first_at, last_at, count)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(user_id, video_url, event_type)
            DO UPDATE SET last_at = ?, count = count + 1
        ''', (user_id, video_url, video_title, event_type, now, now, now))

        # 2. 流水表：每次都插
        c.execute('''
            INSERT INTO event_logs
                (user_id, video_url, video_title, event_type, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, video_url, video_title, event_type, now))

        conn.commit()
        conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    
@app.route('/admin/login', methods=['POST'])
def admin_login():
    pwd = request.get_json().get('password', '')
    if hashlib.sha256(pwd.encode()).hexdigest() == ADMIN_PASSWORD_HASH:
        token = secrets.token_urlsafe(32)
        ADMIN_TOKENS.add(token)
        return jsonify({"token": token})
    return jsonify({"error": "密码错误"}), 401


def _query_analytics(sql, params=()):
    conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# 概览：今日 / 总计
@app.route('/admin/api/overview', methods=['GET'])
@require_admin
def admin_overview():
    today = datetime.utcnow().strftime('%Y-%m-%d')
    return jsonify({
        "total_users":          _query_analytics("SELECT COUNT(DISTINCT user_id) AS c FROM event_logs")[0]['c'],
        "total_play_events":    _query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play'")[0]['c'],
        "total_download_events":_query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='download_complete'")[0]['c'],
        "today_active_users":   _query_analytics("SELECT COUNT(DISTINCT user_id) AS c FROM event_logs WHERE date(created_at)=?", (today,))[0]['c'],
        "today_play":           _query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='play' AND date(created_at)=?", (today,))[0]['c'],
        "today_download":       _query_analytics("SELECT COUNT(*) AS c FROM event_logs WHERE event_type='download_complete' AND date(created_at)=?", (today,))[0]['c'],
    })

# 视频排行榜（区分唯一用户数 / 总次数）
@app.route('/admin/api/top_videos', methods=['GET'])
@require_admin
def admin_top_videos():
    event_type = request.args.get('type', 'play')   # play / download_complete
    period = request.args.get('period', 'all')      # today / 7d / all
    limit = int(request.args.get('limit', 20))

    where_time = ""
    params = [event_type]
    if period == 'today':
        where_time = "AND date(created_at) = date('now')"
    elif period == '7d':
        where_time = "AND created_at >= datetime('now', '-7 days')"

    # 用流水表统计：唯一用户数 + 总触发次数
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
    params.append(limit)
    return jsonify(_query_analytics(sql, params))

# 某个视频的观看用户列表
@app.route('/admin/api/video_users', methods=['GET'])
@require_admin
def admin_video_users():
    video_url = request.args.get('video_url')
    event_type = request.args.get('type', 'play')
    rows = _query_analytics('''
        SELECT user_id, first_at, last_at, count
        FROM user_video_events
        WHERE video_url = ? AND event_type = ?
        ORDER BY last_at DESC
    ''', (video_url, event_type))
    return jsonify(rows)

# 每日趋势（最近 30 天）
@app.route('/admin/api/daily_trend', methods=['GET'])
@require_admin
def admin_daily_trend():
    rows = _query_analytics('''
        SELECT date(created_at) AS day,
               event_type,
               COUNT(*) AS cnt,
               COUNT(DISTINCT user_id) AS uu
        FROM event_logs
        WHERE created_at >= datetime('now', '-30 days')
        GROUP BY day, event_type
        ORDER BY day ASC
    ''')
    return jsonify(rows)

# 活跃用户排行
@app.route('/admin/api/top_users', methods=['GET'])
@require_admin
def admin_top_users():
    rows = _query_analytics('''
        SELECT user_id,
               COUNT(DISTINCT video_url) AS unique_videos,
               COUNT(*) AS total_actions,
               MAX(created_at) AS last_active
        FROM event_logs
        GROUP BY user_id
        ORDER BY unique_videos DESC
        LIMIT 50
    ''')
    return jsonify(rows)

# 【新增】一键清除数据库 API
@app.route('/admin/api/clear_db', methods=['POST'])
@require_admin
def admin_clear_db():
    data = request.get_json() or {}
    clear_type = data.get('type')  # 'analytics', 'users', 'all'
    
    if clear_type not in ['analytics', 'users', 'all']:
        return jsonify({"error": "无效的清除类型"}), 400
        
    try:
        # 1. 清除行为统计数据
        if clear_type in ['analytics', 'all']:
            conn = sqlite3.connect(ANALYTICS_DB_PATH, timeout=30.0)
            c = conn.cursor()
            c.execute("DELETE FROM user_video_events")
            c.execute("DELETE FROM event_logs")
            conn.commit()
            conn.close()
            
        # 2. 清除用户及订阅数据
        if clear_type in ['users', 'all']:
            conn = sqlite3.connect(USER_DB_PATH, timeout=30.0)
            c = conn.cursor()
            c.execute("DELETE FROM users")
            conn.commit()
            conn.close()
            
        return jsonify({"status": "success", "message": f"成功清空了 {clear_type} 相关的数据。"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Dashboard 网页本体
@app.route('/admin', methods=['GET'])
def admin_page():
    return ADMIN_HTML  # 见下方

ADMIN_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OVideo 行为监控</title>
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
  .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}
  .header h1{font-size:22px;background:linear-gradient(135deg,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
  .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}
  .stat-card{background:#1e293b;padding:20px;border-radius:14px;border:1px solid #334155}
  .stat-card .label{font-size:12px;color:#94a3b8;margin-bottom:8px}
  .stat-card .value{font-size:28px;font-weight:700;background:linear-gradient(135deg,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
  .row-full{display:grid;grid-template-columns:1fr;gap:16px;margin-bottom:24px}
  @media(max-width:900px){.row{grid-template-columns:1fr}}
  .panel{background:#1e293b;padding:20px;border-radius:14px;border:1px solid #334155}
  .panel h3{margin-bottom:16px;font-size:15px;color:#cbd5e1;display:flex;justify-content:space-between;align-items:center}
  .tabs{display:flex;gap:6px;margin-bottom:14px}
  .tab{padding:6px 12px;background:#0f172a;border-radius:8px;font-size:12px;cursor:pointer;border:1px solid #334155}
  .tab.active{background:linear-gradient(135deg,#3b82f6,#8b5cf6);border-color:transparent}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #334155}
  th{color:#94a3b8;font-weight:500;font-size:12px}
  tr:hover td{background:#0f172a}
  .pill{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px}
  .pill-green{background:rgba(34,197,94,.2);color:#86efac}
  .err{color:#f87171;text-align:center;margin-top:10px;font-size:13px}
  .clickable{cursor:pointer;color:#60a5fa}
  .clickable:hover{text-decoration:underline}
  canvas{max-height:280px}
  
  /* 数据库管理样式 */
  .danger-zone {
    border: 1px solid #ef4444;
    background: rgba(239, 68, 68, 0.05);
  }
  .danger-zone h3 {
    color: #f87171 !important;
  }
  .btn-group {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-top: 10px;
  }
  .btn-danger {
    background: #dc2626;
    width: auto;
    padding: 10px 20px;
    font-size: 13px;
    border-radius: 8px;
  }
  .btn-danger:hover {
    background: #b91c1c;
  }
  
  /* 自定义双重确认弹窗 */
  .modal-overlay {
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.7);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 1000;
  }
  .modal {
    background: #1e293b;
    border: 1px solid #ef4444;
    border-radius: 14px;
    padding: 24px;
    max-width: 450px;
    width: 90%;
    box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5);
  }
  .modal h4 {
    color: #f87171;
    font-size: 18px;
    margin-bottom: 12px;
  }
  .modal p {
    font-size: 14px;
    color: #cbd5e1;
    line-height: 1.6;
    margin-bottom: 20px;
  }
  .modal-btns {
    display: flex;
    justify-content: flex-end;
    gap: 12px;
  }
  .modal-btn {
    padding: 8px 16px;
    border-radius: 6px;
    font-size: 13px;
    cursor: pointer;
    border: none;
    font-weight: 600;
  }
  .btn-cancel {
    background: #475569;
    color: #e2e8f0;
  }
  .btn-confirm-final {
    background: #dc2626;
    color: white;
  }
</style>
</head>
<body>

<!-- 登录界面 -->
<div class="login-box" id="loginBox">
  <h1>🎬 OVideo 后台</h1>
  <input type="password" id="pwdInput" placeholder="管理员密码" />
  <button onclick="login()">登录</button>
  <div class="err" id="loginErr"></div>
</div>

<!-- 主面板 -->
<div class="container" id="dashboard">
  <div class="header">
    <h1>📊 OVideo 用户行为监控</h1>
    <div>
      <span style="color:#94a3b8;font-size:13px;margin-right:12px" id="updateTime"></span>
      <button onclick="loadAll()" style="width:auto;padding:8px 16px;font-size:13px">🔄 刷新</button>
    </div>
  </div>

  <div class="stats" id="statsBox"></div>

  <!-- 最近30天趋势独占一行 -->
  <div class="row-full">
    <div class="panel">
      <h3>📈 最近 30 天趋势</h3>
      <canvas id="trendChart"></canvas>
    </div>
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
        <!-- 移除了分类列 -->
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
        <!-- 移除了分类列 -->
        <thead><tr><th>#</th><th>视频</th><th>用户数</th><th>次数</th></tr></thead>
        <tbody id="topDlBody"></tbody>
      </table>
    </div>
  </div>

  <div class="panel" style="margin-bottom:24px">
    <h3>👥 活跃用户榜（按观看视频数）</h3>
    <table>
      <thead><tr><th>#</th><th>User ID</th><th>看过视频数</th><th>总操作数</th><th>最后活跃</th></tr></thead>
      <tbody id="topUsersBody"></tbody>
    </table>
  </div>

  <!-- 数据库管理面板 -->
  <div class="panel danger-zone" style="margin-bottom:24px">
    <h3>⚠️ 数据库维护与管理 (危险区)</h3>
    <p style="font-size: 13px; color: #94a3b8; margin-bottom: 15px;">此区域包含破坏性操作，清空数据后不可恢复，请务必谨慎操作。</p>
    <div class="btn-group">
      <button class="btn-danger" onclick="triggerClear('analytics')">🧹 仅清空行为统计数据</button>
      <button class="btn-danger" onclick="triggerClear('users')">👤 仅清空用户及订阅数据</button>
      <button class="btn-danger" onclick="triggerClear('all')">🔥 彻底清空所有数据</button>
    </div>
  </div>
</div>

<!-- 自定义双重确认模态框 -->
<div class="modal-overlay" id="confirmModal">
  <div class="modal">
    <h4 id="modalTitle">⚠️ 危险操作确认</h4>
    <p id="modalMsg">您确定要执行此操作吗？此操作将永久删除数据且无法恢复。</p>
    <div class="modal-btns">
      <button class="modal-btn btn-cancel" onclick="closeModal()">取消</button>
      <button class="modal-btn btn-confirm-final" id="modalConfirmBtn">确认执行</button>
    </div>
  </div>
</div>

<script>
let TOKEN = localStorage.getItem('admin_token') || '';
let trendChart;
let playPeriod='today', dlPeriod='today';
let pendingClearType = '';
let currentStep = 1; // 1: 第一次确认, 2: 第二次确认

async function login(){
  const pwd = document.getElementById('pwdInput').value;
  const r = await fetch('/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pwd})});
  if(!r.ok){document.getElementById('loginErr').innerText='密码错误';return}
  const d = await r.json();
  TOKEN = d.token;
  localStorage.setItem('admin_token', TOKEN);
  showDashboard();
}

function showDashboard(){
  document.getElementById('loginBox').style.display='none';
  document.getElementById('dashboard').style.display='block';
  loadAll();
}

async function api(path, method = 'GET', body = null){
  const headers = {'X-Admin-Token':TOKEN};
  if(body) headers['Content-Type'] = 'application/json';
  const options = { method, headers };
  if(body) options.body = JSON.stringify(body);
  
  const r = await fetch(path, options);
  if(r.status===401){
    localStorage.removeItem('admin_token');
    location.reload();
    return null;
  }
  return r.json();
}

async function loadAll(){
  document.getElementById('updateTime').innerText = '更新于 '+new Date().toLocaleTimeString();
  loadOverview();
  loadTrend();
  loadTopVideos('play', playPeriod);
  loadTopVideos('download_complete', dlPeriod);
  loadTopUsers();
}

async function loadOverview(){
  const d = await api('/admin/api/overview');if(!d)return;
  const items = [
    ['总用户数', d.total_users],
    ['今日活跃', d.today_active_users],
    ['今日播放', d.today_play],
    ['今日下载', d.today_download],
    ['累计播放', d.total_play_events],
    ['累计下载', d.total_download_events],
  ];
  document.getElementById('statsBox').innerHTML = items.map(([l,v])=>
    `<div class="stat-card"><div class="label">${l}</div><div class="value">${v||0}</div></div>`).join('');
}

async function loadTrend(){
  const data = await api('/admin/api/daily_trend');if(!data)return;
  const days=[...new Set(data.map(r=>r.day))].sort();
  const playData = days.map(d=>{const r=data.find(x=>x.day===d&&x.event_type==='play');return r?r.cnt:0});
  const dlData = days.map(d=>{const r=data.find(x=>x.day===d&&x.event_type==='download_complete');return r?r.cnt:0});
  if(trendChart) trendChart.destroy();
  trendChart = new Chart(document.getElementById('trendChart'),{
    type:'line',
    data:{labels:days,datasets:[
      {label:'播放',data:playData,borderColor:'#60a5fa',backgroundColor:'rgba(96,165,250,.15)',tension:.3,fill:true},
      {label:'下载',data:dlData,borderColor:'#a78bfa',backgroundColor:'rgba(167,139,250,.15)',tension:.3,fill:true},
    ]},
    options:{responsive:true,plugins:{legend:{labels:{color:'#cbd5e1'}}},scales:{x:{ticks:{color:'#94a3b8'}},y:{ticks:{color:'#94a3b8'}}}}
  });
}

async function loadTopVideos(type, period){
  const data = await api(`/admin/api/top_videos?type=${type}&period=${period}&limit=15`);if(!data)return;
  const tbody = type==='play'?'topPlayBody':'topDlBody';
  document.getElementById(tbody).innerHTML = data.length===0
    ? '<tr><td colspan="4" style="text-align:center;color:#64748b">暂无数据</td></tr>'
    : data.map((r,i)=>`<tr>
        <td>${i+1}</td>
        <td class="clickable" onclick="showUsers('${encodeURIComponent(r.video_url)}','${type}')">${r.video_title||r.video_url}</td>
        <td><span class="pill pill-green">${r.unique_users}</span></td>
        <td>${r.total_count}</td>
      </tr>`).join('');
}

async function loadTopUsers(){
  const data = await api('/admin/api/top_users');if(!data)return;
  document.getElementById('topUsersBody').innerHTML = data.map((r,i)=>`<tr>
    <td>${i+1}</td>
    <td style="font-family:monospace;font-size:11px">${r.user_id.substring(0,30)}...</td>
    <td>${r.unique_videos}</td>
    <td>${r.total_actions}</td>
    <td style="color:#94a3b8;font-size:12px">${r.last_active.replace('T',' ').substring(0,19)}</td>
  </tr>`).join('');
}

async function showUsers(urlEnc, type){
  const url = decodeURIComponent(urlEnc);
  const data = await api(`/admin/api/video_users?video_url=${encodeURIComponent(url)}&type=${type}`);
  if(!data)return;
  
  // 使用自定义弹窗展示，避免原生 alert 阻塞
  const userListStr = data.slice(0,30).map(u=>`• ${u.user_id.substring(0,25)}... (${u.count}次, 最后:${u.last_at.substring(0,16).replace('T',' ')})`).join('<br>');
  document.getElementById('modalTitle').innerText = `👥 观看此视频的用户 (${data.length} 人)`;
  document.getElementById('modalMsg').innerHTML = userListStr || '暂无用户记录';
  const confirmBtn = document.getElementById('modalConfirmBtn');
  confirmBtn.innerText = "关闭";
  confirmBtn.onclick = closeModal;
  document.getElementById('confirmModal').style.display = 'flex';
}

function switchPlayPeriod(el,p){
  el.parentNode.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  el.classList.add('active');playPeriod=p;loadTopVideos('play',p);
}
function switchDlPeriod(el,p){
  el.parentNode.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  el.classList.add('active');dlPeriod=p;loadTopVideos('download_complete',p);
}

// --- 数据库一键清除与双重确认逻辑 ---
function triggerClear(type) {
  pendingClearType = type;
  currentStep = 1;
  
  let targetName = "";
  if(type === 'analytics') targetName = "【所有视频播放与下载统计数据】";
  if(type === 'users') targetName = "【所有注册用户账号及订阅权限数据】";
  if(type === 'all') targetName = "【全部数据（包括用户、订阅、行为统计）】";
  
  document.getElementById('modalTitle').innerText = "⚠️ 第一次安全确认";
  document.getElementById('modalMsg').innerText = `您正在尝试清空 ${targetName}。此操作是毁灭性的，数据一旦清除将永远无法恢复！\n\n您确定要继续吗？`;
  
  const confirmBtn = document.getElementById('modalConfirmBtn');
  confirmBtn.innerText = "继续下一步";
  confirmBtn.onclick = secondConfirm;
  
  document.getElementById('confirmModal').style.display = 'flex';
}

function secondConfirm() {
  currentStep = 2;
  document.getElementById('modalTitle').innerText = "🚨 终极核对确认";
  document.getElementById('modalMsg').innerText = `请再次确认！此操作将彻底抹除数据。如果您十分确定要清空，请点击下方的“彻底清空并执行”按钮。`;
  
  const confirmBtn = document.getElementById('modalConfirmBtn');
  confirmBtn.innerText = "彻底清空并执行";
  confirmBtn.onclick = executeClear;
}

async function executeClear() {
  const result = await api('/admin/api/clear_db', 'POST', { type: pendingClearType });
  closeModal();
  if (result && result.status === 'success') {
    // 成功后刷新界面
    loadAll();
    setTimeout(() => {
      document.getElementById('modalTitle').innerText = "✅ 操作成功";
      document.getElementById('modalMsg').innerText = result.message;
      document.getElementById('modalConfirmBtn').innerText = "好";
      document.getElementById('modalConfirmBtn').onclick = closeModal;
      document.getElementById('confirmModal').style.display = 'flex';
    }, 300);
  } else {
    setTimeout(() => {
      document.getElementById('modalTitle').innerText = "❌ 操作失败";
      document.getElementById('modalMsg').innerText = (result && result.error) ? result.error : "未知错误";
      document.getElementById('modalConfirmBtn').innerText = "好";
      document.getElementById('modalConfirmBtn').onclick = closeModal;
      document.getElementById('confirmModal').style.display = 'flex';
    }, 300);
  }
}

function closeModal() {
  document.getElementById('confirmModal').style.display = 'none';
}

if(TOKEN) showDashboard();
</script>
</body>
</html>
'''

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
    app.run(host=host_ip, port=port, debug=False)
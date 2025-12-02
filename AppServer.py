import os
import sqlite3
import traceback
from flask import Flask, jsonify, send_from_directory, request, g
from flask_cors import CORS
from werkzeug.utils import safe_join
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# --- 配置 ---
BASE_RESOURCES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')
ALLOWED_APPS = ['ONews', 'Finance']
# 【新增】用户数据库路径
USER_DB_PATH = os.path.join(BASE_RESOURCES_DIR, 'user_data.db')
FINANCE_DB_PATH = os.path.join(BASE_RESOURCES_DIR, 'Finance', 'Finance.db')

# 【新增】简单的邀请码配置 (实际生产中可以放在数据库里)
# 格式: "邀请码": "备注"
VALID_INVITE_CODES = {
    "ONEWS_FAMILY_2024": "Family Access",
    "VIP_FRIEND_888": "Friend Access",
    "DEV_TEST_KEY": "Developer Key"
}

# --- 数据库连接辅助函数 ---
def get_finance_db():
    db = getattr(g, '_finance_database', None)
    if db is None:
        if os.path.exists(FINANCE_DB_PATH):
            db = g._finance_database = sqlite3.connect(FINANCE_DB_PATH)
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
    
    conn = sqlite3.connect(USER_DB_PATH)
    c = conn.cursor()
    
    # 创建表（如果不存在）
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apple_user_id TEXT NOT NULL UNIQUE,
            email TEXT,
            full_name TEXT,
            created_at TIMESTAMP NOT NULL,
            last_login_at TIMESTAMP,
            subscription_expires_at TIMESTAMP,
            app_source TEXT,
            is_vip INTEGER DEFAULT 0  -- 【新增】VIP 标记 (0:否, 1:是)
        )
    ''')
    
    # 【修改】更安全的迁移逻辑：检查列是否存在
    c.execute("PRAGMA table_info(users)")
    columns = [info[1] for info in c.fetchall()]
    if 'subscription_expires_at' not in columns:
        print("正在添加 'subscription_expires_at' 列...")
        try:
            c.execute("ALTER TABLE users ADD COLUMN subscription_expires_at TIMESTAMP")
        except Exception: pass
    if 'app_source' not in columns:
        try: c.execute("ALTER TABLE users ADD COLUMN app_source TEXT")
        except Exception: pass
    # 【新增】检查并添加 is_vip 列
    if 'is_vip' not in columns:
        print("正在添加 'is_vip' 列...")
        try: c.execute("ALTER TABLE users ADD COLUMN is_vip INTEGER DEFAULT 0")
        except Exception: pass
        
    conn.commit()
    conn.close()
    print("用户数据库已准备就绪。")

# --- API 路由 ---
@app.route('/api/<app_name>/check_version', methods=['GET'])
def check_version(app_name):
    print(f"收到来自应用 '{app_name}' 的版本检查请求")
    if app_name not in ALLOWED_APPS:
        return jsonify({"error": "无效的应用名称"}), 404
    
    version_file_path = os.path.join(BASE_RESOURCES_DIR, app_name, 'version.json')
    print(f"正在尝试访问版本文件: {version_file_path}")
    
    if os.path.exists(version_file_path):
        return send_from_directory(os.path.join(BASE_RESOURCES_DIR, app_name), 'version.json')
    else:
        return jsonify({"error": "版本文件未找到"}), 404

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
        # 检查表是否存在以及是否有 volume 列
        # 注意：表名不能参数化，需要验证以防注入。这里简单假设 table_name 是合法的。
        # 生产环境应验证 table_name 是否在白名单中。
        
        # 检查是否有 volume 列
        cur = db.execute(f'PRAGMA table_info("{table_name}")')
        columns = [row['name'].lower() for row in cur.fetchall()]
        has_volume = 'volume' in columns
        
        select_clause = "id, date, price"
        if has_volume:
            select_clause += ", volume"
            
        query = f'SELECT {select_clause} FROM "{table_name}" WHERE name = ? AND date BETWEEN ? AND ? ORDER BY date ASC'
        cur = db.execute(query, (symbol, start_date, end_date))
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            item = {
                "id": row["id"],
                "date": row["date"], # string YYYY-MM-DD
                "price": row["price"]
            }
            if has_volume:
                item["volume"] = row["volume"]
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

# --- 用户认证与权限核心逻辑 ---

def check_user_subscription_status(user_row):
    """
    辅助函数：检查用户是否拥有有效权限（订阅 或 VIP）
    返回: (is_subscribed, expiration_date_string)
    """
    is_subscribed = False
    expiration_date = None
    now = datetime.utcnow()

    # 1. 【优先】检查是否是 VIP
    # 注意：SQLite 中 boolean 往往存为 1/0
    if user_row['is_vip'] == 1:
        is_subscribed = True
        # 给 VIP 一个极其遥远的过期时间
        expiration_date = "2099-12-31T23:59:59"
        return is_subscribed, expiration_date

    # 2. 检查常规订阅
    if user_row['subscription_expires_at']:
        try:
            expires_at = datetime.fromisoformat(str(user_row['subscription_expires_at']))
            if expires_at > now:
                is_subscribed = True
                expiration_date = user_row['subscription_expires_at']
        except:
            pass
            
    return is_subscribed, expiration_date

# --- 用户认证相关 (保持不变) ---
def handle_auth(app_source):
    try:
        data = request.get_json()
        identity_token = data.get('identity_token')
        user_id = data.get('user_id')
        email = data.get('email')
        full_name = data.get('full_name')

        if not user_id: return jsonify({"error": "Missing user_id"}), 400

        conn = sqlite3.connect(USER_DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,))
        user = c.fetchone()
        now = datetime.utcnow()
        
        is_subscribed = False
        expiration_date = None

        if user:
            c.execute("UPDATE users SET last_login_at = ? WHERE apple_user_id = ?", (now, user_id))
            # 【修改】使用统一的检查逻辑
            is_subscribed, expiration_date = check_user_subscription_status(user)
        else:
            c.execute(
                "INSERT INTO users (apple_user_id, email, full_name, created_at, last_login_at, app_source, is_vip) VALUES (?, ?, ?, ?, ?, ?, 0)",
                (user_id, email, full_name, now, now, app_source)
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

def handle_status_check():
    user_id = request.args.get('user_id')
    if not user_id: return jsonify({"error": "Missing user_id"}), 400
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM users WHERE apple_user_id = ?", (user_id,))
        row = c.fetchone()
        is_subscribed = False
        expires_at_str = None
        
        if row:
            # 【修改】使用统一的检查逻辑
            is_subscribed, expires_at_str = check_user_subscription_status(row)
            
        return jsonify({"is_subscribed": is_subscribed, "subscription_expires_at": expires_at_str})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# 【新增】处理邀请码兑换
def handle_redeem_invite():
    data = request.get_json()
    user_id = data.get('user_id')
    invite_code = data.get('invite_code')
    
    if not user_id or not invite_code:
        return jsonify({"error": "缺少参数"}), 400
        
    # 验证邀请码
    if invite_code not in VALID_INVITE_CODES:
        return jsonify({"error": "无效的邀请码"}), 403
        
    conn = sqlite3.connect(USER_DB_PATH)
    c = conn.cursor()
    try:
        # 将用户设为 VIP
        c.execute("UPDATE users SET is_vip = 1 WHERE apple_user_id = ?", (user_id,))
        if c.rowcount == 0:
            return jsonify({"error": "用户不存在，请先登录"}), 404
            
        conn.commit()
        print(f"用户 {user_id} 使用邀请码 {invite_code} 升级为 VIP")
        
        return jsonify({
            "status": "success",
            "is_subscribed": True,
            "subscription_expires_at": "2099-12-31T23:59:59"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

def handle_payment():
    data = request.get_json()
    user_id = data.get('user_id')
    days = data.get('days', 30)
    if not user_id: return jsonify({"error": "Missing user_id"}), 400

    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("SELECT subscription_expires_at FROM users WHERE apple_user_id = ?", (user_id,))
        row = c.fetchone()
        if not row: return jsonify({"error": "User not found"}), 404

        now = datetime.utcnow()
        new_expiry = now + timedelta(days=days)
        
        if row['subscription_expires_at']:
            try:
                current_expiry = datetime.fromisoformat(row['subscription_expires_at'])
                if current_expiry > now:
                    new_expiry = current_expiry + timedelta(days=days)
            except: pass
            
        new_expiry_str = new_expiry.isoformat()
        c.execute("UPDATE users SET subscription_expires_at = ? WHERE apple_user_id = ?", (new_expiry_str, user_id))
        conn.commit()
        return jsonify({"status": "success", "is_subscribed": True, "subscription_expires_at": new_expiry_str})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# --- ONews 路由 (保持兼容) ---
@app.route('/api/ONews/auth/apple', methods=['POST'])
def onews_auth(): return handle_auth('ONews')

@app.route('/api/ONews/payment/subscribe', methods=['POST'])
def onews_pay(): return handle_payment()

@app.route('/api/ONews/user/status', methods=['GET'])
def onews_status(): return handle_status_check()

# 【新增】兑换路由
@app.route('/api/ONews/user/redeem', methods=['POST'])
def onews_redeem(): return handle_redeem_invite()

# --- Finance 路由 (新增) ---
@app.route('/api/Finance/auth/apple', methods=['POST'])
def finance_auth(): return handle_auth('Finance')

@app.route('/api/Finance/payment/subscribe', methods=['POST'])
def finance_pay(): return handle_payment()

@app.route('/api/Finance/user/status', methods=['GET'])
def finance_status(): return handle_status_check()

# 【新增】注册 Finance 的兑换路由！！！
@app.route('/api/Finance/user/redeem', methods=['POST'])
def finance_redeem(): return handle_redeem_invite()

# --- 服务器启动 ---
if __name__ == '__main__':
    # 【新增】在启动时初始化数据库
    init_user_db()
    
    supported_apps_str = ", ".join(ALLOWED_APPS)
    print("多应用服务器正在启动...")
    print(f"支持的应用: {supported_apps_str}")
    print(f"资源目录被定位在: {BASE_RESOURCES_DIR}")
    host_ip = '0.0.0.0'
    port = 5001
    print("请确保您的手机和电脑连接到同一个Wi-Fi网络")
    print(f"在iOS App中请使用 http://{host_ip}:{port}/api/ONews/... 访问")
    app.run(host=host_ip, port=port, debug=False)
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
# 获取当前 app.py 所在的目录 (即 LocalServer)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 获取上级目录 (即 /root 或 /Users/yanzhang/Coding)
PARENT_DIR = os.path.dirname(CURRENT_DIR)

BASE_RESOURCES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')
ALLOWED_APPS = ['ONews', 'Finance']

# 【新增】用户数据库路径
USER_DB_PATH = os.path.join(PARENT_DIR, 'user_data.db')
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
            onews_is_permanent INTEGER DEFAULT 0
        )
    ''')
    
    conn.commit()
    conn.close()
    print("用户数据库（新结构）已准备就绪。")

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
def check_user_subscription_status(user_row, app_name):
    """
    检查用户权限。
    逻辑：
    1. 先检查该 App 的 is_permanent (亲友/后门)。如果是 1，直接返回 2099年。
    2. 再检查该 App 的 expire_at (付费)。如果时间还没到，返回该时间。
    3. 否则返回 False。
    """
    is_subscribed = False
    expiration_date = None
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
        
    # 2. 检查付费订阅时间
    if user_row[expire_col]:
        try:
            expires_at = datetime.fromisoformat(str(user_row[expire_col]))
            if expires_at > now:
                return True, user_row[expire_col]
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
        
        conn = sqlite3.connect(USER_DB_PATH)
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
        
    conn = sqlite3.connect(USER_DB_PATH)
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
    
    conn = sqlite3.connect(USER_DB_PATH)
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
import os
import sqlite3
import json
import traceback
from flask import Flask, jsonify, send_from_directory, request
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
            app_source TEXT -- 新增：区分用户来源 (ONews 或 Finance)
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
        try:
            c.execute("ALTER TABLE users ADD COLUMN app_source TEXT")
        except Exception: pass
        
    conn.commit()
    conn.close()
    print("用户数据库已准备就绪。")

# --- 新增：定义每个表的唯一键列 ---
# 这是一个配置字典，告诉程序如何为每个表构建查询
TABLE_UNIQUE_KEYS = {
    'Earning': ['name', 'date'],
    'Energy': ['name', 'date'],
    'Commodities': ['name', 'date'],
    'Indices': ['name', 'date'],
    'Crypto': ['name', 'date'],
    'Currencies': ['name', 'date'],
    'Bonds': ['name', 'date'],
    'Basic_Materials': ['name', 'date'],
    'Communication_Services': ['name', 'date'],
    'Consumer_Cyclical': ['name', 'date'],
    'Consumer_Defensive': ['name', 'date'],
    'Financial_Services': ['name', 'date'],
    'Utilities': ['name', 'date'],
    'Real_Estate': ['name', 'date'],
    'Industrials': ['name', 'date'],
    'Healthcare': ['name', 'date'],
    'Technology': ['name', 'date'],
    'Economics': ['name', 'date'],
    'ETFs': ['name', 'date'],
    'MNSPP': ['symbol']
    # 如果未来有新表，在这里添加即可
}

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

# --- 新增的API：获取目录文件清单 ---
@app.route('/api/<app_name>/list_files', methods=['GET'])
def list_files(app_name):
    dirname = request.args.get('dirname')
    print(f"收到来自应用 '{app_name}' 的目录清单请求: {dirname}")

    if not dirname:
        return jsonify({"error": "缺少目录名参数"}), 400

    # --- 使用 safe_join 增强安全性 ---
    try:
        target_dir = safe_join(BASE_RESOURCES_DIR, app_name, dirname)
    except Exception:
        # safe_join 在检测到可疑路径时会抛出 werkzeug.exceptions.NotFound
        return jsonify({"error": "无效的目录路径"}), 400

    if not os.path.isdir(target_dir):
        return jsonify({"error": "目录未找到"}), 404
    
    try:
        # 只返回文件名，并且过滤掉macOS的系统隐藏文件
        # 使用 utf-8 显式解码，增强在不同环境下的健壮性
        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f)) and not f.startswith('.')]
        return jsonify(files)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- 修改后的下载路由，现在可以处理子目录中的文件 ---
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
    
@app.route('/api/Finance/sync', methods=['GET'])
def sync_finance():
    # 客户端传上次同步的最大 log id
    last_id_str = request.args.get('last_id', '0')
    print(f"收到 Finance 数据库同步请求, last_id = {last_id_str}")
    
    try:
        last_id = int(last_id_str)
    except ValueError:
        return jsonify({"error": "无效的 last_id, 必须是整数"}), 400

    # 数据库路径是固定的
    db_path = os.path.join(BASE_RESOURCES_DIR, 'Finance', 'Finance.db')
    if not os.path.exists(db_path):
        return jsonify({"error": "数据库文件未在服务器上找到"}), 404

    conn = sqlite3.connect(db_path)
    # 设置 row_factory 让查询结果可以按列名访问，更清晰
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # ========================= 核心修复逻辑开始 =========================
    
    # 1. 首先，无论客户端传来什么，我们都独立查询一次数据库中真正的最新 log id。
    latest_id_row = c.execute("SELECT MAX(id) FROM sync_log").fetchone()
    # 如果 sync_log 表是空的，MAX(id) 会返回 None，所以我们需要处理这种情况，默认为 0。
    actual_latest_id = latest_id_row[0] if latest_id_row and latest_id_row[0] is not None else 0

    # 2. 然后，我们再根据客户端传来的 last_id 去获取变更。
    # 查询新的 sync_log 结构
    c.execute("""
      SELECT id, table_name, op, record_key
        FROM sync_log
       WHERE id > ?
       ORDER BY id ASC
    """, (last_id,))
    logs = c.fetchall()

    # 2) 根据 log 记录，查询具体的数据变更
    changes = []
    for log in logs:
        tbl = log['table_name']
        key_dict = json.loads(log['record_key'])

        change_record = {
            "log_id": log['id'], 
            "table": tbl, 
            "op": log['op'],
            "key": key_dict
        }

        # 对于 I 和 U 操作，需要附带整行数据
        if log['op'] in ('I', 'U'):
            # --- 核心修改：动态构建查询语句 ---
            if tbl not in TABLE_UNIQUE_KEYS:
                print(f"警告：在 TABLE_UNIQUE_KEYS 中未找到表 '{tbl}' 的配置，跳过此日志。")
                continue
            
            # 从配置中获取键列
            key_columns = TABLE_UNIQUE_KEYS[tbl]
            
            # 构建 WHERE 子句和绑定值
            where_clause = " AND ".join([f'"{col}" = ?' for col in key_columns])
            key_values = [key_dict.get(col) for col in key_columns]

            # 检查是否有 key 未在 record_key 中找到
            if None in key_values:
                print(f"警告：record_key '{key_dict}' 与表 '{tbl}' 的配置不匹配，跳过。")
                continue

            query = f'SELECT * FROM "{tbl}" WHERE {where_clause}'
            c.execute(query, key_values)
            row_data = c.fetchone()
            
            if row_data:
                # 将 row_data 转换为字典
                change_record["data"] = dict(row_data)
            else:
                # 如果找不到数据（可能在事务中已被删除），则跳过此日志
                continue
        
        changes.append(change_record)

    conn.close()
    
    print(f"同步完成。返回 {len(changes)} 条变更，新的 last_id 将被设置为: {actual_latest_id}")
    
    # 3. 最后，在返回的 JSON 中，我们使用刚才查到的 actual_latest_id，而不是客户端传来的 last_id。
    return jsonify({
      "last_id": actual_latest_id, # <--- 使用真正正确的最新ID
      "changes": changes
    })

# 封装通用的处理逻辑
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
            if user['subscription_expires_at']:
                try:
                    expires_at = datetime.fromisoformat(str(user['subscription_expires_at']))
                    if expires_at > now:
                        is_subscribed = True
                        expiration_date = user['subscription_expires_at']
                except: pass
        else:
            c.execute(
                "INSERT INTO users (apple_user_id, email, full_name, created_at, last_login_at, app_source) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, email, full_name, now, now, app_source)
            )
        
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

def handle_status_check():
    user_id = request.args.get('user_id')
    if not user_id: return jsonify({"error": "Missing user_id"}), 400
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("SELECT subscription_expires_at FROM users WHERE apple_user_id = ?", (user_id,))
        row = c.fetchone()
        is_subscribed = False
        expires_at_str = None
        if row and row['subscription_expires_at']:
            expires_at_str = row['subscription_expires_at']
            if datetime.fromisoformat(expires_at_str) > datetime.utcnow():
                is_subscribed = True
        return jsonify({"is_subscribed": is_subscribed, "subscription_expires_at": expires_at_str})
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

# --- Finance 路由 (新增) ---
@app.route('/api/Finance/auth/apple', methods=['POST'])
def finance_auth(): return handle_auth('Finance')

@app.route('/api/Finance/payment/subscribe', methods=['POST'])
def finance_pay(): return handle_payment()

@app.route('/api/Finance/user/status', methods=['GET'])
def finance_status(): return handle_status_check()

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
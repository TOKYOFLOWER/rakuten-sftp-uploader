from flask import Flask, render_template, request, jsonify, send_file
import paramiko
import os
import json
from datetime import datetime
import sqlite3
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# アップロードフォルダを作成
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# データベース初期化
def init_db():
    conn = sqlite3.connect('schedules.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS schedules
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  filename TEXT,
                  filepath TEXT,
                  ftp_host TEXT,
                  ftp_user TEXT,
                  ftp_pass TEXT,
                  ftp_path TEXT,
                  schedule_time TEXT,
                  status TEXT)''')
    conn.commit()
    conn.close()

init_db()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        # ファイル保存
        file = request.files['csvfile']
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # スケジュール情報を保存
        schedule_time = request.form['schedule_time']
        ftp_host = request.form['ftp_host']
        ftp_user = request.form['ftp_user']
        ftp_pass = request.form['ftp_pass']
        ftp_path = request.form['ftp_path']
        
        conn = sqlite3.connect('schedules.db')
        c = conn.cursor()
        c.execute('''INSERT INTO schedules
                     (filename, filepath, ftp_host, ftp_user, ftp_pass, ftp_path, schedule_time, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (filename, filepath, ftp_host, ftp_user, ftp_pass, ftp_path, schedule_time, 'pending'))
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'{schedule_time} にアップロード予約しました'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@app.route('/schedules')
def schedules():
    conn = sqlite3.connect('schedules.db')
    c = conn.cursor()
    c.execute('SELECT * FROM schedules WHERE status = "pending" ORDER BY schedule_time')
    rows = c.fetchall()
    conn.close()
    
    schedules = []
    for row in rows:
        schedules.append({
            'id': row[0],
            'filename': row[1],
            'schedule_time': row[7],
            'status': row[8]
        })
    
    return jsonify(schedules)

@app.route('/execute_now', methods=['POST'])
def execute_now():
    try:
        conn = sqlite3.connect('schedules.db')
        c = conn.cursor()
        
        # 全てのpendingスケジュールを取得
        c.execute('''SELECT * FROM schedules 
                     WHERE status = "pending" 
                     ORDER BY schedule_time''')
        
        schedules = c.fetchall()
        
        results = []
        
        for schedule in schedules:
            schedule_id = schedule[0]
            filepath = schedule[2]
            ftp_host = schedule[3]
            ftp_user = schedule[4]
            ftp_pass = schedule[5]
            ftp_path = schedule[6]
            filename = schedule[1]
            
            try:
                # SFTP接続（楽天対応版）
                transport = paramiko.Transport((ftp_host, 22))
                
                # ホストキーの自動承認（本番環境では要注意）
                transport.get_remote_server_key()
                
                # セキュリティオプションを設定
                transport.connect(
                    username=ftp_user, 
                    password=ftp_pass,
                    hostkey=None
                )
                
                sftp = paramiko.SFTPClient.from_transport(transport)
                
                # ファイルアップロード
                remote_path = ftp_path.rstrip('/') + '/' + filename
                sftp.put(filepath, remote_path)
                
                sftp.close()
                transport.close()
                
                # ステータス更新
                c.execute('UPDATE schedules SET status = "completed" WHERE id = ?', (schedule_id,))
                conn.commit()
                
                results.append(f'✓ {filename} アップロード完了')
                
            except paramiko.AuthenticationException as e:
                error_msg = f'認証エラー: ユーザー名またはパスワードが間違っています'
                c.execute('UPDATE schedules SET status = ? WHERE id = ?', (error_msg, schedule_id))
                conn.commit()
                results.append(f'✗ {filename} {error_msg}')
                
            except paramiko.SSHException as e:
                error_msg = f'SSH接続エラー: {str(e)}'
                c.execute('UPDATE schedules SET status = ? WHERE id = ?', (error_msg, schedule_id))
                conn.commit()
                results.append(f'✗ {filename} {error_msg}')
                
            except Exception as e:
                error_msg = f'error: {str(e)}'
                c.execute('UPDATE schedules SET status = ? WHERE id = ?', (error_msg, schedule_id))
                conn.commit()
                results.append(f'✗ {filename} エラー: {str(e)}')
        
        conn.close()
        
        if not results:
            return jsonify({
                'success': True,
                'message': 'アップロード対象がありません'
            })
        
        return jsonify({
            'success': True,
            'message': '\n'.join(results)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'エラー: {str(e)}'
        })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

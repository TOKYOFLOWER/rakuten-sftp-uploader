from flask import Flask, render_template, request, jsonify
import pysftp
import os
from datetime import datetime
import sqlite3
from werkzeug.utils import secure_filename
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import pytz
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# 日本時間のタイムゾーン
JST = pytz.timezone('Asia/Tokyo')

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

# スケジュール実行関数
def check_and_execute_schedules():
    try:
        logger.info('=' * 60)
        
        conn = sqlite3.connect('schedules.db')
        c = conn.cursor()
        
        # 現在の日本時間を取得
        now_jst = datetime.now(JST).strftime('%Y-%m-%dT%H:%M')
        logger.info(f'[スケジューラー] 現在時刻（JST）: {now_jst}')
        
        # 現在時刻を過ぎているpendingスケジュールを取得
        c.execute('''SELECT * FROM schedules 
                     WHERE status = "pending" 
                     AND schedule_time <= ?
                     ORDER BY schedule_time''', (now_jst,))
        
        schedules = c.fetchall()
        
        if schedules:
            logger.info(f'[スケジューラー] 実行対象: {len(schedules)}件')
        else:
            logger.info('[スケジューラー] 実行対象なし')
        
        for schedule in schedules:
            schedule_id = schedule[0]
            filepath = schedule[2]
            ftp_host = schedule[3]
            ftp_user = schedule[4]
            ftp_pass = schedule[5]
            ftp_path = schedule[6]
            filename = schedule[1]
            
            logger.info(f'[デバッグ] ファイル: {filename}')
            logger.info(f'[デバッグ] ホスト: {ftp_host}')
            logger.info(f'[デバッグ] ユーザー: {ftp_user}')
            logger.info(f'[デバッグ] パスワード長: {len(ftp_pass)} 文字')
            logger.info(f'[デバッグ] パス: {ftp_path}')
            
            try:
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                
                with pysftp.Connection(
                    host=ftp_host.strip(),
                    username=ftp_user.strip(),
                    password=ftp_pass.strip(),
                    port=22,
                    cnopts=cnopts
                ) as sftp:
                    sftp.cwd(ftp_path.strip())
                    sftp.put(filepath, filename)
                
                c.execute('UPDATE schedules SET status = "completed" WHERE id = ?', (schedule_id,))
                conn.commit()
                logger.info(f'✓ スケジュール実行成功: {filename}')
                
            except Exception as e:
                error_msg = f'エラー: {str(e)}'
                c.execute('UPDATE schedules SET status = ? WHERE id = ?', (error_msg, schedule_id))
                conn.commit()
                logger.error(f'✗ スケジュール実行失敗: {filename} - {str(e)}')
        
        conn.close()
        logger.info('=' * 60)
        
    except Exception as e:
        logger.error(f'スケジューラーエラー: {str(e)}', exc_info=True)

# スケジューラー設定
logger.info('🚀 スケジューラーを初期化中...')
scheduler = BackgroundScheduler(timezone=JST)
scheduler.add_job(
    func=check_and_execute_schedules,
    trigger=IntervalTrigger(minutes=1),
    id='check_schedules',
    name='スケジュールチェック',
    replace_existing=True
)
scheduler.start()
logger.info('✅ スケジューラーが起動しました')

# アプリ終了時にスケジューラーを停止
atexit.register(lambda: scheduler.shutdown())

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        file = request.files['csvfile']
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        schedule_time = request.form['schedule_time']
        ftp_host = request.form['ftp_host']
        ftp_user = request.form['ftp_user']
        ftp_pass = request.form['ftp_pass']
        ftp_path = request.form['ftp_path']
        
        conn = sqlite3.connect('schedules.db')
        c = conn.cursor()
        
        # 古いpending状態のレコードを削除
        c.execute('DELETE FROM schedules WHERE status = "pending"')
        
        c.execute('''INSERT INTO schedules
                     (filename, filepath, ftp_host, ftp_user, ftp_pass, ftp_path, schedule_time, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (filename, filepath, ftp_host, ftp_user, ftp_pass, ftp_path, schedule_time, 'pending'))
        conn.commit()
        conn.close()
        
        # 現在の日本時間も表示
        now_jst = datetime.now(JST).strftime('%Y-%m-%d %H:%M')
        
        logger.info(f'新規スケジュール登録: {filename} at {schedule_time}')
        
        return jsonify({
            'success': True,
            'message': f'{schedule_time} にアップロード予約しました\n現在時刻（日本時間）: {now_jst}\n1分ごとに自動チェックされます'
        })
        
    except Exception as e:
        logger.error(f'アップロードエラー: {str(e)}')
        return jsonify({
            'success': False,
            'message': str(e)
        })

@app.route('/schedules')
def schedules():
    conn = sqlite3.connect('schedules.db')
    c = conn.cursor()
    c.execute('SELECT * FROM schedules ORDER BY id DESC LIMIT 20')
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
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                
                with pysftp.Connection(
                    host=ftp_host.strip(),
                    username=ftp_user.strip(),
                    password=ftp_pass.strip(),
                    port=22,
                    cnopts=cnopts
                ) as sftp:
                    sftp.cwd(ftp_path.strip())
                    sftp.put(filepath, filename)
                
                results.append(f'✓ {filename} アップロード完了')
                c.execute('UPDATE schedules SET status = "completed" WHERE id = ?', (schedule_id,))
                conn.commit()
                
            except Exception as e:
                error_msg = f'エラー: {str(e)}'
                c.execute('UPDATE schedules SET status = ? WHERE id = ?', (error_msg, schedule_id))
                conn.commit()
                results.append(f'✗ {filename} {error_msg}')
        
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
    logger.info(f'🌐 アプリケーションをポート {port} で起動')
    app.run(host='0.0.0.0', port=port)

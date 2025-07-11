# ==============================================================================
# Memory Library - Process Staged Articles Service
# Role:         Receives tasks from Pub/Sub, analyzes articles, and updates them.
# Version:      1.0 (Flask Architecture)
# Author:       心理 (Thinking Partner)
# Last Updated: 2025-07-11
# ==============================================================================
import os
import base64
import json
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import firestore
from datetime import datetime, timezone
import logging

# Pythonの標準ロギングを設定
logging.basicConfig(level=logging.INFO)

# Flaskアプリケーションを初期化
app = Flask(__name__)

# Firebaseの初期化
try:
    firebase_admin.initialize_app()
    db = firestore.client()
    app.logger.info("Firebase app initialized successfully.")
except Exception as e:
    app.logger.error(f"Error initializing Firebase app: {e}")
    db = None

@app.route('/', methods=['POST'])
def process_pubsub_message():
    """
    Pub/Subからのプッシュ通知を受け取り、記事の処理を行うエンドポイント。
    """
    if not db:
        app.logger.error("Firestore client not initialized. Cannot process message.")
        # Pub/Subにエラーを返し、再配信を促す
        return "Internal Server Error", 500

    # Pub/Subからのリクエストボディを取得
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        app.logger.error(f"Bad Pub/Sub request: {envelope}")
        return "Bad Request: invalid Pub/Sub message format", 400

    # メッセージデータをデコード
    try:
        message = envelope['message']
        # メッセージデータはBase64でエンコードされている
        doc_id = base64.b64decode(message['data']).decode('utf-8').strip()
        app.logger.info(f"Received task to process document: {doc_id}")
    except Exception as e:
        app.logger.error(f"Failed to decode Pub/Sub message: {e}")
        return "Bad Request: could not decode message data", 400

    # Firestoreからドキュメントを取得
    try:
        doc_ref = db.collection('staging_articles').document(doc_id)
        doc = doc_ref.get()

        if not doc.exists:
            app.logger.error(f"Document {doc_id} not found in staging_articles.")
            # 見つからない場合は再試行しても無駄なので、成功を返してメッセージをACKする
            return "Success", 204

        # --- ここからAIによる分析・分類処理 ---
        # 将来的に、ここでGemini APIなどを呼び出し、カテゴリやタグを生成する
        # For now, we simulate the process by updating the status.
        
        # (仮の処理)
        raw_text = doc.to_dict().get('content', {}).get('rawText', '')
        simulated_categories = ["分類テスト"]
        simulated_tags = ["AI処理済", f"文字数_{len(raw_text)}"]
        
        update_data = {
            'status': 'processed',
            'aiGenerated': {
                'categories': simulated_categories,
                'tags': simulated_tags
            },
            'updatedAt': datetime.now(timezone.utc)
        }
        
        doc_ref.update(update_data)
        
        app.logger.info(f"Successfully processed and updated document {doc_id}.")
        
        # --- AI処理ここまで ---

        # Pub/Subにメッセージの処理成功を伝える (ACK)
        return "Success", 204

    except Exception as e:
        app.logger.error(f"Failed to process document {doc_id}: {e}")
        # 不明なエラーが発生した場合、再試行を期待してエラーを返す
        return "Internal Server Error", 500

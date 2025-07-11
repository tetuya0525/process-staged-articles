# ==============================================================================
# Memory Library - Process Staged Articles Service
# Role:         Receives tasks, analyzes articles, and passes them to the next service.
# Version:      1.2 (Lazy Initialization / Stable)
# Author:       心理 (Thinking Partner)
# Last Updated: 2025-07-11
# ==============================================================================
import os
import base64
from flask import Flask, request
import firebase_admin
from firebase_admin import firestore
from google.cloud import pubsub_v1
import logging

# Pythonの標準ロギングを設定
logging.basicConfig(level=logging.INFO)

# Flaskアプリケーションを初期化
app = Flask(__name__)

# グローバル変数としてクライアントを保持 (遅延初期化のためNoneで開始)
db = None
publisher = None

def get_firestore_client():
    """Firestoreクライアントをシングルトンとして取得・初期化する"""
    global db
    if db is None:
        try:
            firebase_admin.initialize_app()
            db = firestore.client()
            app.logger.info("Firebase app initialized successfully.")
        except Exception as e:
            app.logger.error(f"Error initializing Firebase app: {e}")
    return db

def get_pubsub_publisher():
    """Pub/Sub Publisherクライアントをシングルトンとして取得・初期化する"""
    global publisher
    if publisher is None:
        try:
            publisher = pubsub_v1.PublisherClient()
            app.logger.info("Pub/Sub publisher initialized successfully.")
        except Exception as e:
            app.logger.error(f"Error initializing Pub/Sub client: {e}")
    return publisher

@app.route('/', methods=['POST'])
def process_pubsub_message():
    # 必要なクライアントを取得 (ここで初めて初期化が試みられる)
    db_client = get_firestore_client()
    pubsub_publisher = get_pubsub_publisher()

    # 環境変数を取得
    PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    INTEGRATE_TOPIC_ID = os.environ.get('INTEGRATE_ARTICLE_TOPIC_ID')

    # 依存関係のチェック
    if not all([db_client, pubsub_publisher, PROJECT_ID, INTEGRATE_TOPIC_ID]):
        app.logger.error("A critical component or environment variable is missing.")
        return "Internal Server Error", 500

    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        app.logger.error(f"Bad Pub/Sub request: {envelope}")
        return "Bad Request: invalid Pub/Sub message format", 400

    try:
        doc_id = base64.b64decode(envelope['message']['data']).decode('utf-8').strip()
        app.logger.info(f"Received task to process document: {doc_id}")
    except Exception as e:
        app.logger.error(f"Failed to decode Pub/Sub message: {e}")
        return "Bad Request: could not decode message data", 400

    try:
        doc_ref = db_client.collection('staging_articles').document(doc_id)
        doc = doc_ref.get()

        if not doc.exists:
            app.logger.warning(f"Document {doc_id} not found. Acknowledging message.")
            return "Success", 204

        # (仮のAI処理)
        raw_text = doc.to_dict().get('content', {}).get('rawText', '')
        update_data = {
            'status': 'processed',
            'aiGenerated': {
                'categories': ["分類テスト"],
                'tags': ["AI処理済", f"文字数_{len(raw_text)}"]
            },
            'updatedAt': firestore.SERVER_TIMESTAMP
        }
        doc_ref.update(update_data)
        app.logger.info(f"Successfully processed and updated document {doc_id}.")

        # 次のパイプラインを呼び出す
        topic_path = pubsub_publisher.topic_path(PROJECT_ID, INTEGRATE_TOPIC_ID)
        future = pubsub_publisher.publish(topic_path, doc_id.encode('utf-8'))
        future.result()
        app.logger.info(f"Published document {doc_id} to topic {INTEGRATE_TOPIC_ID} for integration.")

        return "Success", 204

    except Exception as e:
        app.logger.error(f"Failed to process document {doc_id}: {e}")
        return "Internal Server Error", 500

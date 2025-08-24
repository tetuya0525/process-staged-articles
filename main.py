# ==============================================================================
# Memory Library - Process Staged Articles Service
# main.py (v2.2 Critical Fix)
#
# Role:         Pub/Subメッセージをトリガーにステージング記事をAIで分析・処理し、
#               次のワークフローへ連携する。重大な問題を修正した安定版。
# Version:      2.2
# Last Updated: 2025-08-24
# ==============================================================================
import os
import json
import logging
import base64
from typing import Dict, Any, Tuple, Optional, Protocol
from dataclasses import dataclass
from enum import Enum

from flask import Flask, request, jsonify, current_app
import firebase_admin
from firebase_admin import firestore
from google.cloud import pubsub_v1
from google.api_core import exceptions as gcp_exceptions


# --- 定数定義 ---
class DocumentStatus(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    PROCESSED = "processed"


# --- 設定クラス ---
@dataclass
class Config:
    GCP_PROJECT_ID: str
    INTEGRATE_ARTICLE_TOPIC_ID: str
    STAGING_COLLECTION: str = "staging_articles"
    LOG_LEVEL: str = "INFO"

    @classmethod
    def from_env(cls):
        # ... (v2.1と同じ)
        return cls(
            GCP_PROJECT_ID=os.environ.get("GCP_PROJECT_ID"),
            INTEGRATE_ARTICLE_TOPIC_ID=os.environ.get("INTEGRATE_ARTICLE_TOPIC_ID"),
            LOG_LEVEL=os.environ.get("LOG_LEVEL", "INFO"),
        )

    def validate(self):
        # ... (v2.1と同じ)
        if not self.GCP_PROJECT_ID or not self.INTEGRATE_ARTICLE_TOPIC_ID:
            raise ValueError(
                "GCP_PROJECT_ID and INTEGRATE_ARTICLE_TOPIC_ID are required."
            )


# --- エラーとバリデーション ---
class ValidationError(Exception):
    pass


class Validator:
    # ... (v2.1と同じ)
    @staticmethod
    def validate_message(data: Dict[str, Any]) -> Tuple[str, str]:
        doc_id = data.get("documentId")
        batch_id = data.get("batchId", "unknown")
        if not doc_id:
            raise ValidationError("メッセージにdocumentIdが含まれていません")
        return doc_id, batch_id

    @staticmethod
    def validate_document(doc_data: Optional[Dict[str, Any]], doc_id: str) -> None:
        if not doc_data:
            raise ValidationError(f"ドキュメントが存在しません: {doc_id}")
        if doc_data.get("status") != DocumentStatus.QUEUED.value:
            raise ValidationError(
                f"ドキュメントは処理対象外です (status: {doc_data.get('status')})"
            )
        if not doc_data.get("content", {}).get("rawText"):
            raise ValidationError(
                f"ドキュメントに処理対象のテキストがありません: {doc_id}"
            )


# --- 依存関係の抽象化 ---
class FirestoreClientProtocol(Protocol):
    def collection(self, name: str) -> Any: ...
    def transaction(self) -> Any: ...


class PubSubPublisherProtocol(Protocol):
    def topic_path(self, project: str, topic: str) -> str: ...
    def publish(self, topic: str, data: bytes) -> Any: ...


# --- ビジネスロジック層 ---
class ArticleProcessor:
    # ... (v2.1と同じロジック)
    def __init__(
        self,
        db: FirestoreClientProtocol,
        publisher: PubSubPublisherProtocol,
        config: Config,
    ):
        self.db = db
        self.publisher = publisher
        self.config = config

    def _simulate_ai_processing(self, raw_text: str) -> Dict[str, Any]:
        text_length = len(raw_text)
        categories = ["技術文書", "AI分析"] if "AI" in raw_text else ["一般文書"]
        tags = [f"文字数_{text_length}"]
        return {
            "summary": f"本文の文字数は{text_length}文字です。",
            "categories": categories,
            "tags": tags,
        }

    def process(self, doc_id: str, batch_id: str) -> bool:
        doc_ref = self.db.collection(self.config.STAGING_COLLECTION).document(doc_id)
        transaction = self.db.transaction()

        @firestore.transactional
        def _process_in_transaction(trans):
            snapshot = doc_ref.get(transaction=trans)
            doc_data = snapshot.to_dict()
            Validator.validate_document(doc_data, doc_id)
            trans.update(doc_ref, {"status": DocumentStatus.PROCESSING.value})
            ai_results = self._simulate_ai_processing(doc_data["content"]["rawText"])
            final_update = {
                "status": DocumentStatus.PROCESSED.value,
                "aiGenerated": ai_results,
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "batchId": batch_id,
            }
            trans.update(doc_ref, final_update)
            return True

        result = _process_in_transaction(transaction)
        if result:
            topic_path = self.publisher.topic_path(
                self.config.GCP_PROJECT_ID, self.config.INTEGRATE_ARTICLE_TOPIC_ID
            )
            next_message = json.dumps(
                {"documentId": doc_id, "batchId": batch_id}
            ).encode("utf-8")
            future = self.publisher.publish(topic_path, next_message)
            future.result(timeout=30)
            log_structured(
                "INFO", "次のワークフローへメッセージを送信", document_id=doc_id
            )
        return result is not None


# --- アプリケーション層 (Flask) ---
# ★ [ChatGPT] log_structured関数の実装漏れを修正
def log_structured(level: str, message: str, **kwargs: Any) -> None:
    log_data = {"message": message, "severity": level, **kwargs}
    log_level_map = {
        "INFO": logging.info,
        "WARNING": logging.warning,
        "ERROR": logging.error,
        "CRITICAL": logging.critical,
    }
    logger_func = log_level_map.get(level.upper(), logging.info)
    exc_info = kwargs.pop("exc_info", None)
    logger_func(json.dumps(log_data, ensure_ascii=False), exc_info=exc_info)


def create_app() -> Flask:
    """
    ★ [Gemini] アプリケーションファクトリパターンを導入 。
    これにより、非推奨の`@before_first_request`を廃止し、
    グローバル変数への依存をなくすことでテスト容易性を向上させる 。
    """
    app = Flask(__name__)

    try:
        # 1. 設定の読み込みと検証
        config = Config.from_env()
        config.validate()
        app.config["APP_CONFIG"] = config

        # 2. ログ設定
        logging.basicConfig(level=config.LOG_LEVEL, format="%(message)s")

        # 3. Firebase初期化
        if not firebase_admin._apps:
            firebase_admin.initialize_app()

        # 4. DIコンテナの代わりに、アプリケーションコンテキストにサービスを登録
        app.db_client = firestore.client()
        app.pubsub_publisher = pubsub_v1.PublisherClient()
        app.processor = ArticleProcessor(app.db_client, app.pubsub_publisher, config)

        log_structured("INFO", "アプリケーションの初期化が完了しました")

    except Exception as e:
        log_structured("CRITICAL", "アプリケーション初期化に失敗", error=str(e))
        raise

    # 5. ルートの登録
    @app.route("/", methods=["POST"])
    def process_pubsub_message():
        envelope = request.get_json()
        if not envelope or "message" not in envelope:
            return "Bad Request", 400

        try:
            pubsub_message = base64.b64decode(envelope["message"]["data"]).decode(
                "utf-8"
            )
            message_data = json.loads(pubsub_message)
            doc_id, batch_id = Validator.validate_message(message_data)

            # アプリケーションコンテキストからプロセッサを取得
            processor = current_app.processor
            success = processor.process(doc_id, batch_id)

            return ("Success", 204) if success else ("Skipped", 200)

        except ValidationError as e:
            log_structured("WARNING", "バリデーションエラー", error=str(e))
            return "Bad Request", 400
        except Exception as e:
            log_structured("ERROR", "予期せぬエラー", error=str(e), exc_info=True)
            return "Internal Server Error", 500

    @app.route("/health", methods=["GET"])
    def health_check():
        try:
            # アプリケーションコンテキストからクライアントを取得して疎通確認
            list(current_app.db_client.collections(page_size=1))
            current_app.pubsub_publisher.topic_path(
                current_app.config["APP_CONFIG"].GCP_PROJECT_ID, "health-check"
            )
            return jsonify({"status": "healthy"}), 200
        except Exception as e:
            return jsonify({"status": "unhealthy", "error": str(e)}), 503

    return app


# --- 起動 ---
if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

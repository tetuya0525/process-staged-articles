# ==============================================================================
# Dockerfile for Process Staged Articles Service (v2.2)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# 依存関係を先にインストール（root権限）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 非rootユーザーを作成し、切り替え
RUN adduser --system --group appuser
USER appuser

# ソースコードをコピー
COPY . .

# Cloud RunのPORT環境変数を設定
ENV PORT 8080

# ★ エントリーポイントをアプリケーションファクトリ`create_app()`を呼び出すように修正
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "main:create_app()"]

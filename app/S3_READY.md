# S3設定済みメモ

このプロジェクトには `.env` が追加されており、画像アップロードは Amazon S3 へ保存されます。

## 起動前

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --reload
```

## 現在の設定

- AWS Region: ap-northeast-1
- S3 Bucket: reserve-site-images-001
- S3 Prefix: shops

## 注意

- `.env` にはAWSアクセスキーが入っています。公開リポジトリへ上げないでください。
- 万一外部へ共有した場合は、AWSでアクセスキーをローテーションしてください。

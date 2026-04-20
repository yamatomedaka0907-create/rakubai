# S3 image upload setup

1. Install dependencies
   `pip install -r requirements.txt`
2. Set environment variables from `.env.s3.example`
3. Create an S3 bucket and grant PutObject/ListBucket/GetObject for the app user
4. Make uploaded objects publicly readable, either by bucket policy or `S3_ACL=public-read`
5. Restart the app

When `S3_BUCKET` is set, image uploads switch from local `/uploads/...` storage to S3 automatically.

## Notes
- Existing local images remain valid.
- New uploads return a full S3 URL and are saved into the DB as-is.
- If you use CloudFront, set `S3_PUBLIC_BASE_URL` to the CDN URL.

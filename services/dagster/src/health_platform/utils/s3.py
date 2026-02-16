def upload_text_object(s3_client, bucket_name: str, key: str, payload):
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=payload.read(), ContentType="text/plain")

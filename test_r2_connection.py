#!/usr/bin/env python3
"""
R2 boto3 연결 테스트
"""

import boto3
from botocore.exceptions import ClientError
import os

# 환경변수 시뮬레이션 (새로운 S3 호환 키)
os.environ['R2_ACCOUNT_ID'] = '56c0dee4c3224e8e318e42a60f4755d7'
os.environ['R2_ACCESS_KEY_ID'] = 'f95492415bd3edd81868d7d34c98adbb'
os.environ['R2_SECRET_ACCESS_KEY'] = '2b11a97290cd91cda0855df89139ac0d43c3da55010df4d33467ae051854585f'
os.environ['R2_PRIVATE_BUCKET_NAME'] = 'rra-datas'

account_id = os.environ['R2_ACCOUNT_ID']
access_key_id = os.environ['R2_ACCESS_KEY_ID']
secret_access_key = os.environ['R2_SECRET_ACCESS_KEY']
bucket_name = os.environ['R2_PRIVATE_BUCKET_NAME']

endpoint_url = f'https://{account_id}.r2.cloudflarestorage.com'

print(f"테스트 설정:")
print(f"  Account ID: {account_id}")
print(f"  Access Key: {access_key_id}")
print(f"  Secret Key: {secret_access_key[:10]}...")
print(f"  Bucket: {bucket_name}")
print(f"  Endpoint: {endpoint_url}")
print()

try:
    # boto3 클라이언트 생성
    s3_client = boto3.client(
        service_name='s3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='auto'
    )

    print("✅ boto3 클라이언트 생성 성공")

    # 버킷 리스트 테스트
    try:
        response = s3_client.list_buckets()
        print("✅ 버킷 리스트 조회 성공:")
        for bucket in response.get('Buckets', []):
            print(f"  - {bucket['Name']}")
    except ClientError as e:
        print(f"❌ 버킷 리스트 조회 실패: {e}")

    # 특정 버킷 파일 리스트 테스트
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        print(f"✅ 버킷 '{bucket_name}' 파일 리스트 조회 성공:")
        for obj in response.get('Contents', []):
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    except ClientError as e:
        print(f"❌ 파일 리스트 조회 실패: {e}")

    # 파일 존재 확인 테스트
    file_key = "11_rra_cert_flattened.parquet"
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
        print(f"✅ 파일 '{file_key}' 존재 확인 성공")
    except ClientError as e:
        print(f"❌ 파일 존재 확인 실패: {e}")

    # Presigned URL 생성 테스트
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': file_key},
            ExpiresIn=3600
        )
        print(f"✅ Presigned URL 생성 성공:")
        print(f"  {presigned_url[:100]}...")
    except ClientError as e:
        print(f"❌ Presigned URL 생성 실패: {e}")

except Exception as e:
    print(f"❌ boto3 클라이언트 생성 실패: {e}")
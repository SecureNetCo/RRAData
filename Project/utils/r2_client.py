#!/usr/bin/env python3
"""
Cloudflare R2 Storage 클라이언트
AWS S3 호환 API를 사용하여 R2와 연결
"""

import os
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

class R2Client:
    """Cloudflare R2 Storage 클라이언트"""

    def __init__(self):
        """
        환경변수에서 R2 인증 정보를 읽어 클라이언트 초기화

        필요한 환경변수:
        - R2_ACCOUNT_ID: Cloudflare 계정 ID
        - R2_ACCESS_KEY_ID: R2 API 토큰의 Access Key ID
        - R2_SECRET_ACCESS_KEY: R2 API 토큰의 Secret Access Key
        - R2_BUCKET_NAME: 사용할 R2 버킷 이름
        """
        self.account_id = os.getenv('R2_ACCOUNT_ID')
        self.access_key_id = os.getenv('R2_ACCESS_KEY_ID')
        self.secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
        self.bucket_name = os.getenv('R2_BUCKET_NAME', 'datapage-parquet')

        if not all([self.account_id, self.access_key_id, self.secret_access_key]):
            raise ValueError("R2 환경변수가 설정되지 않았습니다: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")

        # R2 엔드포인트 URL 구성
        self.endpoint_url = f'https://{self.account_id}.r2.cloudflarestorage.com'

        # boto3 S3 클라이언트 생성 (R2 호환)
        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name='auto'  # R2는 'auto' 리전 사용
        )

        logger.info(f"R2 클라이언트 초기화 완료: {self.endpoint_url}")

    def upload_file(self, local_file_path: str, r2_key: str) -> str:
        """
        로컬 파일을 R2에 업로드

        Args:
            local_file_path (str): 업로드할 로컬 파일 경로
            r2_key (str): R2에 저장될 파일 키 (파일명)

        Returns:
            str: 업로드된 파일의 R2 URL
        """
        try:
            logger.info(f"R2 업로드 시작: {local_file_path} -> {r2_key}")

            # 파일 업로드
            self.s3_client.upload_file(
                local_file_path,
                self.bucket_name,
                r2_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )

            # 업로드된 파일의 공개 URL 생성
            file_url = f"{self.endpoint_url}/{self.bucket_name}/{r2_key}"
            logger.info(f"R2 업로드 완료: {file_url}")

            return file_url

        except ClientError as e:
            logger.error(f"R2 업로드 실패: {e}")
            raise

    def download_file(self, r2_key: str, local_file_path: str):
        """
        R2에서 파일을 다운로드

        Args:
            r2_key (str): R2의 파일 키
            local_file_path (str): 저장할 로컬 파일 경로
        """
        try:
            logger.info(f"R2 다운로드 시작: {r2_key} -> {local_file_path}")

            self.s3_client.download_file(
                self.bucket_name,
                r2_key,
                local_file_path
            )

            logger.info(f"R2 다운로드 완료: {local_file_path}")

        except ClientError as e:
            logger.error(f"R2 다운로드 실패: {e}")
            raise

    def list_files(self, prefix: str = '') -> list:
        """
        R2 버킷의 파일 목록 조회

        Args:
            prefix (str): 파일 키 접두사 (폴더 경로)

        Returns:
            list: 파일 키 목록
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]

            logger.info(f"R2 파일 목록 조회 완료: {len(files)}개 파일")
            return files

        except ClientError as e:
            logger.error(f"R2 파일 목록 조회 실패: {e}")
            raise

    def delete_file(self, r2_key: str):
        """
        R2에서 파일 삭제

        Args:
            r2_key (str): 삭제할 파일 키
        """
        try:
            logger.info(f"R2 파일 삭제 시작: {r2_key}")

            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=r2_key
            )

            logger.info(f"R2 파일 삭제 완료: {r2_key}")

        except ClientError as e:
            logger.error(f"R2 파일 삭제 실패: {e}")
            raise

    def get_file_url(self, r2_key: str) -> str:
        """
        R2 파일의 공개 URL 생성

        Args:
            r2_key (str): 파일 키

        Returns:
            str: 파일의 공개 URL
        """
        return f"{self.endpoint_url}/{self.bucket_name}/{r2_key}"

    def file_exists(self, r2_key: str) -> bool:
        """
        R2에 파일이 존재하는지 확인

        Args:
            r2_key (str): 확인할 파일 키

        Returns:
            bool: 파일 존재 여부
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=r2_key
            )
            return True
        except ClientError:
            return False

    def generate_presigned_url(self, r2_key: str, expires_in: int = 3600) -> str:
        """
        Presigned URL 생성 (Private 버킷용)

        Args:
            r2_key (str): 파일 키 (예: "datapage-parquet/11_rra_cert_flattened.parquet")
            expires_in (int): 만료시간 (초, 기본 1시간)

        Returns:
            str: Presigned URL
        """
        try:
            logger.info(f"Presigned URL 생성 시작: {r2_key}")

            presigned_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': r2_key
                },
                ExpiresIn=expires_in
            )

            logger.info(f"Presigned URL 생성 완료: {r2_key}")
            return presigned_url

        except ClientError as e:
            logger.error(f"Presigned URL 생성 실패: {r2_key}, {e}")
            raise

    def get_private_bucket_name(self) -> str:
        """Private 버킷명 반환"""
        private_bucket = os.getenv('R2_PRIVATE_BUCKET_NAME')
        if private_bucket:
            return private_bucket
        return self.bucket_name
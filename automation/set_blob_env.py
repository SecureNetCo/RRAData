#!/usr/bin/env python3
"""
이미 업로드된 Blob 파일들의 URL을 환경변수로 설정하는 스크립트
"""

import subprocess
import sys

def run_command(cmd):
    """명령어 실행"""
    print(f"[실행중] {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[에러] {result.stderr}")
        return False
    return True

def set_blob_env_vars():
    """Blob URL들을 환경변수로 설정"""
    
    # 이미 업로드된 파일들의 URL (vercel blob ls 결과)
    blob_urls = {
        "BLOB_URL_1_SAFETYKOREA": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/1_safetykorea_flattened.parquet",
        "BLOB_URL_2_WADIZ": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/2_wadiz_flattened.parquet",
        "BLOB_URL_3_EFFICIENCY": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/3_efficiency_flattened.parquet",
        "BLOB_URL_4_HIGH_EFFICIENCY": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/4_high_efficiency_flattened.parquet",
        "BLOB_URL_5_STANDBY_POWER": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/5_standby_power_flattened.parquet",
        "BLOB_URL_6_APPROVAL": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/6_approval_flattened.parquet",
        "BLOB_URL_7_DECLARE": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/7_declare_flattened.parquet",
        "BLOB_URL_8_KC_CERTIFICATION": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/8_kwtc_flattened.parquet",
        "BLOB_URL_9_RECALL": "https://k1kufh81n6yzt4ix.public.blob.vercel-storage.com/9_recall_flattened.parquet"
    }
    
    print("=" * 60)
    print("Blob URL 환경변수 설정 시작")
    print("=" * 60)
    
    success_count = 0
    
    for env_var, url in blob_urls.items():
        print(f"\n[설정중] {env_var}")
        
        # 기존 환경변수 삭제 (있다면)
        try:
            subprocess.run([
                "vercel", "env", "rm", env_var, "production", "--yes"
            ], capture_output=True, check=False)
        except:
            pass
        
        # 새 환경변수 추가
        if run_command([
            "vercel", "env", "add", env_var, url, "production"
        ]):
            print(f"[성공] {env_var}")
            success_count += 1
        else:
            print(f"[실패] {env_var}")
    
    print("=" * 60)
    print(f"[완료] {success_count}/{len(blob_urls)}개 환경변수 설정")
    
    if success_count == len(blob_urls):
        print("[성공] 모든 환경변수가 설정되었습니다!")
        print("[알림] 이제 웹사이트에서 Blob 데이터를 사용할 수 있습니다.")
    else:
        print("[경고] 일부 환경변수 설정에 실패했습니다.")

if __name__ == "__main__":
    set_blob_env_vars()
#!/bin/bash
# 데이터 업데이트 간편 실행 스크립트 (Mac/Linux용)

echo "DataPage 데이터 자동 업데이트 시작..."
echo "현재 위치: $(pwd)"
echo "="

# automation 폴더로 이동 후 Python 스크립트 실행
cd "$(dirname "$0")"

if command -v python3 &> /dev/null; then
    python3 auto_blob_update.py
elif command -v python &> /dev/null; then
    python auto_blob_update.py
else
    echo "[오류] Python이 설치되지 않았습니다."
    exit 1
fi

echo ""
echo "데이터 업데이트 완료!"
echo "새로운 데이터가 곧 서비스에 반영됩니다."
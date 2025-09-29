# DataPage 데이터 자동 업로드 가이드

## 📋 개요

새로운 parquet 파일들을 Vercel Blob에 업로드하고 환경변수를 자동으로 설정하는 스크립트입니다.

## 🖥️ 지원 플랫폼

- **Mac/Linux**: `update_data.sh` 사용
- **Windows**: `update_data.bat` 사용

## 📦 사전 준비

1. **Vercel CLI 설치**
   ```bash
   npm install -g vercel
   ```

2. **Vercel 로그인**
   ```bash
   vercel login
   ```

3. **파일 위치 확인**
   - parquet 파일들이 `Project/data/last/` 디렉토리에 있어야 함
   - 파일명: `1_safetykorea_flattened.parquet` ~ `9_recall_flattened.parquet`

## 🚀 사용법

### Mac/Linux
```bash
# automation 폴더로 이동
cd automation

# 실행 권한 부여 (최초 1회만)
chmod +x update_data.sh

# 스크립트 실행
./update_data.sh
```

### Windows
```batch
# automation 폴더로 이동 후 더블클릭 또는 명령프롬프트에서
cd automation
update_data.bat
```

### 직접 Python 실행
```bash
cd automation
python3 auto_blob_update.py  # Mac/Linux
python auto_blob_update.py   # Windows
```

## 📝 처리되는 파일 목록

| 파일명 | 환경변수 |
|--------|----------|
| `1_safetykorea_flattened.parquet` | `BLOB_URL_1_SAFETYKOREA` |
| `2_wadiz_flattened.parquet` | `BLOB_URL_2_WADIZ` |
| `3_efficiency_flattened.parquet` | `BLOB_URL_3_EFFICIENCY` |
| `4_high_efficiency_flattened.parquet` | `BLOB_URL_4_HIGH_EFFICIENCY` |
| `5_standby_power_flattened.parquet` | `BLOB_URL_5_STANDBY_POWER` |
| `6_approval_flattened.parquet` | `BLOB_URL_6_APPROVAL` |
| `7_declare_flattened.parquet` | `BLOB_URL_7_DECLARE` |
| `8_kwtc_flattened.parquet` | `BLOB_URL_8_KC_CERTIFICATION` |
| `9_recall_flattened.parquet` | `BLOB_URL_9_RECALL` |

## 🔧 문제 해결

### "Vercel CLI에 로그인되지 않았습니다"
```bash
vercel login
```

### "파일 없음" 오류
- `Project/data/last/` 디렉토리에 해당 parquet 파일이 있는지 확인
- 파일명이 정확한지 확인

### Windows에서 "python 명령을 찾을 수 없습니다"
- Python 설치 및 PATH 설정 확인
- 명령프롬프트에서 `python --version` 테스트

## ✅ 성공 확인

스크립트 완료 후:
1. Vercel 대시보드에서 환경변수 9개 확인
2. 웹사이트에서 검색 테스트
3. 새로운 데이터로 결과가 나오는지 확인

## 📅 정기 업데이트

월 1회 데이터 갱신 시:
1. 새 parquet 파일들을 `Project/data/last/`에 저장
2. 해당 플랫폼의 스크립트 실행
3. 완료!
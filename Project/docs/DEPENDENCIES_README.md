# Dependencies Management

## 통합된 의존성 구조

### Project/requirements.txt (활성)
- **현재 사용 중인 프로덕션 의존성**
- 실제 애플리케이션에서 사용되는 패키지만 포함
- Pydantic v2와 호환됨

### requirements_legacy.txt (레거시)
- 이전에 사용하던 루트 레벨 의존성 파일
- 참고용으로 보존됨
- 일부 중복되거나 사용되지 않는 패키지 포함

## 주요 변경사항

### 제거된 패키지
- `aiohttp==3.10.11` - 실제 사용되지 않음
- `xlsxwriter==3.1.9` - openpyxl과 중복
- `requests==2.31.0` - 실제 사용되지 않음  
- `python-dateutil==2.8.2` - 직접 사용되지 않음
- `fastparquet==2023.10.1` - 실제 사용되지 않음

### 버전 업데이트
- `pandas`: 2.1.4 → >=2.2.0 (최신 호환)
- `duckdb`: 0.9.2 → 1.1.3 (최신 기능)
- `aiofiles`: 0.24.0 → 24.1.0 (최신 버전)
- `pydantic`: 추가 (>=2.10.0, v2 호환)

### 유지된 패키지
- `fastapi==0.104.1` - 웹 프레임워크
- `uvicorn==0.24.0` - ASGI 서버
- `openpyxl==3.1.2` - Excel 처리
- `pyarrow==14.0.2` - Parquet 지원 (scripts에서 사용)
- `psutil==5.9.8` - 시스템 모니터링

## 설치 방법

```bash
cd Project
pip install -r requirements.txt
```

## 의존성 검증

모든 패키지가 실제 프로젝트 코드에서 사용되는지 검증되었음:
- ✅ fastapi, uvicorn, pydantic - API 서버
- ✅ pandas, duckdb, ijson - 데이터 처리  
- ✅ aiofiles, psutil - 파일 및 시스템 처리
- ✅ openpyxl - Excel 생성
- ✅ pyarrow - Parquet 변환 스크립트
- ✅ python-multipart - 파일 업로드
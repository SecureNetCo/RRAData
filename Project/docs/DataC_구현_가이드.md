# DataC 구현 가이드 - 성공/실패 데이터 페이지 추가

## 📋 프로젝트 개요

### 현재 상황
- **기존 DataA/B**: 9개 데이터셋 완전 구현
- **새로운 DataA 추가**: 10번 safetykoreachild 추가 예정  
- **새로운 DataC 구현**: 기존 10개 데이터의 성공/실패 버전 구현

### 목표
- **DataA**: 10개 데이터셋 완성 (1-10번)
- **DataC**: 성공/실패로 분류된 20개 페이지 (10개 × 2)
- **미래 대비**: 현재 데이터가 없는 항목들도 미리 구현

---

## 📂 데이터 파일 구조

### 기존 데이터 (DataA/B)
```
/Project/data/last/
├── 1_safetykorea_flattened.json/parquet
├── 2_wadiz_flattened.json/parquet
├── 3_efficiency_flattened.json/parquet
├── 4_high_efficiency_flattened.json/parquet
├── 5_standby_power_flattened.json/parquet
├── 6_approval_flattened.json/parquet
├── 7_declare_flattened.json/parquet
├── 8_kwtc_flattened.json/parquet
├── 9_recall_flattened.json/parquet
└── (10_safetykoreachild_flattened.json/parquet - 추가 예정)
```

### 새로운 데이터 (DataC) 
```
/Project/parquet/
├── 기존 10개 데이터 (DataA 업데이트용)
└── enhanced/
    ├── success/
    │   ├── 1_safetykorea_flattened_success.parquet ✅
    │   ├── 2_wadiz_flattened_success.parquet ✅
    │   ├── 3_efficiency_flattened_success.parquet ✅
    │   ├── 4_high_efficiency_flattened_success.parquet ✅
    │   ├── 5_standby_power_flattened_success.parquet ✅
    │   ├── 6_approval_flattened_success.parquet ❌ (미래 대비)
    │   ├── 7_declare_flattened_success.parquet ❌ (미래 대비)
    │   ├── 8_kwtc_flattened_success.parquet ❌ (미래 대비)
    │   ├── 9_recall_flattened_success.parquet ✅
    │   └── 10_safetykoreachild_flattened_success.parquet ✅
    └── failed/
        ├── 1_safetykorea_flattened_failed.parquet ✅
        ├── 2_wadiz_flattened_failed.parquet ✅
        ├── 3_efficiency_flattened_failed.parquet ✅
        ├── 4_high_efficiency_flattened_failed.parquet ✅
        ├── 5_standby_power_flattened_failed.parquet ✅
        ├── 6_approval_flattened_failed.parquet ✅
        ├── 7_declare_flattened_failed.parquet ✅
        ├── 8_kwtc_flattened_failed.parquet ✅
        ├── 9_recall_flattened_failed.parquet ✅
        └── 10_safetykoreachild_flattened_failed.parquet ✅
```

### 데이터 구조 특징
- **기존 필드**: 원본 데이터의 모든 필드 포함
- **추가 필드**: 매칭 결과 관련 14개 새 컬럼 추가
- **파일 형식**: Parquet 파일 (고성능 DuckDB 처리)

---

## 🎯 URL 구조 및 라우팅

### 기존 URL 패턴
```
/search/dataA/safetykorea
/search/dataA/wadiz
...
/search/dataB/high-efficiency
/search/dataB/standby-power
...
```

### 새로운 DataC URL 패턴
```
/search/dataC/success/safetykorea
/search/dataC/success/wadiz
/search/dataC/success/efficiency
...
/search/dataC/failed/safetykorea  
/search/dataC/failed/wadiz
/search/dataC/failed/efficiency
...
```

### 데이터셋 매핑 테이블

| 번호 | 내부 키 | URL 경로 | 파일명 패턴 | 한글명 |
|------|---------|----------|-------------|--------|
| 1 | safetykorea | safetykorea | 1_safetykorea_flattened | SafetyKorea 인증정보 |
| 2 | wadiz | wadiz | 2_wadiz_flattened | 와디즈 |
| 3 | efficiency | efficiency | 3_efficiency_flattened | 에너지소비효율등급 |
| 4 | high_efficiency | high-efficiency | 4_high_efficiency_flattened | 고효율기자재 |
| 5 | standby_power | standby-power | 5_standby_power_flattened | 대기전력저감 |
| 6 | approval | approval-details | 6_approval_flattened | 승인정보 |
| 7 | declare | declaration-details | 7_declare_flattened | 신고정보 |
| 8 | kwtc | kwtc | 8_kwtc_flattened | KC인증 |
| 9 | recall | domestic-latest | 9_recall_flattened | 리콜정보 |
| 10 | safetykoreachild | safetykoreachild | 10_safetykoreachild_flattened | SafetyKorea 어린이제품 |

---

## 🔧 구현 단계별 가이드

### 1단계: DataA에 10번 safetykoreachild 추가

#### 1.1 파일 복사
```bash
# parquet 폴더에서 data/last 폴더로 복사
cp /Project/parquet/10_safetykoreachild_flattened.parquet /Project/data/last/
cp /Project/parquet/10_safetykoreachild_flattened.parquet /Project/data/last/10_safetykoreachild_flattened.json
```

#### 1.2 field_settings.json 업데이트
파일: `/Project/config/field_settings.json`

추가할 섹션:
```json
"dataA": {
  "safetykoreachild": {
    "category_info": {
      "display_name": "SafetyKorea 어린이제품 인증정보",
      "description": "SafetyKorea 어린이제품 안전 인증정보",
      "icon": "child",
      "data_file": "data/last/10_safetykoreachild_flattened.parquet",
      "is_large_file": false,
      "data_path": "root"
    },
    "display_fields": [
      // 파일 스키마 분석 후 전체 필드 추가
    ],
    "download_fields": [
      // 전체 필드 포함
    ],
    "search_fields": [
      {"field": "all", "name": "전체", "placeholder": "전체 필드에서 검색"},
      // 주요 필드들 추가 (업체명, 제품명, 인증번호 등)
    ],
    "field_types": {
      // 모든 필드의 타입 정의
    },
    "ui_settings": {
      "default_sort": {"field": "crawl_date", "direction": "desc"},
      "items_per_page": 20,
      "enable_search": true,
      "enable_download": true,
      "enable_pagination": true,
      "show_total_count": true
    }
  }
}
```

#### 1.3 네비게이션 메뉴 업데이트
파일: `/Project/static/search.html`

추가 위치: DataA 드롭다운 메뉴 내
```html
<a href="/search/dataA/safetykoreachild" class="dropdown-item">SafetyKorea 어린이제품</a>
```

### 2단계: DataC 카테고리 전체 구현

#### 2.1 field_settings.json에 DataC 섹션 추가

```json
"dataC": {
  "success": {
    "safetykorea": {
      "category_info": {
        "display_name": "SafetyKorea 인증정보 (매칭 성공)",
        "description": "SafetyKorea 인증정보 - 매칭에 성공한 데이터",
        "icon": "check-circle",
        "data_file": "parquet/enhanced/success/1_safetykorea_flattened_success.parquet",
        "is_large_file": false,
        "data_path": "root"
      },
      "display_fields": [
        // 기존 필드 + 14개 매칭 필드 모두 포함
      ],
      "download_fields": [
        // 전체 필드 포함
      ],
      "search_fields": [
        {"field": "all", "name": "전체", "placeholder": "전체 필드에서 검색"},
        // 기존 검색 필드 + 매칭 관련 필드
      ],
      "field_types": {
        // 모든 필드 타입 정의 (기존 + 14개 새 필드)
      }
    },
    "wadiz": { /* 동일한 구조 */ },
    "efficiency": { /* 동일한 구조 */ },
    "high_efficiency": { /* 동일한 구조 */ },
    "standby_power": { /* 동일한 구조 */ },
    "approval": { /* 데이터 없음 - 미래 대비 */ },
    "declare": { /* 데이터 없음 - 미래 대비 */ },
    "kwtc": { /* 데이터 없음 - 미래 대비 */ },
    "recall": { /* 동일한 구조 */ },
    "safetykoreachild": { /* 동일한 구조 */ }
  },
  "failed": {
    "safetykorea": { /* success와 동일, 파일 경로만 failed */ },
    "wadiz": { /* success와 동일, 파일 경로만 failed */ },
    "efficiency": { /* success와 동일, 파일 경로만 failed */ },
    "high_efficiency": { /* success와 동일, 파일 경로만 failed */ },
    "standby_power": { /* success와 동일, 파일 경로만 failed */ },
    "approval": { /* success와 동일, 파일 경로만 failed */ },
    "declare": { /* success와 동일, 파일 경로만 failed */ },
    "kwtc": { /* success와 동일, 파일 경로만 failed */ },
    "recall": { /* success와 동일, 파일 경로만 failed */ },
    "safetykoreachild": { /* success와 동일, 파일 경로만 failed */ }
  }
}
```

#### 2.2 네비게이션 메뉴 추가
파일: `/Project/static/search.html`

메인 네비게이션에 DataC 추가:
```html
<li class="nav-item dropdown">
  <a class="nav-link dropdown-toggle" href="#" id="dataCDropdown" role="button" data-bs-toggle="dropdown">
    데이터C (매칭결과)
  </a>
  <ul class="dropdown-menu" aria-labelledby="dataCDropdown">
    <!-- 성공 섹션 -->
    <li><h6 class="dropdown-header">매칭 성공</h6></li>
    <li><a href="/search/dataC/success/safetykorea" class="dropdown-item">SafetyKorea</a></li>
    <li><a href="/search/dataC/success/wadiz" class="dropdown-item">와디즈</a></li>
    <li><a href="/search/dataC/success/efficiency" class="dropdown-item">에너지효율등급</a></li>
    <li><a href="/search/dataC/success/high-efficiency" class="dropdown-item">고효율기자재</a></li>
    <li><a href="/search/dataC/success/standby-power" class="dropdown-item">대기전력저감</a></li>
    <li><a href="/search/dataC/success/approval-details" class="dropdown-item">승인정보</a></li>
    <li><a href="/search/dataC/success/declaration-details" class="dropdown-item">신고정보</a></li>
    <li><a href="/search/dataC/success/kwtc" class="dropdown-item">KC인증</a></li>
    <li><a href="/search/dataC/success/domestic-latest" class="dropdown-item">리콜정보</a></li>
    <li><a href="/search/dataC/success/safetykoreachild" class="dropdown-item">SafetyKorea 어린이</a></li>
    
    <li><hr class="dropdown-divider"></li>
    
    <!-- 실패 섹션 -->
    <li><h6 class="dropdown-header">매칭 실패</h6></li>
    <li><a href="/search/dataC/failed/safetykorea" class="dropdown-item">SafetyKorea</a></li>
    <li><a href="/search/dataC/failed/wadiz" class="dropdown-item">와디즈</a></li>
    <li><a href="/search/dataC/failed/efficiency" class="dropdown-item">에너지효율등급</a></li>
    <li><a href="/search/dataC/failed/high-efficiency" class="dropdown-item">고효율기자재</a></li>
    <li><a href="/search/dataC/failed/standby-power" class="dropdown-item">대기전력저감</a></li>
    <li><a href="/search/dataC/failed/approval-details" class="dropdown-item">승인정보</a></li>
    <li><a href="/search/dataC/failed/declaration-details" class="dropdown-item">신고정보</a></li>
    <li><a href="/search/dataC/failed/kwtc" class="dropdown-item">KC인증</a></li>
    <li><a href="/search/dataC/failed/domestic-latest" class="dropdown-item">리콜정보</a></li>
    <li><a href="/search/dataC/failed/safetykoreachild" class="dropdown-item">SafetyKorea 어린이</a></li>
  </ul>
</li>
```

### 3단계: API 라우팅 업데이트

#### 3.1 URL 매핑 추가
파일: `/Project/api/main.py`

기존 라우팅 함수에서 dataC 처리 추가가 필요하다면 업데이트.
현재는 동적 라우팅이므로 field_settings.json 설정만으로 자동 처리될 것으로 예상.

#### 3.2 파일 경로 매핑 확인
`search_category_data` 함수에서 dataC 경로 처리 확인:
- `dataC/success/*` → `parquet/enhanced/success/*_success.parquet`
- `dataC/failed/*` → `parquet/enhanced/failed/*_failed.parquet`

### 4단계: 데이터 구조 분석 및 필드 설정

#### 4.1 각 데이터셋별 스키마 분석
각 파일에 대해 다음 명령어로 스키마 분석:
```python
import duckdb
conn = duckdb.connect()
result = conn.execute('DESCRIBE SELECT * FROM "파일경로.parquet" LIMIT 1').fetchall()
for row in result:
    print(f'{row[0]}: {row[1]}')
```

#### 4.2 필드 매핑 작업
1. **기존 필드**: 원본 데이터에서 가져온 필드들
2. **새로운 필드**: 14개 매칭 관련 필드들
3. **한글 매핑**: 모든 필드에 대한 한글명 정의
4. **타입 정의**: DuckDB BinderError 방지를 위한 정확한 타입 설정

#### 4.3 공통 새 필드 예상 목록 (실제 분석 후 확정)
매칭 프로세스에서 추가될 것으로 예상되는 필드들:
- 매칭 상태 관련
- 매칭 점수/신뢰도 관련  
- 매칭 일시 정보
- 매칭 알고리즘 버전 정보
- 오류/실패 이유 (failed 데이터)
- 등등...

---

## ⚠️ 주의사항 및 고려사항

### DuckDB BinderError 방지
- 모든 숫자형 필드는 `field_types`에서 정확한 타입 지정
- 범용 CAST 해결책이 적용되어 있지만, 정확한 타입 정의 권장
- STRUCT 타입 필드는 자동으로 검색에서 제외됨

### 파일 존재 여부 처리
- 현재 존재하지 않는 success 파일들 (6,7,8번):
  - 설정은 미리 완료
  - 파일이 없을 경우 적절한 에러 메시지 표시
  - 데이터가 추가되면 바로 작동하도록 준비

### 성능 최적화
- 모든 파일이 Parquet 형식으로 DuckDB 고성능 처리
- 대용량 파일의 경우 `is_large_file: true` 설정
- 적절한 페이지네이션 설정 (기본 20개/페이지)

### 사용자 경험
- 성공/실패 데이터 구분을 위한 명확한 UI 표시
- 매칭 관련 필드들의 의미 있는 한글 번역
- 검색 기능에서 새로운 필드들도 활용

---

## 🔄 향후 데이터 추가 시 절차

### 새로운 원본 데이터 추가 시
1. **원본 파일**: `/data/last/`에 추가
2. **성공 파일**: `/parquet/enhanced/success/`에 추가 (있는 경우)
3. **실패 파일**: `/parquet/enhanced/failed/`에 추가
4. **설정 업데이트**: `field_settings.json`에 해당 섹션 추가
5. **메뉴 업데이트**: `search.html`에 링크 추가

### 기존 데이터의 success 버전 추가 시 (6,7,8번 등)
1. **파일 추가**: 해당 success 파일을 `/parquet/enhanced/success/`에 추가
2. **설정 확인**: 이미 구현되어 있으므로 바로 작동
3. **테스트**: 해당 페이지 접속하여 정상 작동 확인

### 완전히 새로운 데이터셋 추가 시 (11번, 12번 등)
1. **번호 할당**: 다음 순번 부여 (11, 12, ...)
2. **파일 배치**: 모든 위치에 파일 배치
   - `/data/last/{번호}_{이름}_flattened.parquet`
   - `/parquet/enhanced/success/{번호}_{이름}_flattened_success.parquet`
   - `/parquet/enhanced/failed/{번호}_{이름}_flattened_failed.parquet`
3. **전체 설정**: dataA, dataC 양쪽 모두 설정 추가
4. **메뉴 업데이트**: 모든 관련 메뉴에 추가

---

## 📋 구현 체크리스트

### 1단계: DataA 확장
- [ ] 10번 safetykoreachild 파일 복사
- [ ] field_settings.json에 dataA/safetykoreachild 섹션 추가
- [ ] 스키마 분석 및 필드 설정 완료
- [ ] search.html 메뉴 업데이트
- [ ] 테스트 및 검증

### 2단계: DataC 구현  
- [ ] field_settings.json에 dataC 섹션 전체 추가
- [ ] success 카테고리 10개 설정 (7개 실제 + 3개 미래대비)
- [ ] failed 카테고리 10개 설정 (10개 모두)
- [ ] 각 데이터셋별 스키마 분석
- [ ] 14개 새 필드 한글 매핑 완료
- [ ] search.html에 DataC 메뉴 추가
- [ ] 전체 페이지 테스트 및 검증

### 3단계: 최종 검증
- [ ] 모든 URL 접속 테스트
- [ ] 검색 기능 정상 작동 확인
- [ ] Excel 다운로드 정상 작동 확인
- [ ] 데이터 없는 페이지들 적절한 처리 확인
- [ ] 성능 테스트 (DuckDB 고성능 처리 확인)

---

## 🚀 최신 구현 현황 (2025.09.04)

### 완료된 작업

#### 1. Vercel Blob 환경변수 시스템 전면 개편
**문제**: 기존 9개 파일만 지원하던 시스템을 27개 파일로 확장
**해결책**: 카테고리별 명명 규칙으로 변경

**기존 환경변수 (삭제 예정)**:
```
R2_URL_1_SAFETYKOREA
R2_URL_2_WADIZ
...
R2_URL_9_RECALL
```

**새로운 환경변수 시스템**:
```bash
# DataA (10개) - 기본 데이터
R2_URL_DATAA_1_SAFETYKOREA
R2_URL_DATAA_2_WADIZ
R2_URL_DATAA_3_EFFICIENCY
R2_URL_DATAA_4_HIGH_EFFICIENCY
R2_URL_DATAA_5_STANDBY_POWER
R2_URL_DATAA_6_APPROVAL
R2_URL_DATAA_7_DECLARE
R2_URL_DATAA_8_KC_CERTIFICATION
R2_URL_DATAA_9_RECALL
R2_URL_DATAA_10_SAFETYKOREACHILD

# DataC Success (7개) - 매칭 성공 데이터
R2_URL_DATAC_SUCCESS_1_SAFETYKOREA
R2_URL_DATAC_SUCCESS_2_WADIZ
R2_URL_DATAC_SUCCESS_3_EFFICIENCY
R2_URL_DATAC_SUCCESS_4_HIGH_EFFICIENCY
R2_URL_DATAC_SUCCESS_5_STANDBY_POWER
R2_URL_DATAC_SUCCESS_9_RECALL
R2_URL_DATAC_SUCCESS_10_SAFETYKOREACHILD

# DataC Failed (10개) - 매칭 실패 데이터
R2_URL_DATAC_FAILED_1_SAFETYKOREA
R2_URL_DATAC_FAILED_2_WADIZ
R2_URL_DATAC_FAILED_3_EFFICIENCY
R2_URL_DATAC_FAILED_4_HIGH_EFFICIENCY
R2_URL_DATAC_FAILED_5_STANDBY_POWER
R2_URL_DATAC_FAILED_6_APPROVAL
R2_URL_DATAC_FAILED_7_DECLARE
R2_URL_DATAC_FAILED_8_KC_CERTIFICATION
R2_URL_DATAC_FAILED_9_RECALL
R2_URL_DATAC_FAILED_10_SAFETYKOREACHILD
```

#### 2. 자동화 스크립트 업데이트
**파일**: `/automation/auto_blob_update.py`
**변경사항**:
- 9개 → 27개 파일 지원 확장
- 새로운 환경변수 명명 규칙 적용
- 크로스 플랫폼 호환성 유지
- Vercel CLI 통합 자동화

#### 3. 백엔드 API 전면 개편
**파일**: `/api/main.py`

**추가된 라우트**:
```python
# DataC HTML 페이지 라우트
@app.get("/search/dataC/{result_type}/{subcategory}")
async def serve_search_page_data_c(result_type: str, subcategory: str)

# DataC API 검색 라우트  
@app.post("/api/search/dataC/{result_type}/{subcategory}")
async def search_data_c(result_type: str, subcategory: str, request: SearchRequest)
```

**환경변수 매핑 업데이트**:
- 기존 2-parameter: `get_data_file_path(category, subcategory)`
- 새로운 3-parameter: `get_data_file_path_c(category, result_type, subcategory)`
- DataC 전용 blob URL 매핑 함수 구현

#### 4. Git 계정 설정 수정
**문제**: 개인 계정과 회사 계정 충돌로 Vercel 배포 실패
**해결책**: 
```bash
git config --local user.name "SecureNetCo"
git config --local user.email "help@securenet.kr"
```

### 현재 상태

#### ✅ 완료된 부분
1. **환경변수 시스템**: 27개 파일 모두 Vercel에 업로드 및 환경변수 설정 완료
2. **자동화 스크립트**: 전체 데이터 갱신 프로세스 완전 자동화
3. **백엔드 API**: DataC 3-parameter 구조 완전 구현
4. **Git 설정**: Vercel 배포 권한 문제 해결

#### ⚠️ 진행 중
1. **API 테스트**: 새로운 환경변수로 27개 파일 접근 테스트 필요
2. **프론트엔드**: search.html의 DataC 메뉴 완전 구현 필요
3. **레거시 정리**: 기존 9개 환경변수 및 오래된 코드 정리 필요

### 다음 단계

#### 즉시 필요한 작업
1. **환경변수 연동 테스트**: 백엔드에서 새로운 환경변수로 파일 접근 확인
2. **DataC 라우트 테스트**: 3-parameter 구조 정상 작동 확인
3. **레거시 코드 정리**: 사용하지 않는 health, cert 등 엔드포인트 정리

#### 향후 작업
1. **프론트엔드 완성**: DataC 네비게이션 메뉴 추가
2. **설정 파일 업데이트**: field_settings.json에 DataC 섹션 추가
3. **통합 테스트**: 전체 27개 파일 검색/다운로드 기능 검증

### 기술적 개선사항

#### 환경변수 명명 규칙의 장점
- **확장성**: 새로운 카테고리 추가 시 일관된 패턴 유지
- **가독성**: DATAA, DATAC_SUCCESS, DATAC_FAILED로 용도 명확
- **유지보수**: 카테고리별 그룹핑으로 관리 효율성 증대

#### API 아키텍처 개선
- **동적 라우팅**: 3-parameter 구조로 확장성 확보
- **코드 재사용**: 기존 검색 로직을 DataC에서 재사용
- **오류 처리**: 파일 없음, 환경변수 없음 등 상세 에러 메시지

---

이 가이드를 참조하여 단계별로 구현하고, 향후 데이터 추가 시에도 동일한 절차를 따라 진행하면 됩니다.
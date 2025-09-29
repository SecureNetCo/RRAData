# BinderError 해결 로그

## 개요
DuckDB에서 발생하는 BinderError와 ConversionError 문제들을 해결한 과정을 기록합니다.

## 해결된 문제들

### 1. KWTC 인증등록정보망 (kwtc) - ✅ 해결됨

**문제**: BinderError 및 ConversionError 발생
**원인**: 
- ORDER BY절에서 BIGINT 타입 필드 "no"를 빈 문자열('')과 비교
- field_types에서 실제 BIGINT 필드들을 "text"로 잘못 정의

**해결방법**:
1. **field_settings.json 수정**:
   ```json
   "field_types": {
     "crtfcDe": "text",        // DATE → text (문자열 형태)
     "frstCrtfcDe": "text",    // DATE → text  
     "crtfcVer": "integer",    // TEXT → integer (실제 BIGINT)
     "modelCnt": "integer",    // TEXT → integer (실제 BIGINT)
     "no": "integer",          // TEXT → integer (실제 BIGINT)
     "totalCnt": "integer"     // TEXT → integer (실제 BIGINT)
   }
   ```

2. **duckdb_processor.py 수정**:
   ```python
   # 숫자 타입 필드 정의
   numeric_fields = {'crtfcVer', 'modelCnt', 'no', 'totalCnt'}
   
   # ORDER BY 절 수정
   if first_field in numeric_fields:
       order_by = f'"{first_field}"'  # 숫자는 NULL 체크만
   else:
       order_by = f'CASE WHEN "{first_field}" = \'\' THEN NULL ELSE "{first_field}" END'
       
   # WHERE 절 LIKE 검색 수정  
   if field in numeric_fields:
       conditions.append(f"CAST({table_alias}\"{field}\" AS VARCHAR) LIKE ?")
   else:
       conditions.append(f"{table_alias}\"{field}\" LIKE ?")
   ```

**결과**: KWTC 검색 및 테이블 표시 정상 작동 ✅

### 2. 리콜정보 (domestic-latest) - ✅ 해결됨

**문제**: 검색은 되지만 테이블이 생성되지 않음
**원인**: 
- display_fields가 빈 배열 []
- field_types 누락
- 잘못된 data_file 경로

**해결방법**:
1. **field_settings.json 완전 재구성**:
   - display_fields: 17개 필드 모두 추가
   - field_types: 전체 필드에 대한 타입 정의
   - data_file 경로 수정: "data/last/9_recall_flattened.parquet"

**결과**: 리콜정보 검색 및 테이블 표시 정상 작동 ✅

---

## 현재 진행 중인 문제

### 3. 효율등급 (efficiency-rating) - ✅ 해결완료

**문제**: BinderError 발생
**원인 발견**: field_types와 실제 파라켓 스키마 완전 불일치

**실제 파라켓 스키마** (16개 컬럼):
```
1. crawl_date (object)
2. 신청번호 (object)  
3. 업체명 (object)
4. 모델명 (object)
5. 월간소비전력량 (object)
6. 용량 (object)
7. 효율등급 (object)
8. 구효율등급 (object)  
9. 완료일 (object)
10. detail_url (object)
11. product_id (object)
12. category_code (object) 
13. category_name (object)
14. 에너지소비효율등급정보 (float64)
15. 에너지소비효율등급제품상세정보 (float64)
16. 제품이미지 (object)
```

**잘못 설정된 field_types**:
- 존재하지 않는 필드들: "효율값", "기준값", "효율비", "단위", "대리점_조합", "대리점_전화번호", "수입업체", "기타", "산업부인증번호"
- 실제 존재하는 필드 누락: "월간소비전력량", "구효율등급", "완료일", "에너지소비효율등급정보" 등

**해결 과정**:
1. ✅ 실제 파라켓 스키마 분석 완료 (16개 필드)
2. ✅ field_types를 실제 스키마에 맞게 완전 재구성:
   ```json
   "field_types": {
     "crawl_date": "text",
     "신청번호": "text", "업체명": "text", "모델명": "text",
     "월간소비전력량": "text", "용량": "text", "효율등급": "text", "구효율등급": "text",
     "완료일": "text", "detail_url": "text", "product_id": "text", 
     "category_code": "text", "category_name": "text",
     "에너지소비효율등급정보": "double",  // float64 타입
     "에너지소비효율등급제품상세정보": "double",  // float64 타입
     "제품이미지": "text"
   }
   ```
3. ✅ display_fields를 실제 필드로 수정 (16개 전체 필드)
4. ✅ search_fields를 실제 필드로 수정
5. ✅ download_fields를 전체 실제 필드로 설정 (16개)
6. ✅ DuckDB 직접 쿼리 테스트 성공
7. ✅ 서버 정상 작동 확인 (/health 엔드포인트)
8. ✅ API 테스트 성공 - 정상 검색 결과 반환

**최종 결과**:
- ✅ total_count: 7,667개 데이터 정상 인식
- ✅ "삼성" 키워드로 50개 결과 정상 검색
- ✅ 모든 필드 정상 반환 (신청번호, 업체명, 모델명, 월간소비전력량, 용량, 효율등급 등)
- ✅ DuckDB 고성능 처리 (0.23초, 220 records/second)
- ✅ BinderError 완전 해결

**해결됨**: efficiency-rating 카테고리 정상 작동 ✅

---

## 일반적인 BinderError 패턴

1. **타입 불일치**: field_types와 실제 파라켓 스키마가 다름
2. **ORDER BY 에러**: 숫자 필드를 문자열과 비교
3. **LIKE 에러**: 숫자 필드에 직접 LIKE 적용
4. **빈 설정**: display_fields나 field_types가 비어있음

## 해결 체크리스트

- [ ] 파라켓 파일 스키마 확인
- [ ] field_types 정확성 검증  
- [ ] 숫자 필드 목록 파악
- [ ] ORDER BY 로직 수정
- [ ] LIKE 검색 로직 수정
- [ ] API 테스트
- [ ] 웹 인터페이스 테스트
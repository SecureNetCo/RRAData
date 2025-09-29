#!/usr/bin/env python3
"""
고도화된 필드 분석 스크립트
- DataA, DataB, DataC 모든 카테고리 지원
- field_settings.json vs 실제 parquet 파일 필드 비교
- 누락된 필드 및 추가 필드 감지
- 상세한 갭 분석 보고서 생성
"""

import json
import os
from pathlib import Path
from collections import defaultdict

# 필드 정보를 수집하기 위한 DuckDB 사용 (pyarrow 대신)
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("⚠️ DuckDB가 설치되지 않았습니다. 실제 파일 분석 기능이 제한됩니다.")

def extract_field_settings():
    """field_settings.json에서 모든 카테고리의 필드 정보 추출"""
    
    field_settings_path = Path("/Users/jws/cursorPrj/DataPagePrj/Project/config/field_settings.json")
    
    with open(field_settings_path, 'r', encoding='utf-8') as f:
        field_settings = json.load(f)
    
    # 파일명 매핑 (모든 카테고리)
    file_mapping = {
        # DataA Base Files
        "safetykorea": "1_safetykorea_flattened.parquet",
        "wadiz-makers": "2_wadiz_flattened.parquet",
        "efficiency-rating": "3_efficiency_flattened.parquet",
        "high-efficiency": "4_high_efficiency_flattened.parquet",
        "standby-power": "5_standby_power_flattened.parquet",
        "approval": "6_approval_flattened.parquet",
        "declaration-details": "7_declare_flattened.parquet",
        "kwtc": "8_kwtc_flattened.parquet",
        "recall": "9_recall_flattened.parquet",
        "safetykoreachild": "10_safetykoreachild_flattened.parquet",
        "rra-cert": "11_rra_cert_flattened.parquet",
        "rra-self-cert": "12_rra_self_cert_flattened.parquet",
        "safetykoreahome": "13_safetykoreahome_flattened.parquet"
    }
    
    results = []
    
    # DataA 처리
    if "dataA" in field_settings:
        for subcategory, config in field_settings["dataA"].items():
            if subcategory in file_mapping:
                result = process_category_config(
                    "DataA", subcategory, config, file_mapping[subcategory]
                )
                results.append(result)
    
    # DataB 처리
    if "dataB" in field_settings:
        for subcategory, config in field_settings["dataB"].items():
            if subcategory in file_mapping:
                result = process_category_config(
                    "DataB", subcategory, config, file_mapping[subcategory]
                )
                results.append(result)
    
    # DataC 처리
    if "dataC" in field_settings:
        for subcategory, config in field_settings["dataC"].items():
            if subcategory in file_mapping:
                # DataC Success 파일
                success_file = f"enhanced/success/{file_mapping[subcategory].replace('.parquet', '_success.parquet')}"
                result_success = process_category_config(
                    "DataC_Success", subcategory, config, success_file
                )
                results.append(result_success)
                
                # DataC Failed 파일
                failed_file = f"enhanced/failed/{file_mapping[subcategory].replace('.parquet', '_failed.parquet')}"
                result_failed = process_category_config(
                    "DataC_Failed", subcategory, config, failed_file
                )
                results.append(result_failed)
    
    return results

def process_category_config(category, subcategory, config, file_path):
    """단일 카테고리 설정 처리"""
    
    category_name = config.get("카테고리명", config.get("category_info", {}).get("display_name", subcategory))
    
    # display_fields에서 컬럼명 추출
    display_fields = config.get("display_fields", [])
    display_columns = []
    for field in display_fields:
        if isinstance(field, dict) and "field" in field:
            display_columns.append({
                "field": field["field"],
                "name": field.get("name", field["field"]),
                "width": field.get("width", "auto")
            })
    
    # download_fields에서 다운로드 컬럼 추출
    download_fields = config.get("download_fields", [])
    
    # search_fields에서 검색 필드 추출
    search_fields = config.get("search_fields", [])
    search_field_names = [sf.get("field") for sf in search_fields if isinstance(sf, dict) and sf.get("field") != "all"]
    
    return {
        "category": category,
        "subcategory": subcategory,
        "file_path": file_path,
        "category_name": category_name,
        "display_columns": display_columns,
        "download_fields": download_fields,
        "search_fields": search_field_names,
        "total_display_fields": len(display_columns),
        "total_download_fields": len(download_fields),
        "total_search_fields": len(search_field_names)
    }

def get_actual_parquet_columns(file_path):
    """실제 parquet 파일의 컬럼명 조회 (DuckDB 사용)"""
    
    if not DUCKDB_AVAILABLE:
        return None, "DuckDB not available"
    
    full_path = Path("/Users/jws/cursorPrj/DataPagePrj/Project/parquet") / file_path
    
    if not full_path.exists():
        return None, f"File not found: {full_path}"
    
    try:
        # DuckDB로 파일 스키마 조회
        conn = duckdb.connect()
        result = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{full_path}') LIMIT 1").fetchall()
        conn.close()
        
        # 컬럼명만 추출
        columns = [row[0] for row in result]
        return columns, None
        
    except Exception as e:
        return None, str(e)

def analyze_field_gaps(settings_results):
    """설정과 실제 파일 간의 필드 갭 분석"""
    
    gap_analysis = []
    
    for result in settings_results:
        file_path = result['file_path']
        
        # 실제 파일의 컬럼 조회
        actual_columns, error = get_actual_parquet_columns(file_path)
        
        if error:
            gap_info = {
                **result,
                "actual_columns": None,
                "actual_column_count": 0,
                "error": error,
                "missing_display_fields": [],
                "missing_download_fields": [],
                "extra_columns": [],
                "coverage_ratio": 0.0
            }
        else:
            # 설정된 필드들
            configured_display = set([col['field'] for col in result['display_columns']])
            configured_download = set(result['download_fields'])
            configured_search = set(result['search_fields'])
            all_configured = configured_display | configured_download | configured_search
            
            # 실제 파일 컬럼
            actual_set = set(actual_columns)
            
            # 갭 분석
            missing_display = configured_display - actual_set
            missing_download = configured_download - actual_set
            missing_search = configured_search - actual_set
            extra_columns = actual_set - all_configured
            
            # 커버리지 계산
            coverage_ratio = len(all_configured & actual_set) / len(actual_set) if actual_set else 0
            
            gap_info = {
                **result,
                "actual_columns": actual_columns,
                "actual_column_count": len(actual_columns),
                "error": None,
                "missing_display_fields": list(missing_display),
                "missing_download_fields": list(missing_download),
                "missing_search_fields": list(missing_search),
                "extra_columns": list(extra_columns),
                "coverage_ratio": coverage_ratio,
                "total_configured_fields": len(all_configured),
                "matched_fields": len(all_configured & actual_set)
            }
        
        gap_analysis.append(gap_info)
    
    return gap_analysis

def write_comprehensive_field_report(settings_results, gap_analysis):
    """종합적인 필드 분석 보고서 작성"""
    
    output_path = "/Users/jws/cursorPrj/DataPagePrj/Project/enhanced_field_analysis_report.txt"
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("=" * 120 + "\n")
        f.write("🔬 종합 필드 분석 보고서 (DataA + DataB + DataC 포함)\n")
        f.write("field_settings.json vs 실제 parquet 파일 비교\n")
        f.write("=" * 120 + "\n\n")
        
        # 1. 전체 현황 요약
        write_overall_summary(f, settings_results, gap_analysis)
        
        # 2. 카테고리별 상세 분석
        write_category_analysis(f, gap_analysis)
        
        # 3. 갭 분석 요약
        write_gap_summary(f, gap_analysis)
        
        # 4. 누락 필드 상세 목록
        write_missing_fields_detail(f, gap_analysis)
        
        # 5. 추가 필드 목록 (설정에 없는 실제 필드)
        write_extra_fields_detail(f, gap_analysis)
        
        # 6. 권장사항
        write_recommendations(f, gap_analysis)
    
    print(f"\n📄 종합 필드 분석 보고서 저장: {output_path}")

def write_overall_summary(f, settings_results, gap_analysis):
    """전체 현황 요약 작성"""
    
    f.write("📊 전체 현황 요약\n")
    f.write("-" * 60 + "\n")
    
    # 카테고리별 통계
    categories = defaultdict(int)
    for result in settings_results:
        categories[result['category']] += 1
    
    total_files = len(settings_results)
    analyzable_files = len([g for g in gap_analysis if g['error'] is None])
    error_files = total_files - analyzable_files
    
    f.write(f"총 설정 파일: {total_files}개\n")
    for category, count in sorted(categories.items()):
        f.write(f"  - {category}: {count}개\n")
    
    f.write(f"\n분석 가능한 파일: {analyzable_files}개\n")
    f.write(f"분석 불가 파일: {error_files}개\n")
    
    if analyzable_files > 0:
        # 전체 통계
        total_display = sum([g['total_display_fields'] for g in gap_analysis if g['error'] is None])
        total_download = sum([g['total_download_fields'] for g in gap_analysis if g['error'] is None])
        total_actual = sum([g['actual_column_count'] for g in gap_analysis if g['error'] is None])
        avg_coverage = sum([g['coverage_ratio'] for g in gap_analysis if g['error'] is None]) / analyzable_files
        
        f.write(f"\n📈 필드 통계:\n")
        f.write(f"  전체 설정된 표시 필드: {total_display}개\n")
        f.write(f"  전체 설정된 다운로드 필드: {total_download}개\n")
        f.write(f"  전체 실제 파일 컬럼: {total_actual}개\n")
        f.write(f"  평균 커버리지: {avg_coverage*100:.1f}%\n")
    
    f.write("\n")

def write_category_analysis(f, gap_analysis):
    """카테고리별 상세 분석 작성"""
    
    categories = ['DataA', 'DataB', 'DataC_Success', 'DataC_Failed']
    
    for category in categories:
        category_results = [g for g in gap_analysis if g['category'] == category]
        
        if not category_results:
            continue
        
        f.write("\n" + "=" * 120 + "\n")
        f.write(f"📂 {category} 카테고리 상세 분석\n")
        f.write("-" * 60 + "\n")
        
        for result in category_results:
            f.write(f"\n🗂️ {result['subcategory']} ({result['category_name']})\n")
            f.write(f"   파일: {result['file_path']}\n")
            
            if result['error']:
                f.write(f"   ❌ 분석 실패: {result['error']}\n")
                continue
            
            f.write(f"   📊 표시 필드: {result['total_display_fields']}개\n")
            f.write(f"   📥 다운로드 필드: {result['total_download_fields']}개\n")
            f.write(f"   🔍 검색 필드: {result['total_search_fields']}개\n")
            f.write(f"   📁 실제 컬럼: {result['actual_column_count']}개\n")
            f.write(f"   📈 커버리지: {result['coverage_ratio']*100:.1f}%\n")
            
            # 갭 정보
            if result['missing_display_fields']:
                f.write(f"   ⚠️ 누락된 표시 필드: {len(result['missing_display_fields'])}개\n")
            
            if result['missing_download_fields']:
                f.write(f"   ⚠️ 누락된 다운로드 필드: {len(result['missing_download_fields'])}개\n")
            
            if result['extra_columns']:
                f.write(f"   ➕ 추가 컬럼: {len(result['extra_columns'])}개\n")

def write_gap_summary(f, gap_analysis):
    """갭 분석 요약 작성"""
    
    f.write("\n" + "=" * 120 + "\n")
    f.write("🚨 갭 분석 요약\n")
    f.write("-" * 60 + "\n")
    
    analyzable = [g for g in gap_analysis if g['error'] is None]
    
    if not analyzable:
        f.write("분석 가능한 파일이 없습니다.\n")
        return
    
    # 문제가 있는 파일들
    files_with_missing_display = [g for g in analyzable if g['missing_display_fields']]
    files_with_missing_download = [g for g in analyzable if g['missing_download_fields']]
    files_with_extra = [g for g in analyzable if g['extra_columns']]
    low_coverage_files = [g for g in analyzable if g['coverage_ratio'] < 0.8]
    
    f.write(f"📊 문제 현황:\n")
    f.write(f"  누락된 표시 필드가 있는 파일: {len(files_with_missing_display)}개\n")
    f.write(f"  누락된 다운로드 필드가 있는 파일: {len(files_with_missing_download)}개\n")
    f.write(f"  추가 컬럼이 있는 파일: {len(files_with_extra)}개\n")
    f.write(f"  낮은 커버리지(<80%) 파일: {len(low_coverage_files)}개\n")
    
    # TOP 문제 파일들
    f.write(f"\n🔥 주요 문제 파일 TOP 5:\n")
    problem_files = sorted(analyzable, 
                          key=lambda x: len(x['missing_display_fields']) + len(x['missing_download_fields']), 
                          reverse=True)[:5]
    
    for i, pf in enumerate(problem_files, 1):
        total_missing = len(pf['missing_display_fields']) + len(pf['missing_download_fields'])
        if total_missing > 0:
            f.write(f"  {i}. {pf['file_path']} - {total_missing}개 필드 누락\n")

def write_missing_fields_detail(f, gap_analysis):
    """누락 필드 상세 목록 작성"""
    
    f.write("\n" + "=" * 120 + "\n")
    f.write("❌ 누락된 필드 상세 목록\n")
    f.write("-" * 60 + "\n")
    
    analyzable = [g for g in gap_analysis if g['error'] is None]
    files_with_missing = [g for g in analyzable if g['missing_display_fields'] or g['missing_download_fields']]
    
    if not files_with_missing:
        f.write("누락된 필드가 없습니다. 모든 설정이 올바릅니다! ✅\n")
        return
    
    for result in files_with_missing:
        f.write(f"\n📁 {result['file_path']} ({result['category']})\n")
        
        if result['missing_display_fields']:
            f.write(f"  🔸 누락된 표시 필드 ({len(result['missing_display_fields'])}개):\n")
            for field in sorted(result['missing_display_fields']):
                f.write(f"    - {field}\n")
        
        if result['missing_download_fields']:
            f.write(f"  🔸 누락된 다운로드 필드 ({len(result['missing_download_fields'])}개):\n")
            for field in sorted(result['missing_download_fields']):
                f.write(f"    - {field}\n")

def write_extra_fields_detail(f, gap_analysis):
    """추가 필드 상세 목록 작성"""
    
    f.write("\n" + "=" * 120 + "\n")
    f.write("➕ 추가 필드 목록 (설정에 없는 실제 컬럼)\n")
    f.write("-" * 60 + "\n")
    
    analyzable = [g for g in gap_analysis if g['error'] is None]
    files_with_extra = [g for g in analyzable if g['extra_columns']]
    
    if not files_with_extra:
        f.write("모든 실제 컬럼이 설정에 포함되어 있습니다! ✅\n")
        return
    
    for result in files_with_extra:
        f.write(f"\n📁 {result['file_path']} ({result['category']})\n")
        f.write(f"  🔸 추가 컬럼 ({len(result['extra_columns'])}개):\n")
        for field in sorted(result['extra_columns']):
            f.write(f"    - {field}\n")

def write_recommendations(f, gap_analysis):
    """권장사항 작성"""
    
    f.write("\n" + "=" * 120 + "\n")
    f.write("💡 권장사항 및 개선 방안\n")
    f.write("-" * 60 + "\n")
    
    analyzable = [g for g in gap_analysis if g['error'] is None]
    
    f.write("🎯 즉시 해결 필요:\n")
    
    # 누락 필드 해결
    missing_count = len([g for g in analyzable if g['missing_display_fields'] or g['missing_download_fields']])
    if missing_count > 0:
        f.write(f"  1. {missing_count}개 파일의 누락된 필드 설정 추가\n")
        f.write("     - field_settings.json에 누락된 필드들을 추가하거나\n")
        f.write("     - 실제 파일에서 해당 필드가 제거되었는지 확인\n")
    
    # 추가 필드 활용
    extra_count = len([g for g in analyzable if g['extra_columns']])
    if extra_count > 0:
        f.write(f"  2. {extra_count}개 파일의 추가 컬럼 활용 검토\n")
        f.write("     - 유용한 추가 컬럼이 있다면 display_fields나 download_fields에 추가\n")
        f.write("     - 불필요한 컬럼이라면 데이터 파이프라인에서 제거 검토\n")
    
    f.write("\n🔄 지속적인 개선:\n")
    f.write("  3. 자동 필드 검증 시스템 구축\n")
    f.write("     - CI/CD 파이프라인에 필드 일치성 검사 추가\n")
    f.write("     - 새로운 필드 추가 시 자동 알림\n")
    
    f.write("  4. DataC Success/Failed 파일 별도 설정 고려\n")
    f.write("     - Enhanced 필드에 대한 별도 UI 표시 방안\n")
    f.write("     - Success와 Failed 데이터의 차별화된 활용\n")
    
    f.write("\n📊 성능 최적화:\n")
    f.write("  5. 불필요한 컬럼 제거로 파일 크기 최적화\n")
    f.write("  6. 자주 사용되는 필드 우선순위 조정\n")
    f.write("  7. 검색 성능 향상을 위한 인덱싱 컬럼 선별\n")

if __name__ == "__main__":
    print("🚀 고도화된 필드 분석 시작")
    print("DataA + DataB + DataC 포함, 실제 파일과의 갭 분석")
    
    # 1. field_settings.json에서 설정 정보 추출
    print("\n📖 field_settings.json 분석 중...")
    settings_results = extract_field_settings()
    print(f"✅ {len(settings_results)}개 설정 추출 완료")
    
    # 2. 실제 파일과의 갭 분석
    print("\n🔍 실제 parquet 파일과의 갭 분석 중...")
    gap_analysis = analyze_field_gaps(settings_results)
    
    analyzable_count = len([g for g in gap_analysis if g['error'] is None])
    error_count = len(gap_analysis) - analyzable_count
    print(f"✅ 분석 완료: {analyzable_count}개 성공, {error_count}개 실패")
    
    # 3. 종합 보고서 작성
    print("\n📝 종합 보고서 작성 중...")
    write_comprehensive_field_report(settings_results, gap_analysis)
    
    print("\n🎉 고도화된 필드 분석 완료!")
    print("📄 보고서 파일: enhanced_field_analysis_report.txt")

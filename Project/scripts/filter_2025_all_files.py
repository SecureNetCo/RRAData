#!/usr/bin/env python3
"""
2025년 데이터 필터링 스크립트 (확장버전)
전체 39개 parquet 파일을 2025년 데이터만 남기도록 필터링
DataA Base + DataC Success + DataC Failed 모두 포함
"""

import pandas as pd
import os
from datetime import datetime
from pathlib import Path

def get_all_parquet_files():
    """39개 전체 parquet 파일 목록 생성"""
    
    base_files = [
        '1_safetykorea_flattened.parquet',
        '2_wadiz_flattened.parquet', 
        '3_efficiency_flattened.parquet',
        '4_high_efficiency_flattened.parquet',
        '5_standby_power_flattened.parquet',
        '6_approval_flattened.parquet',
        '7_declare_flattened.parquet',
        '8_kwtc_flattened.parquet',
        '9_recall_flattened.parquet',
        '10_safetykoreachild_flattened.parquet',
        '11_rra_cert_flattened.parquet',
        '12_rra_self_cert_flattened.parquet',
        '13_safetykoreahome_flattened.parquet'
    ]
    
    all_files = {
        'DataA_Base': base_files,
        'DataC_Success': [f"enhanced/success/{f.replace('.parquet', '_success.parquet')}" for f in base_files],
        'DataC_Failed': [f"enhanced/failed/{f.replace('.parquet', '_failed.parquet')}" for f in base_files]
    }
    
    return all_files

def get_filter_config_for_file(filename):
    """파일별 날짜 컬럼 설정 반환"""
    
    # 원본 파일명 추출 (success/failed 제거)
    base_filename = filename.replace('_success.parquet', '.parquet').replace('_failed.parquet', '.parquet')
    base_filename = os.path.basename(base_filename)
    
    # 기본 파일들의 날짜 컬럼 설정
    base_configs = {
        '1_safetykorea_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYYMMDD'
        },
        '2_wadiz_flattened.parquet': {
            'date_columns': [],  # wadiz는 날짜 필터링 안함
            'format': None
        },
        '3_efficiency_flattened.parquet': {
            'date_columns': ['완료일'],
            'format': 'YYYY-MM-DD'
        },
        '4_high_efficiency_flattened.parquet': {
            'date_columns': ['인증일자'],
            'format': 'YYYY-MM-DD'
        },
        '5_standby_power_flattened.parquet': {
            'date_columns': ['완료일자'],
            'format': 'YYYY-MM-DD'
        },
        '6_approval_flattened.parquet': {
            'date_columns': ['승인일자'],
            'format': 'YYYY-MM-DD'
        },
        '7_declare_flattened.parquet': {
            'date_columns': ['신고증명서 발급일'],
            'format': 'YYYY-MM-DD'
        },
        '8_kwtc_flattened.parquet': {
            'date_columns': ['frstCrtfcDe'],
            'format': 'YYYY-MM-DD'
        },
        '9_recall_flattened.parquet': {
            'date_columns': ['인증/신고일자'],
            'format': 'YYYYMMDD'
        },
        '10_safetykoreachild_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYYMMDD'
        },
        '11_rra_cert_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYY-MM-DD'
        },
        '12_rra_self_cert_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYY-MM-DD'
        },
        '13_safetykoreahome_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYYMMDD'
        }
    }
    
    return base_configs.get(base_filename, {'date_columns': [], 'format': None})

def filter_file_to_2025(file_path, group_name):
    """단일 파일을 2025년 데이터로 필터링"""
    
    filename = os.path.basename(file_path)
    config = get_filter_config_for_file(filename)
    
    try:
        print(f"🔄 처리 중: {file_path}")
        
        # 파일 존재 확인
        if not os.path.exists(file_path):
            print(f"   ❌ 파일 없음")
            return {'error': '파일 없음'}
        
        # 파일 읽기
        df = pd.read_parquet(file_path)
        original_count = len(df)
        print(f"   원본 레코드 수: {original_count:,}")
        
        # 날짜 컬럼이 없는 경우 (wadiz 등)
        if not config['date_columns']:
            print(f"   ℹ️  날짜 필터링 건너뜀 (설정 없음)")
            return {
                'original': original_count,
                'filtered': original_count,
                'percentage': 100.0,
                'skipped': True
            }
        
        # 날짜 필터링 조건 생성
        filter_condition = None
        
        for date_col in config['date_columns']:
            if date_col in df.columns:
                # 2025년 데이터 필터링
                if config['format'] == 'YYYYMMDD':
                    # YYYYMMDD 형식 (20250101 형태)
                    col_condition = df[date_col].astype(str).str.startswith('2025')
                else:
                    # YYYY-MM-DD 형식
                    col_condition = df[date_col].astype(str).str.startswith('2025')
                
                if filter_condition is None:
                    filter_condition = col_condition
                else:
                    filter_condition = filter_condition | col_condition
                    
                print(f"   📅 날짜 컬럼 '{date_col}' 사용")
        
        if filter_condition is not None:
            # 필터링 적용
            df_filtered = df[filter_condition].copy()
            filtered_count = len(df_filtered)
            
            print(f"   2025년 데이터: {filtered_count:,} ({filtered_count/original_count*100:.1f}%)")
            
            if filtered_count > 0:
                # 파일 덮어쓰기
                df_filtered.to_parquet(file_path, index=False)
                print(f"   ✅ 파일 업데이트 완료")
            else:
                print(f"   ⚠️  2025년 데이터 없음 - 원본 유지")
            
            return {
                'original': original_count,
                'filtered': filtered_count,
                'percentage': filtered_count/original_count*100 if original_count > 0 else 0
            }
        else:
            print(f"   ❌ 날짜 컬럼을 찾을 수 없음: {config['date_columns']}")
            return {'error': '날짜 컬럼 없음'}
            
    except Exception as e:
        print(f"   ❌ 오류: {e}")
        return {'error': str(e)}

def filter_all_2025_data():
    """전체 39개 파일을 2025년 데이터로 필터링"""
    
    parquet_base = Path('/Users/jws/cursorPrj/DataPagePrj/Project/parquet')
    all_files = get_all_parquet_files()
    
    results = {}
    
    for group_name, file_list in all_files.items():
        print(f"\n📂 {group_name} 그룹 처리 중...")
        print("-" * 50)
        
        for filename in file_list:
            full_path = parquet_base / filename
            results[filename] = filter_file_to_2025(str(full_path), group_name)
    
    return results

def print_comprehensive_summary(results):
    """전체 필터링 결과 요약 출력"""
    print("\n" + "="*80)
    print("📊 39개 파일 2025년 데이터 필터링 결과 요약")
    print("="*80)
    
    # 그룹별 통계
    groups = {
        'DataA_Base': [f for f in results.keys() if 'enhanced/' not in f],
        'DataC_Success': [f for f in results.keys() if 'success/' in f],
        'DataC_Failed': [f for f in results.keys() if 'failed/' in f]
    }
    
    overall_stats = {
        'total_original': 0,
        'total_filtered': 0,
        'success_count': 0,
        'error_count': 0,
        'skipped_count': 0
    }
    
    for group_name, file_list in groups.items():
        print(f"\n🔹 {group_name} ({len(file_list)}개 파일)")
        print("-" * 40)
        
        group_original = 0
        group_filtered = 0
        group_success = 0
        
        for filename in file_list:
            if filename in results:
                result = results[filename]
                
                if 'error' in result:
                    print(f"❌ {os.path.basename(filename)}: {result['error']}")
                    overall_stats['error_count'] += 1
                else:
                    original = result['original']
                    filtered = result['filtered']
                    percentage = result['percentage']
                    skipped = result.get('skipped', False)
                    
                    if skipped:
                        print(f"⏭️  {os.path.basename(filename)}: {original:,} (건너뜀)")
                        overall_stats['skipped_count'] += 1
                    else:
                        print(f"✅ {os.path.basename(filename)}: {original:,} → {filtered:,} ({percentage:.1f}%)")
                    
                    group_original += original
                    group_filtered += filtered
                    group_success += 1
                    overall_stats['success_count'] += 1
        
        if group_success > 0:
            print(f"   📊 그룹 합계: {group_original:,} → {group_filtered:,} ({group_filtered/group_original*100:.1f}%)")
            overall_stats['total_original'] += group_original
            overall_stats['total_filtered'] += group_filtered
    
    # 전체 요약
    print("\n" + "="*80)
    print("🎯 전체 요약")
    print("-" * 40)
    print(f"총 처리 파일: 39개")
    print(f"✅ 성공: {overall_stats['success_count']}개")
    print(f"❌ 오류: {overall_stats['error_count']}개")
    print(f"⏭️  건너뜀: {overall_stats['skipped_count']}개")
    
    if overall_stats['total_original'] > 0:
        print(f"\n📈 데이터 현황:")
        print(f"   전체 레코드: {overall_stats['total_original']:,} → {overall_stats['total_filtered']:,}")
        print(f"   전체 비율: {overall_stats['total_filtered']/overall_stats['total_original']*100:.1f}%")
        
        reduction = overall_stats['total_original'] - overall_stats['total_filtered']
        print(f"   감소량: {reduction:,} 레코드 ({reduction/overall_stats['total_original']*100:.1f}%)")

if __name__ == "__main__":
    print("🚀 전체 39개 파일 2025년 데이터 필터링 시작")
    print("DataA Base + DataC Success + DataC Failed 모두 포함")
    print("\n⚠️  주의: 이 작업은 원본 파일을 직접 수정합니다!")
    
    # 사용자 확인
    response = input("\n계속 진행하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("작업이 취소되었습니다.")
        exit(0)
    
    results = filter_all_2025_data()
    print_comprehensive_summary(results)
    
    print("\n✨ 2025년 데이터 필터링 완료!")

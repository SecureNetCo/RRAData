#!/usr/bin/env python3
"""
2025년 데이터 필터링 스크립트
parquet 파일들을 직접 수정하여 2025년 데이터만 남김
"""

import pandas as pd
import os
from datetime import datetime

def filter_2025_data():
    """12개 카테고리의 parquet 파일을 2025년 데이터만 남기도록 필터링"""

    parquet_dir = '/Users/jws/cursorPrj/DataPagePrj/Project/parquet'

    # 필터링 대상 파일과 날짜 컬럼 매핑
    filter_configs = {
        '1_safetykorea_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYYMMDD'
        },
        '4_high_efficiency_flattened.parquet': {
            'date_columns': ['인증일자'],
            'format': 'YYYY-MM-DD'
        },
        '5_standby_power_flattened.parquet': {
            'date_columns': ['완료일자'],
            'format': 'YYYY-MM-DD'
        },
        '8_kwtc_flattened.parquet': {
            'date_columns': ['frstCrtfcDe'],
            'format': 'YYYY-MM-DD'
        },
        '3_efficiency_flattened.parquet': {
            'date_columns': ['완료일'],
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
        '9_recall_flattened.parquet': {
            'date_columns': ['인증/신고일자'],
            'format': 'YYYYMMDD'
        },
        '10_safetykoreachild_flattened.parquet': {
            'date_columns': ['cert_date'],
            'format': 'YYYYMMDD'
        },
        '13_safetykoreahome_flattened.parquet': {
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
        }
    }

    results = {}

    for filename, config in filter_configs.items():
        file_path = os.path.join(parquet_dir, filename)

        try:
            print(f"🔄 처리 중: {filename}")

            # 파일 읽기
            df = pd.read_parquet(file_path)
            original_count = len(df)
            print(f"   원본 레코드 수: {original_count:,}")

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

                results[filename] = {
                    'original': original_count,
                    'filtered': filtered_count,
                    'percentage': filtered_count/original_count*100 if original_count > 0 else 0
                }
            else:
                print(f"   ❌ 날짜 컬럼을 찾을 수 없음: {config['date_columns']}")
                results[filename] = {'error': '날짜 컬럼 없음'}

        except Exception as e:
            print(f"   ❌ 오류: {e}")
            results[filename] = {'error': str(e)}

    return results

def print_summary(results):
    """필터링 결과 요약 출력"""
    print("\n" + "="*60)
    print("📊 2025년 데이터 필터링 결과 요약")
    print("="*60)

    total_original = 0
    total_filtered = 0
    success_count = 0

    for filename, result in results.items():
        if 'error' in result:
            print(f"❌ {filename}: {result['error']}")
        else:
            original = result['original']
            filtered = result['filtered']
            percentage = result['percentage']

            print(f"✅ {filename}")
            print(f"   {original:,} → {filtered:,} ({percentage:.1f}%)")

            total_original += original
            total_filtered += filtered
            success_count += 1

    print("\n" + "-"*60)
    print(f"총 처리 파일: {success_count}개")
    print(f"전체 레코드: {total_original:,} → {total_filtered:,}")
    if total_original > 0:
        print(f"전체 비율: {total_filtered/total_original*100:.1f}%")

if __name__ == "__main__":
    print("🚀 2025년 데이터 필터링 시작")
    results = filter_2025_data()
    print_summary(results)
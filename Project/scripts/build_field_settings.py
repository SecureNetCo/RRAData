#!/usr/bin/env python3
"""
field_settings.json 자동 생성 스크립트
DataA + DataB + FTC 설정을 합쳐서 최종 field_settings.json 생성
DataC는 DataA/DataB + FTC 조합으로 자동 생성됨
"""

import json
import os
from pathlib import Path
from copy import deepcopy

def load_base_settings():
    """DataA, DataB, FTC 설정 로드"""
    config_dir = Path("/Users/jws/cursorPrj/DataPagePrj/Project/config")

    with open(config_dir / "dataA_settings.json", 'r', encoding='utf-8') as f:
        dataA = json.load(f)

    with open(config_dir / "dataB_settings.json", 'r', encoding='utf-8') as f:
        dataB = json.load(f)

    with open(config_dir / "ftc_settings.json", 'r', encoding='utf-8') as f:
        ftc = json.load(f)

    return dataA, dataB, ftc

def generate_datac_config(base_config, category_name, result_type, ftc_fields):
    """DataA/DataB 설정을 기반으로 DataC 설정 생성"""

    # 기본 설정 복사
    datac_config = deepcopy(base_config)

    # category_info 업데이트
    if "category_info" in datac_config:
        original_name = datac_config["category_info"].get("display_name", category_name)
        status_text = "매칭 성공" if result_type == "success" else "매칭 실패"

        datac_config["category_info"]["display_name"] = f"{original_name} ({status_text})"
        datac_config["category_info"]["description"] = f"{original_name} - {status_text}한 데이터"
        datac_config["category_info"]["icon"] = "check-circle" if result_type == "success" else "x-circle"

        # 파일 경로 업데이트
        original_file = datac_config["category_info"].get("data_file", "")
        if original_file:
            base_filename = os.path.basename(original_file)
            new_filename = base_filename.replace(".parquet", f"_{result_type}.parquet")
            datac_config["category_info"]["data_file"] = f"parquet/enhanced/{result_type}/{new_filename}"

    # Display fields에 FTC 필드 추가
    if "display_fields" in datac_config:
        datac_config["display_fields"].extend(ftc_fields["display_fields"])

    # Download fields에 FTC 필드 추가
    if "download_fields" in datac_config:
        datac_config["download_fields"].extend(ftc_fields["download_fields"])

    # Search fields에 FTC 필드 추가
    if "search_fields" in datac_config:
        datac_config["search_fields"].extend(ftc_fields["search_fields"])

    # Field types에 FTC 타입 추가
    if "field_types" in datac_config:
        datac_config["field_types"].update(ftc_fields["field_types"])
    else:
        datac_config["field_types"] = ftc_fields["field_types"]

    # UI Settings 업데이트
    if "ui_settings" in datac_config:
        # FTC 관련 필드를 기본 정렬로 설정
        datac_config["ui_settings"]["default_sort_field"] = "ftc_registration_date"
        datac_config["ui_settings"]["default_sort_order"] = "desc"

    return datac_config

def build_field_settings():
    """최종 field_settings.json 생성"""

    print("🔧 field_settings.json 자동 생성 시작")

    # 기본 설정 로드
    dataA, dataB, ftc_fields = load_base_settings()

    # 최종 설정 구조 생성
    field_settings = {
        "dataA": dataA,
        "dataB": dataB,
        "dataC": {
            "success": {},
            "failed": {}
        }
    }

    print("\n📂 DataA → DataC 변환...")
    # DataA 기반으로 DataC 생성
    for subcategory, config in dataA.items():
        print(f"  ✅ {subcategory} → Success/Failed")

        # Success 버전 생성
        success_config = generate_datac_config(config, subcategory, "success", ftc_fields)
        field_settings["dataC"]["success"][subcategory] = success_config

        # Failed 버전 생성
        failed_config = generate_datac_config(config, subcategory, "failed", ftc_fields)
        field_settings["dataC"]["failed"][subcategory] = failed_config

    print("\n📂 DataB → DataC 변환...")
    # DataB 기반으로 DataC 생성
    for subcategory, config in dataB.items():
        print(f"  ✅ {subcategory} → Success/Failed")

        # Success 버전 생성
        success_config = generate_datac_config(config, subcategory, "success", ftc_fields)
        field_settings["dataC"]["success"][subcategory] = success_config

        # Failed 버전 생성
        failed_config = generate_datac_config(config, subcategory, "failed", ftc_fields)
        field_settings["dataC"]["failed"][subcategory] = failed_config

    return field_settings

def save_field_settings(settings):
    """최종 field_settings.json 저장"""
    output_path = "/Users/jws/cursorPrj/DataPagePrj/Project/config/field_settings.json"

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)

    print(f"\n💾 field_settings.json 생성 완료: {output_path}")

def show_statistics(settings):
    """생성된 설정 통계 표시"""
    print("\n📊 생성된 설정 통계:")

    dataA_count = len(settings.get("dataA", {}))
    dataB_count = len(settings.get("dataB", {}))
    dataC_success_count = len(settings.get("dataC", {}).get("success", {}))
    dataC_failed_count = len(settings.get("dataC", {}).get("failed", {}))

    print(f"  📁 DataA: {dataA_count}개 (원본)")
    print(f"  📁 DataB: {dataB_count}개 (원본)")
    print(f"  📁 DataC Success: {dataC_success_count}개 (자동생성)")
    print(f"  📁 DataC Failed: {dataC_failed_count}개 (자동생성)")
    print(f"  📁 총 카테고리: {dataA_count + dataB_count + dataC_success_count + dataC_failed_count}개")

    # FTC 필드 추가 통계
    _, _, ftc_fields = load_base_settings()
    ftc_fields_count = len(ftc_fields["download_fields"])
    total_enhanced = dataC_success_count + dataC_failed_count
    print(f"  🏷️ FTC 필드: {ftc_fields_count}개")
    print(f"  🚀 FTC 적용 카테고리: {total_enhanced}개")

if __name__ == "__main__":
    print("🏗️ Field Settings 자동 빌드 시스템")
    print("DataA + DataB + FTC → field_settings.json")
    print("=" * 60)

    try:
        # 1. 설정 빌드
        settings = build_field_settings()

        # 2. 통계 표시
        show_statistics(settings)

        # 3. 파일 저장
        save_field_settings(settings)

        print("\n✅ field_settings.json 자동 생성 완료!")
        print("💡 이제 DataA/DataB/FTC 파일만 수정하면 자동으로 반영됩니다.")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
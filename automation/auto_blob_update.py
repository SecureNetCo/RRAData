#!/usr/bin/env python3
"""
Vercel Blob 자동 업로드 및 환경변수 설정 스크립트
39개 DuckDB 파일 지원: DataA(13개) + DataC Success(13개) + DataC Failed(13개)
데이터 갱신시 한 번만 실행하면 모든 과정 자동화
Mac/Windows 크로스 플랫폼 호환
"""

import subprocess
import json
import os
import sys
import re
from pathlib import Path

def run_command(cmd, check=True):
    """명령어 실행 및 결과 반환 (크로스 플랫폼)"""
    print(f"[실행중] {' '.join(cmd)}")
    
    # Windows에서 vercel 명령어 처리
    if sys.platform.startswith('win') and cmd[0] == 'vercel':
        cmd[0] = 'vercel.cmd'
    
    # stdout과 stderr 모두 캡처
    result = subprocess.run(cmd, capture_output=True, text=True, shell=sys.platform.startswith('win'))
    
    if check and result.returncode != 0:
        print(f"[에러] {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd)
    
    return result

def upload_and_set_env_vars():
    """DuckDB 파일들을 업로드하고 환경변수 자동 설정"""
    
    # 파일명 → 환경변수명 매핑 (39개 전체)
    files_mapping = {
        # DataA (13개) - duckdb/ 경로
        "duckdb/1_safetykorea_flattened.duckdb": "BLOB_URL_DATAA_1_SAFETYKOREA",
        "duckdb/2_wadiz_flattened.duckdb": "BLOB_URL_DATAB_2_WADIZ",
        "duckdb/3_efficiency_flattened.duckdb": "BLOB_URL_DATAA_3_EFFICIENCY",
        "duckdb/4_high_efficiency_flattened.duckdb": "BLOB_URL_DATAA_4_HIGH_EFFICIENCY",
        "duckdb/5_standby_power_flattened.duckdb": "BLOB_URL_DATAA_5_STANDBY_POWER",
        "duckdb/6_approval_flattened.duckdb": "BLOB_URL_DATAA_6_APPROVAL",
        "duckdb/7_declare_flattened.duckdb": "BLOB_URL_DATAA_7_DECLARE",
        "duckdb/8_kwtc_flattened.duckdb": "BLOB_URL_DATAA_8_KWTC",
        "duckdb/9_recall_flattened.duckdb": "BLOB_URL_DATAA_9_RECALL",
        "duckdb/10_safetykoreachild_flattened.duckdb": "BLOB_URL_DATAA_10_SAFETYKOREACHILD",
        "duckdb/11_rra_cert_flattened.duckdb": "BLOB_URL_DATAA_11_RRA_CERT",
        "duckdb/12_rra_self_cert_flattened.duckdb": "BLOB_URL_DATAA_12_RRA_SELF_CERT",
        "duckdb/13_safetykoreahome_flattened.duckdb": "BLOB_URL_DATAA_13_SAFETYKOREAHOME",

        # DataC Success (13개) - duckdb/enhanced/success/ 경로
        "duckdb/enhanced/success/1_safetykorea_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_1_SAFETYKOREA",
        "duckdb/enhanced/success/2_wadiz_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_2_WADIZ",
        "duckdb/enhanced/success/3_efficiency_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_3_EFFICIENCY",
        "duckdb/enhanced/success/4_high_efficiency_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_4_HIGH_EFFICIENCY",
        "duckdb/enhanced/success/5_standby_power_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_5_STANDBY_POWER",
        "duckdb/enhanced/success/6_approval_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_6_APPROVAL",
        "duckdb/enhanced/success/7_declare_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_7_DECLARE",
        "duckdb/enhanced/success/8_kwtc_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_8_KWTC",
        "duckdb/enhanced/success/9_recall_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_9_RECALL",
        "duckdb/enhanced/success/10_safetykoreachild_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_10_SAFETYKOREACHILD",
        "duckdb/enhanced/success/11_rra_cert_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_11_RRA_CERT",
        "duckdb/enhanced/success/12_rra_self_cert_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_12_RRA_SELF_CERT",
        "duckdb/enhanced/success/13_safetykoreahome_flattened_success.duckdb": "BLOB_URL_DATAC_SUCCESS_13_SAFETYKOREAHOME",

        # DataC Failed (13개) - duckdb/enhanced/failed/ 경로
        "duckdb/enhanced/failed/1_safetykorea_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_1_SAFETYKOREA",
        "duckdb/enhanced/failed/2_wadiz_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_2_WADIZ",
        "duckdb/enhanced/failed/3_efficiency_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_3_EFFICIENCY",
        "duckdb/enhanced/failed/4_high_efficiency_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_4_HIGH_EFFICIENCY",
        "duckdb/enhanced/failed/5_standby_power_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_5_STANDBY_POWER",
        "duckdb/enhanced/failed/6_approval_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_6_APPROVAL",
        "duckdb/enhanced/failed/7_declare_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_7_DECLARE",
        "duckdb/enhanced/failed/8_kwtc_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_8_KWTC",
        "duckdb/enhanced/failed/9_recall_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_9_RECALL",
        "duckdb/enhanced/failed/10_safetykoreachild_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_10_SAFETYKOREACHILD",
        "duckdb/enhanced/failed/11_rra_cert_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_11_RRA_CERT",
        "duckdb/enhanced/failed/12_rra_self_cert_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_12_RRA_SELF_CERT",
        "duckdb/enhanced/failed/13_safetykoreahome_flattened_failed.duckdb": "BLOB_URL_DATAC_FAILED_13_SAFETYKOREAHOME"
    }
    
    # 프로젝트 루트 경로 설정
    project_root = Path(__file__).parent.parent / "Project"
    
    print(f"[프로젝트 루트] {project_root.absolute()}")
    print(f"[처리할 파일 수] {len(files_mapping)}개")
    print("=" * 50)
    
    success_count = 0
    
    for relative_path, env_var in files_mapping.items():
        file_path = project_root / relative_path

        if not file_path.exists():
            print(f"[경고] 파일 없음: {relative_path}")
            continue

        try:
            print(f"\n[업로드 중] {relative_path}")

            file_extension = file_path.suffix
            if not file_extension:
                print(f"[에러] 파일 확장자를 확인할 수 없습니다: {relative_path}")
                continue

            # 1. Vercel Blob에 업로드 (경로를 문자열로 변환, 덮어쓰기 허용)
            upload_result = run_command([
                "vercel", "blob", "put", str(file_path.resolve()), "--force"
            ])

            # 정규 표현식으로 순수한 URL만 완벽하게 추출
            full_output = upload_result.stdout + "\n" + upload_result.stderr
            url_pattern = rf'(https://[^\s]+{re.escape(file_extension)})'
            url_match = re.search(url_pattern, full_output)

            if not url_match:
                print(f"[에러] 업로드 결과에서 URL을 찾을 수 없습니다.")
                print(f"[전체 출력] stdout: {upload_result.stdout}")
                print(f"[전체 출력] stderr: {upload_result.stderr}")
                continue

            blob_url = url_match.group(1).strip()

            # URL 검증
            if not blob_url.startswith('https://') or not blob_url.endswith(file_extension):
                print(f"[에러] 잘못된 URL 형식: {repr(blob_url)}")
                continue
                
            print(f"[성공] 업로드 완료: {blob_url}")
            print(f"[디버그] URL repr: {repr(blob_url)}")
            
            # 2. 기존 환경변수 삭제 (있다면)
            try:
                run_command([
                    "vercel", "env", "rm", env_var, "production", "--yes"
                ], check=False)  # 에러 무시
            except:
                pass  # 기존 변수 없으면 무시
            
            # 3. 새 환경변수 추가 (non-interactive)
            env_process = subprocess.Popen([
                "vercel", "env", "add", env_var, "production"
            ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # 환경변수 값을 stdin으로 전달 (완벽하게 정리된 URL 사용)
            print(f"[디버그] 환경변수에 설정할 URL: {repr(blob_url)}")
            print(f"[디버그] URL 길이: {len(blob_url)}")
            # Vercel CLI는 Enter 키 입력을 기대하므로 \n 추가 (이는 값에 포함되지 않음)
            stdout, stderr = env_process.communicate(input=blob_url)
            
            if env_process.returncode != 0:
                print(f"[에러] 환경변수 설정 실패: {stderr}")
                continue
            
            print(f"[성공] 환경변수 설정: {env_var}")
            success_count += 1
            
        except subprocess.CalledProcessError as e:
            print(f"[실패] {relative_path} - {e}")
            continue
        except Exception as e:
            print(f"[오류] {relative_path} - {e}")
            continue
    
    print("=" * 50)
    print(f"[완료] {success_count}/{len(files_mapping)}개 파일 처리")
    
    if success_count == len(files_mapping):
        print("[성공] 모든 파일이 성공적으로 업로드되고 환경변수가 설정되었습니다!")
        print("[알림] 이제 Vercel에서 자동으로 새로운 데이터를 사용합니다.")
    else:
        print("[경고] 일부 파일에서 오류가 발생했습니다. 로그를 확인해주세요.")

if __name__ == "__main__":
    print("DataPage 데이터 자동 업로드 및 환경변수 설정")
    print("크로스 플랫폼 호환 (Mac/Windows)")
    print("=" * 50)
    
    # Vercel CLI 로그인 상태 확인
    try:
        result = run_command(["vercel", "whoami"], check=False)
        if result.returncode != 0:
            print("[오류] Vercel CLI에 로그인되지 않았습니다.")
            print("[해결] 'vercel login' 명령어로 먼저 로그인해주세요.")
            sys.exit(1)
        else:
            print(f"[로그인] {result.stdout.strip()}")
    except Exception as e:
        print(f"[오류] Vercel CLI 확인 실패: {e}")
        sys.exit(1)
    
    # 프로젝트 링크 확인 및 자동 설정
    project_root = Path(__file__).parent.parent
    vercel_json_path = project_root / "vercel.json"
    
    if vercel_json_path.exists():
        print(f"[프로젝트] vercel.json 발견: {project_root}")
        # 프로젝트 루트로 이동해서 실행
        os.chdir(project_root)
        
        # 링크 상태 확인
        link_result = run_command(["vercel", "project", "ls"], check=False)
        if "No projects found" in link_result.stdout:
            print("[설정] Vercel 프로젝트에 링크가 필요합니다.")
            print("[안내] 잠시 후 브라우저에서 프로젝트를 선택해주세요.")
            try:
                run_command(["vercel", "link"])
                print("[성공] 프로젝트 링크 완료")
            except:
                print("[오류] 프로젝트 링크 실패")
                sys.exit(1)
    else:
        print("[경고] vercel.json을 찾을 수 없습니다.")
        print("[해결] 프로젝트 루트에서 실행하거나 수동으로 'vercel link'를 실행해주세요.")
        sys.exit(1)
    
    upload_and_set_env_vars()

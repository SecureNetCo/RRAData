"""
DuckDB 기반 대용량 JSON 파일 처리 모듈
다양한 JSON 구조에 대응하는 고성능 검색 엔진

성능 목표:
- 기존 ijson 113초 → DuckDB 10-15초 (8-11배 향상)  
- 메모리 효율적인 스트리밍 처리
- 다중 코어 병렬 처리 활용
- JSON 배열/객체 구조 자동 감지
"""

import duckdb
import asyncio
import time
import os
import hashlib
import urllib.request
import shutil
from typing import Dict, List, Any, Optional, Callable
from pathlib import Path
import json
import logging
from functools import lru_cache
from urllib.parse import urlparse
from threading import Lock

logger = logging.getLogger(__name__)

# DuckDB httpfs 설치 여부 캐시
HTTPFS_INSTALLED = False

# 스키마 캐시 (URL 및 파일명 기준) - Blob/R2 도메인 변경 대응
SCHEMA_CACHE_BY_URL: Dict[str, List[str]] = {}
SCHEMA_CACHE_BY_FILENAME: Dict[str, List[str]] = {}

# DuckDB 연결 캐시 (파일 경로/URL 기준)
CONNECTION_CACHE: Dict[str, duckdb.DuckDBPyConnection] = {}
CONNECTION_LOCKS: Dict[str, Lock] = {}
CONNECTION_CACHE_LOCK = Lock()

# 원격 DuckDB 파일 로컬 캐시
DUCKDB_REMOTE_CACHE: Dict[str, Path] = {}
DUCKDB_REMOTE_CACHE_LOCK = Lock()
DUCKDB_CACHE_ROOT = Path("/tmp/datapage_duckdb_cache")
DUCKDB_CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def _get_search_pattern_and_operator(keyword: str, field: str) -> tuple[str, str]:
    """
    필드별 검색 패턴과 연산자 생성 함수
    인증번호/신고번호 필드에 대해 정확 매칭 적용

    Returns:
        tuple: (search_pattern, operator)
        - operator: 'LIKE' 또는 '='
        - search_pattern: 검색 패턴 (LIKE용 '%keyword%' 또는 정확매칭용 'keyword')
    """
    # 인증번호/신고번호 필드는 정확 매칭
    if field in ['cert_no', 'cert_num', 'declare_no', '신고번호', '승인번호']:
        return keyword, '='  # 정확 매칭

    # 기본: 부분 매칭 (LIKE '%keyword%')
    return f"%{keyword}%", 'LIKE'


def _get_search_pattern(keyword: str, field: str) -> str:
    """하위 호환성을 위한 래퍼 함수"""
    pattern, _ = _get_search_pattern_and_operator(keyword, field)
    return pattern


def _extract_file_name(path_like: Any) -> Optional[str]:
    """URL/경로 문자열에서 파일명만 안전하게 추출"""
    if not path_like:
        return None

    try:
        path_str = str(path_like)
        if path_str.startswith("http://") or path_str.startswith("https://"):
            parsed = urlparse(path_str)
            return Path(parsed.path).name if parsed.path else None
        return Path(path_str).name
    except Exception:
        return None

def _normalize_memory_setting(value: str, default: str) -> str:
    """Return DuckDB-friendly memory setting (accept plain numbers as MB)."""
    if not value:
        return default

    value = value.strip()
    if not value:
        return default

    lowered = value.lower()
    if lowered.endswith(("mb", "gb")):
        return value

    if lowered.isdigit():
        return f"{value}MB"

    return default


def _configure_connection(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """DuckDB 연결에 공통 설정 적용"""

    # **🚀 성능 최적화: Vercel 서버리스 환경 맞춤 DuckDB 설정**
    try:
        memory_limit = _normalize_memory_setting(
            os.getenv("DUCKDB_MEMORY_LIMIT"),
            "512MB"
        )
        max_memory = _normalize_memory_setting(
            os.getenv("DUCKDB_MAX_MEMORY"),
            "640MB"
        )

        # 메모리 관리 최적화 (Vercel 1GB 제한 고려)
        conn.execute(f"SET memory_limit = '{memory_limit}'")
        conn.execute(f"SET max_memory = '{max_memory}'")
        conn.execute("SET temp_directory = '/tmp'")          # 임시 파일 경로 지정

        # 처리 성능 최적화
        conn.execute("SET threads = 2")                      # 서버리스에서 병렬 처리 활성화
        conn.execute("SET enable_progress_bar = false")      # 진행률 표시 비활성화로 오버헤드 제거
        conn.execute("SET enable_object_cache = true")       # 객체 캐싱 활성화
        conn.execute("SET preserve_insertion_order = false") # 정렬 성능 향상

        logger.info("⚡ DuckDB 고급 최적화 설정 완료 - Vercel 특화")
    except Exception as e:
        logger.warning(f"DuckDB 최적화 설정 일부 실패: {e}")
        # 기본 설정이라도 적용
        try:
            conn.execute("SET memory_limit = '256MB'")
            conn.execute("SET max_memory = '320MB'")
            conn.execute("SET threads = 2")
            logger.info("💡 DuckDB 기본 최적화 설정 적용")
        except:
            logger.warning("DuckDB 기본 설정 실패 - 기본값으로 진행")

    # Vercel 서버리스 환경을 위한 안전한 httpfs 설정
    try:
        # 1단계: home_directory 설정 (Vercel 환경 대응)
        conn.execute("SET home_directory='/tmp'")
        logger.info("home_directory 설정 완료")

        # 2단계: httpfs 설치 및 로드 (INSTALL은 최초 1회만)
        global HTTPFS_INSTALLED
        if not HTTPFS_INSTALLED:
            try:
                conn.execute("INSTALL httpfs")
                logger.info("httpfs extension 최초 설치 완료")
            except Exception as install_error:
                logger.debug(f"httpfs install 스킵: {install_error}")
            finally:
                HTTPFS_INSTALLED = True

        conn.execute("LOAD httpfs")
        logger.info("httpfs extension 로드 완료")

    except Exception as e:
        # httpfs 실패 시 완전 무시하고 계속 진행
        logger.info(f"httpfs 설정 스킵 (서버리스 환경): {str(e)[:100]}...")

    return conn


def _create_optimized_connection() -> duckdb.DuckDBPyConnection:
    """최적화된 DuckDB 연결 생성"""
    conn = duckdb.connect()
    return _configure_connection(conn)


def _get_or_create_connection(connection_key: str) -> tuple[duckdb.DuckDBPyConnection, Lock]:
    """파일별 DuckDB 연결을 생성 또는 재사용"""
    with CONNECTION_CACHE_LOCK:
        conn = CONNECTION_CACHE.get(connection_key)
        if conn is None:
            conn = _create_optimized_connection()
            CONNECTION_CACHE[connection_key] = conn
            CONNECTION_LOCKS[connection_key] = Lock()
            logger.info(f"DuckDB 연결 캐시 생성: {connection_key}")

        conn_lock = CONNECTION_LOCKS[connection_key]

    return conn, conn_lock

    return conn

@lru_cache(maxsize=1)
def load_case_sensitivity_config():
    """대소문자 구분 설정 로드"""
    try:
        config_path = Path(__file__).parent.parent / "config" / "case_sensitivity_config.json"
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            logger.info("대소문자 구분 설정 로드 완료")
            return config
    except Exception as e:
        logger.warning(f"대소문자 구분 설정 로드 실패: {e}, 기본값 사용")
        return {
            "case_insensitive_fields": {"업체명": True, "제품명": True, "company_name": True, "product_name": True},
            "case_sensitive_fields": {"인증번호": False, "모델명": False, "certification_no": False, "model_name": False},
            "default_case_insensitive": False
        }

@lru_cache(maxsize=1)
def load_field_settings():
    """field_settings.json 로드"""
    try:
        config_path = Path(__file__).parent.parent / "config" / "field_settings.json"
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            logger.info("field_settings.json 로드 완료")
            return config
    except Exception as e:
        logger.warning(f"field_settings.json 로드 실패: {e}")
        return {}

class DuckDBProcessor:
    """DuckDB 기반 SafetyKorea 데이터 처리 클래스"""

    def __init__(
        self,
        file_path: str,
        category: str = None,
        subcategory: str = None,
        result_type: str = None,
        required_fields: Optional[List[str]] = None,
    ):
        """
        Args:
            file_path: 처리할 JSON/Parquet 파일 경로 (로컬 파일 또는 URL)
            category: 데이터 카테고리 (dataA, dataC 등) - SELECT 최적화용
            subcategory: 데이터 서브카테고리 (safetykoreachild 등) - SELECT 최적화용
        """
        self.file_path_str = file_path  # 원본 경로 문자열 저장
        self.category = category        # SELECT 최적화용 카테고리
        self.subcategory = subcategory  # SELECT 최적화용 서브카테고리
        self.json_structure = None      # 'array', 'object', 'nested_object'
        self.case_config = load_case_sensitivity_config()  # 대소문자 구분 설정 로드
        self.field_settings = load_field_settings()        # field_settings.json 로드
        self.result_type = result_type  # dataC success/failed 구분용
        self.connection_key = file_path  # 기본적으로 문자열 형태 (URL 포함)
        self.required_fields: List[str] = required_fields or []
        self.dynamic_required_fields: List[str] = []
        self.is_duckdb_storage = False
        self.duckdb_table_name: Optional[str] = None
        self._duckdb_alias: Optional[str] = None
        self._duckdb_view_name: Optional[str] = None
        self._duckdb_attached = False
        self._local_duckdb_path: Optional[str] = None
        
        # 파일 경로가 URL인지 로컬 경로인지 확인
        self.is_url = self.file_path_str.startswith('https://') or self.file_path_str.startswith('http://')
        
        if not self.is_url:
            # 로컬 파일일 경우에만 Path 객체로 변환하고 존재 여부 확인
            self.file_path = Path(file_path)
            if not self.file_path.exists():
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
            self.connection_key = str(self.file_path.resolve())
        else:
            # URL은 문자열로 유지 (DuckDB가 URL을 직접 처리 가능)
            self.file_path = self.file_path_str
            
        logger.info(f"DuckDBProcessor 초기화: {self.file_path} (URL: {self.is_url})")

        # DuckDB 파일일 경우 메타데이터 설정
        suffix_target: Optional[Path] = None
        if self.is_url:
            try:
                parsed = urlparse(self.file_path_str)
                if parsed.path:
                    suffix_target = Path(parsed.path)
            except Exception:
                suffix_target = None
        else:
            suffix_target = self.file_path

        if suffix_target is not None and suffix_target.suffix.lower() == '.duckdb':
            self.is_duckdb_storage = True
            self.duckdb_table_name = suffix_target.stem
            digest_source = str(self.connection_key)
            digest = hashlib.md5(digest_source.encode('utf-8', errors='ignore')).hexdigest()
            identifier = digest[:12]
            self._duckdb_alias = f"db_{identifier}"
            self._duckdb_view_name = f"vw_{identifier}"
            logger.info(
                "DuckDB 스토리지 감지: table=%s, alias=%s", self.duckdb_table_name, self._duckdb_alias
            )

        # 🚀 성능 최적화: R2 파일 스키마 캐시 (첫 번째 네트워크 통신 제거)
        global SCHEMA_CACHE_BY_URL, SCHEMA_CACHE_BY_FILENAME


    def _get_connection(self) -> tuple[duckdb.DuckDBPyConnection, Lock]:
        return _get_or_create_connection(self.connection_key)

    @staticmethod
    def _escape_path(path: str) -> str:
        return path.replace("'", "''")

    def _ensure_local_duckdb_copy(self, source_url: str) -> Path:
        with DUCKDB_REMOTE_CACHE_LOCK:
            cached = DUCKDB_REMOTE_CACHE.get(source_url)
            if cached and cached.exists():
                return cached

            parsed = urlparse(source_url)
            filename = Path(parsed.path).name or f"duckdb_{hashlib.md5(source_url.encode('utf-8', errors='ignore')).hexdigest()}.duckdb"
            cache_path = DUCKDB_CACHE_ROOT / filename

            temp_path = cache_path.with_suffix('.download')
            try:
                logger.info(f"DuckDB 원격 파일 다운로드: {source_url} → {cache_path}")
                with urllib.request.urlopen(source_url) as response, open(temp_path, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                os.replace(temp_path, cache_path)
            except Exception as download_error:
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass
                raise RuntimeError(f"DuckDB 파일 다운로드 실패: {source_url} ({download_error})") from download_error

            DUCKDB_REMOTE_CACHE[source_url] = cache_path
            return cache_path

    def _ensure_duckdb_view(self, conn: duckdb.DuckDBPyConnection, path: str) -> str:
        """DuckDB 파일을 현재 연결에서 뷰로 노출시키고 뷰 이름을 반환"""
        if not self.is_duckdb_storage and not path.lower().endswith('.duckdb'):
            raise ValueError("DuckDB 뷰 준비는 DuckDB 파일에서만 호출 가능합니다")

        if not self._duckdb_alias or not self._duckdb_view_name:
            digest = hashlib.md5(path.encode('utf-8', errors='ignore')).hexdigest()
            identifier = digest[:12]
            if not self._duckdb_alias:
                self._duckdb_alias = f"db_{identifier}"
            if not self._duckdb_view_name:
                self._duckdb_view_name = f"vw_{identifier}"

        alias = self._duckdb_alias
        view_name = self._duckdb_view_name
        escaped_path = self._escape_path(path)

        try:
            conn.execute(f"ATTACH '{escaped_path}' AS {alias} (READ_ONLY)")
        except (duckdb.CatalogException, duckdb.BinderException):
            # 이미 ATTACH 된 경우 무시
            pass

        table_name = self.duckdb_table_name
        if not table_name:
            table_result = conn.execute(f"PRAGMA show_tables FROM {alias}").fetchall()
            if table_result:
                # fetchall() 결과에서 첫 번째 행의 첫 번째 컬럼 값 추출
                table_name = table_result[0][0] if table_result[0] else None
                self.duckdb_table_name = table_name
            else:
                raise RuntimeError(f"DuckDB 파일에서 테이블을 찾을 수 없습니다: {path}")

        table_identifier = f'"{table_name}"'
        conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {alias}.{table_identifier}"
        )
        self._duckdb_attached = True
        return view_name

    def _get_table_expression(self, conn: duckdb.DuckDBPyConnection, path: str) -> str:
        """주어진 경로를 DuckDB SQL FROM 절에서 사용할 수 있는 표현식으로 변환"""
        if path.lower().endswith('.duckdb'):
            if path.startswith(('http://', 'https://')):
                local_path = self._ensure_local_duckdb_copy(path)
                if self._local_duckdb_path is None:
                    self._local_duckdb_path = str(local_path)
                path = str(local_path)
            return self._ensure_duckdb_view(conn, path)
        escaped_path = self._escape_path(path)
        return f"read_parquet('{escaped_path}')"

    def _resolve_tabular_path(self) -> Optional[str]:
        """현재 파일 경로 중 DuckDB/Parquet 형태를 우선 반환"""
        if self.is_url:
            if self.file_path_str.endswith('.duckdb'):
                try:
                    local_path = self._ensure_local_duckdb_copy(self.file_path_str)
                    self._local_duckdb_path = str(local_path)
                    return str(local_path)
                except Exception as download_error:
                    logger.error(download_error)
                    return None
            if self.file_path_str.endswith('.parquet'):
                return self.file_path_str
            return None

        target_path = None
        if hasattr(self.file_path, 'resolve'):
            abs_file_path = str(self.file_path.resolve())
        else:
            abs_file_path = str(Path(self.file_path).resolve())

        suffix = Path(abs_file_path).suffix.lower()
        if suffix == '.duckdb' and Path(abs_file_path).exists():
            target_path = abs_file_path
        else:
            parquet_path = abs_file_path.replace('.json', '.parquet')
            if Path(parquet_path).exists():
                target_path = parquet_path

        return target_path

    def _cache_schema(self, columns: List[str]) -> List[str]:
        """계산된 스키마 정보를 URL/파일명 캐시에 동시 저장 (동적 스키마 대상은 캐시 제외)"""
        if columns is None:
            return columns

        cache_key = self.file_path_str if self.is_url else str(self.file_path)
        file_name = _extract_file_name(cache_key)

        # 동적 스키마 로드 대상은 캐시에 저장하지 않음
        if self._should_use_dynamic_schema(cache_key, file_name):
            logger.debug(f"🔄 동적 스키마 대상: 캐시 저장 건너뛰기 - {file_name or cache_key}")
            return columns

        # 일반 데이터셋은 캐시에 저장
        if cache_key:
            SCHEMA_CACHE_BY_URL[cache_key] = columns

        if file_name:
            SCHEMA_CACHE_BY_FILENAME[file_name] = columns

        return columns

    def _should_use_dynamic_schema(self, cache_key: str, file_name: Optional[str]) -> bool:
        """동적 스키마 로드 대상인지 판단 (3,4,5번 데이터셋: 효율등급, 고효율, 대기전력)"""
        # 파일명 기반 판단
        if file_name:
            # 3_efficiency, 4_high_efficiency, 5_standby_power 및 success/failed 버전 포함
            dynamic_targets = [
                "3_efficiency_flattened.parquet",
                "3_efficiency_flattened_success.parquet",
                "3_efficiency_flattened_failed.parquet",
                "4_high_efficiency_flattened.parquet",
                "4_high_efficiency_flattened_success.parquet",
                "4_high_efficiency_flattened_failed.parquet",
                "5_standby_power_flattened.parquet",
                "5_standby_power_flattened_success.parquet",
                "5_standby_power_flattened_failed.parquet"
            ]
            if file_name in dynamic_targets:
                return True

        # URL 기반 판단 (R2 URLs)
        dynamic_url_patterns = [
            "/3_efficiency_flattened.parquet",
            "/3_efficiency_flattened_success.parquet",
            "/3_efficiency_flattened_failed.parquet",
            "/4_high_efficiency_flattened.parquet",
            "/4_high_efficiency_flattened_success.parquet",
            "/4_high_efficiency_flattened_failed.parquet",
            "/5_standby_power_flattened.parquet",
            "/5_standby_power_flattened_success.parquet",
            "/5_standby_power_flattened_failed.parquet"
        ]

        for pattern in dynamic_url_patterns:
            if pattern in cache_key:
                return True

        return False

    def _try_duckdb_blob_schema_mapping(self, cache_key: str, file_name: Optional[str]) -> Optional[List[str]]:
        """DuckDB BLOB URL을 위한 스키마 매핑 (2025모드 DuckDB 파일 지원)"""
        if not cache_key or not file_name:
            return None

        # DuckDB BLOB URL 감지
        is_duckdb_blob = (cache_key.endswith('.duckdb') and
                         ('blob.vercel-storage.com' in cache_key or 'blob' in cache_key.lower()))

        if not is_duckdb_blob:
            return None

        logger.info(f"🔥 DuckDB BLOB URL 감지, 스키마 매핑 시도: {file_name}")

        # 파일명에서 데이터셋 패턴 추출하여 R2 스키마와 매핑
        dataset_patterns = {
            'safetykorea': ['safetykorea', 'safety_korea'],
            'wadiz': ['wadiz', 'makers'],
            'efficiency': ['efficiency', 'energy'],
            'high_efficiency': ['high_efficiency', 'high-efficiency'],
            'standby_power': ['standby_power', 'standby-power'],
            'approval': ['approval', 'approve'],
            'declare': ['declare', 'declaration'],
            'kwtc': ['kwtc'],
            'recall': ['recall'],
            'safetykoreachild': ['safetykoreachild', 'safety_korea_child', 'child'],
            'rra_cert': ['rra_cert', 'rra-cert'],
            'rra_self_cert': ['rra_self_cert', 'rra-self-cert'],
            'safetykoreahome': ['safetykoreahome', 'safety_korea_home', 'home']
        }

        # 파일명을 소문자로 변환하여 패턴 매칭
        file_name_lower = file_name.lower()

        matched_dataset = None
        for dataset, patterns in dataset_patterns.items():
            for pattern in patterns:
                if pattern in file_name_lower:
                    matched_dataset = dataset
                    break
            if matched_dataset:
                break

        if not matched_dataset:
            logger.warning(f"DuckDB BLOB URL 데이터셋 패턴 매핑 실패: {file_name}")
            return None

        # DataC (enhanced) 여부 확인
        is_enhanced = 'enhanced' in file_name_lower or 'success' in file_name_lower or 'failed' in file_name_lower
        is_success = 'success' in file_name_lower

        # 매핑된 R2 스키마 찾기
        target_r2_patterns = []

        if is_enhanced:
            # DataC enhanced 스키마 우선
            if is_success:
                target_r2_patterns.append(f"{matched_dataset}_flattened_success.parquet")
            else:
                target_r2_patterns.append(f"{matched_dataset}_flattened_failed.parquet")

        # 기본 DataA 스키마
        if matched_dataset == 'wadiz':
            target_r2_patterns.append('2_wadiz_flattened.parquet')
        elif matched_dataset == 'safetykorea':
            target_r2_patterns.append('1_safetykorea_flattened.parquet')
        elif matched_dataset == 'efficiency':
            target_r2_patterns.append('3_efficiency_flattened.parquet')
        elif matched_dataset == 'high_efficiency':
            target_r2_patterns.append('4_high_efficiency_flattened.parquet')
        elif matched_dataset == 'standby_power':
            target_r2_patterns.append('5_standby_power_flattened.parquet')
        elif matched_dataset == 'approval':
            target_r2_patterns.append('6_approval_flattened.parquet')
        elif matched_dataset == 'declare':
            target_r2_patterns.append('7_declare_flattened.parquet')
        elif matched_dataset == 'kwtc':
            target_r2_patterns.append('8_kwtc_flattened.parquet')
        elif matched_dataset == 'recall':
            target_r2_patterns.append('9_recall_flattened.parquet')
        elif matched_dataset == 'safetykoreachild':
            target_r2_patterns.append('10_safetykoreachild_flattened.parquet')
        elif matched_dataset == 'rra_cert':
            target_r2_patterns.append('11_rra_cert_flattened.parquet')
        elif matched_dataset == 'rra_self_cert':
            target_r2_patterns.append('12_rra_self_cert_flattened.parquet')
        elif matched_dataset == 'safetykoreahome':
            target_r2_patterns.append('13_safetykoreahome_flattened.parquet')

        # SCHEMA_CACHE_BY_FILENAME에서 스키마 찾기
        for target_pattern in target_r2_patterns:
            if target_pattern in SCHEMA_CACHE_BY_FILENAME:
                schema = SCHEMA_CACHE_BY_FILENAME[target_pattern]
                logger.info(f"✅ DuckDB BLOB 스키마 매핑 성공: {file_name} → {target_pattern} ({len(schema)}개 컬럼)")

                # DuckDB BLOB URL 캐시에 저장
                SCHEMA_CACHE_BY_URL[cache_key] = schema
                if file_name not in SCHEMA_CACHE_BY_FILENAME:
                    SCHEMA_CACHE_BY_FILENAME[file_name] = schema

                return schema

        logger.warning(f"DuckDB BLOB URL 매핑 대상 스키마를 찾을 수 없음: {matched_dataset} → {target_r2_patterns}")
        return None


    def _detect_json_structure(self) -> str:
        """JSON 파일의 구조를 감지합니다"""
        if self.json_structure:
            return self.json_structure

        # 파켓 파일인 경우 JSON 구조 감지 건너뛰기 (UTF-8 오류 방지)
        if str(self.file_path).lower().endswith('.parquet'):
            self.json_structure = 'parquet'
            logger.info(f"파켓 파일 감지, JSON 구조 감지 건너뛰기: {self.file_path}")
            return self.json_structure

        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                # 파일의 첫 부분을 읽어서 구조 감지
                content = f.read(1000).strip()
                
            if content.startswith('['):
                self.json_structure = 'array'
                logger.info(f"JSON 구조 감지: 배열 형태")
            elif content.startswith('{'):
                # 객체 내에 배열이 있는지 확인
                if 'LED램프_details' in content:
                    self.json_structure = 'nested_safetykorea'
                elif '"data"' in content and '[' in content:
                    self.json_structure = 'nested_data'
                else:
                    self.json_structure = 'object'
                logger.info(f"JSON 구조 감지: 객체 형태 ({self.json_structure})")
            else:
                self.json_structure = 'unknown'
                logger.warning(f"알 수 없는 JSON 구조")
                
        except Exception as e:
            logger.error(f"JSON 구조 감지 실패: {e}")
            self.json_structure = 'unknown'
            
        return self.json_structure

    def _get_search_fields_from_config(self) -> list:
        """field_settings.json에서 search_fields 추출"""
        try:
            if not self.category or not self.subcategory:
                logger.info("카테고리 정보 없음 - 전체 컬럼 사용")
                return []

            # dataC의 경우 result_type(success/failed 등) 기반으로 설정 구분
            if self.category == "dataC":
                data_c_config = self.field_settings.get("dataC", {})
                result_bucket = data_c_config.get(self.result_type or "success", {})
                config_path = result_bucket.get(self.subcategory, {})
            else:
                config_path = self.field_settings.get(self.category, {}).get(self.subcategory, {})

            search_fields = config_path.get("search_fields", [])
            field_names = [field.get("field") for field in search_fields if field.get("field")]

            logger.info(f"🔍 검색 필드 로드: {len(field_names)}개 - {field_names[:3]}...")
            return field_names
        except Exception as e:
            logger.warning(f"search_fields 로드 실패: {e}")
            return []

    def _get_display_fields_from_config(self) -> list:
        """field_settings.json에서 display_fields 추출"""
        try:
            if not self.category or not self.subcategory:
                return []

            # dataC의 경우 result_type(success/failed 등) 기반으로 설정 구분
            if self.category == "dataC":
                data_c_config = self.field_settings.get("dataC", {})
                result_bucket = data_c_config.get(self.result_type or "success", {})
                config_path = result_bucket.get(self.subcategory, {})
            else:
                config_path = self.field_settings.get(self.category, {}).get(self.subcategory, {})

            display_fields = config_path.get("display_fields", [])
            field_names = [field.get("field") for field in display_fields if field.get("field")]

            logger.info(f"📊 표시 필드 로드: {len(field_names)}개")
            return field_names
        except Exception as e:
            logger.warning(f"display_fields 로드 실패: {e}")
            return []

    def _get_essential_columns(self) -> str:
        """⚡ 성능 최적화: field_settings.json 기반 동적 컬럼 선택 (실제 존재하는 컬럼만)"""
        if not self.category or not self.subcategory:
            logger.info("카테고리 정보 없음 - SELECT * 사용")
            return "*"

        try:
            # 실제 파일에서 사용 가능한 컬럼 목록 가져오기
            available_fields = self._get_available_fields()
            if not available_fields:
                logger.warning("사용 가능한 필드를 가져올 수 없음 - SELECT * 사용")
                return "*"

            # 1. search_fields에서 검색 대상 컬럼들 추출
            search_columns = self._get_search_fields_from_config()

            # 2. display_fields에서 표시 컬럼들 추출
            display_columns = self._get_display_fields_from_config()

            # 3. 동적 정렬 컬럼 감지 (실제 존재하는 날짜 컬럼 확인)
            sort_columns = []
            for date_col in ["cert_date", "crawl_date", "crawled_at", "설립일"]:
                if date_col in available_fields:
                    sort_columns.append(date_col)
                    break

            # 4. 필수 컬럼 리스트 구성 (download 등 추가 필드 포함)
            all_columns = search_columns + display_columns + sort_columns
            if self.required_fields:
                all_columns.extend(self.required_fields)

            # 5. 중복 제거 및 None 제거 (순서 유지)
            essential_cols = []
            seen = set()
            for col in all_columns:
                if col and col not in seen:
                    essential_cols.append(col)
                    seen.add(col)

            # 6. 추가로 동적으로 필요한 컬럼 포함 (검색 시 매핑된 필드 등)
            dynamic_extra = getattr(self, "dynamic_required_fields", [])
            if dynamic_extra:
                for col in dynamic_extra:
                    if col and col not in all_columns:
                        essential_cols.append(col)

            # 7. 실제 존재하는 컬럼만 필터링
            existing_cols = [col for col in essential_cols if col in available_fields]

            if not existing_cols:
                logger.warning("필수 컬럼 중 실제 존재하는 컬럼이 없음 - SELECT * 사용")
                return "*"

            # 누락된 컬럼이 있으면 로그로 알림
            missing_cols = [col for col in essential_cols if col not in available_fields]
            if missing_cols:
                logger.warning(f"⚠️ 누락된 컬럼들: {missing_cols} (파일에 존재하지 않음)")

            # 컬럼명을 쌍따옴표로 감싸서 DuckDB 안전성 확보
            quoted_cols = [f'"{col}"' for col in existing_cols]
            columns_str = ", ".join(quoted_cols)

            logger.info(f"📊 성능 최적화: {len(existing_cols)}개 필수 컬럼 선택 ({self.category}/{self.subcategory})")
            return columns_str

        except Exception as e:
            logger.error(f"필수 컬럼 추출 중 오류: {e}")
            return "*"

    def _build_base_query(self, conn: duckdb.DuckDBPyConnection, file_size_mb: float) -> str:
        """Parquet 또는 JSON 파일에 따른 기본 쿼리 생성"""
        if self.is_url:
            # URL인 경우 - 성능을 위해 Parquet 우선 사용
            abs_file_path = self.file_path_str

            # JSON URL을 Parquet URL로 변환 시도 (성능 최적화)
            if abs_file_path.endswith('.json'):
                parquet_url = abs_file_path.replace('.json', '.parquet')
                logger.info(f"⚡ 성능 최적화: JSON → Parquet 변환 시도 ({parquet_url.split('/')[-1]})")
                abs_file_path = parquet_url

            if abs_file_path.endswith(('.parquet', '.duckdb')):
                logger.info(f"🚀 Blob Tabular 파일 사용: {abs_file_path.split('/')[-1]}")
                essential_cols = self._get_essential_columns()
                table_expr = self._get_table_expression(conn, abs_file_path)
                return f"SELECT {essential_cols} FROM {table_expr}"
            else:
                logger.info(f"📄 Blob JSON 파일 사용 (Fallback): {abs_file_path.split('/')[-1]}")
                read_options = ""
        else:
            # 로컬 파일인 경우 (self.file_path가 Path 객체여야 함)
            if hasattr(self.file_path, 'resolve'):
                abs_file_path = str(self.file_path.resolve())
            else:
                # 문자열인 경우 Path 객체로 변환
                abs_file_path = str(Path(self.file_path).resolve())
            tabular_path = self._resolve_tabular_path()
            if tabular_path:
                logger.info(f"Tabular 파일 사용: {Path(tabular_path).name}")
                essential_cols = self._get_essential_columns()
                table_expr = self._get_table_expression(conn, tabular_path)
                return f"SELECT {essential_cols} FROM {table_expr}"
            
            # Parquet이 없으면 기존 JSON 방식 사용 (Fallback)
            logger.info(f"JSON 파일 사용 (Fallback): {Path(abs_file_path).name}")
            read_options = ""
        
        if file_size_mb > 10:
            read_options = ", maximum_object_size=2147483648"
            
        structure = self._detect_json_structure()
        
        if structure == 'array':
            # JSON 배열: [{"field1": "value1"}, {"field2": "value2"}]
            essential_cols = self._get_essential_columns()
            if essential_cols == "*":
                return f"SELECT * FROM read_json_auto('{abs_file_path}'{read_options})"
            else:
                return f"SELECT {essential_cols} FROM read_json_auto('{abs_file_path}'{read_options})"
            
        elif structure == 'nested_safetykorea':
            # SafetyKorea 구조: {"LED램프_details": [...]}
            return f'SELECT UNNEST("LED램프_details") as item FROM read_json_auto(\'{abs_file_path}\'{read_options})'
            
        elif structure == 'nested_data':
            # data 필드에 배열: {"data": [...]}
            return f'SELECT UNNEST("data") as item FROM read_json_auto(\'{abs_file_path}\'{read_options})'
            
        else:
            # 기본적으로 배열로 시도
            essential_cols = self._get_essential_columns()
            if essential_cols == "*":
                return f"SELECT * FROM read_json_auto('{abs_file_path}'{read_options})"
            else:
                return f"SELECT {essential_cols} FROM read_json_auto('{abs_file_path}'{read_options})"
    
    def _get_available_fields(self) -> list:
        """파일에서 실제 사용 가능한 필드명을 가져옵니다 (스키마 캐시 우선)"""

        # 🚀 성능 최적화: 캐시된 스키마가 있으면 즉시 반환 (R2 통신 제거)
        cached_fields: Optional[List[str]] = None
        cache_key = self.file_path_str if self.is_url else str(self.file_path)
        file_name = _extract_file_name(cache_key)

        # 🔥 동적 스키마 로드 대상 체크 (3,4,5번 데이터셋)
        is_dynamic_schema_target = self._should_use_dynamic_schema(cache_key, file_name)

        if is_dynamic_schema_target:
            # 동적 스키마 로드 대상은 캐시를 건너뛰고 직접 스키마 조회
            logger.info(f"🔄 동적 스키마 로드 (캐시 건너뛰기): {file_name or cache_key}")
            cached_fields = None
        else:
            # 기존 캐시 로직 사용
            if cache_key in SCHEMA_CACHE_BY_URL:
                cached_fields = SCHEMA_CACHE_BY_URL[cache_key]
                logger.info(f"⚡ 스키마 캐시 사용: {len(cached_fields)}개 컬럼 (캐시 키: {cache_key})")
            elif file_name and file_name in SCHEMA_CACHE_BY_FILENAME:
                cached_fields = SCHEMA_CACHE_BY_FILENAME[file_name]
                logger.info(f"⚡ 스키마 캐시 사용 (파일명): {file_name} → {len(cached_fields)}개 컬럼")
            else:
                # 🔥 2025모드 BLOB URL 지원: DuckDB 파일 스키마 추론
                cached_fields = self._try_duckdb_blob_schema_mapping(cache_key, file_name)
                if cached_fields:
                    logger.info(f"⚡ DuckDB BLOB URL 스키마 매핑: {file_name} → {len(cached_fields)}개 컬럼")

        if cached_fields is not None and not is_dynamic_schema_target:
            return cached_fields

        # 캐시에 없는 경우에만 원래 로직 수행 (fallback)
        logger.debug(f"스키마 캐시 미스, 원격 스키마 조회 수행: {cache_key}")

        try:
            # **성능 최적화: 최적화된 DuckDB 연결 사용**
            conn = _create_optimized_connection()
            try:
                if self.is_url:
                    # URL인 경우 (R2 URL은 이미 parquet)
                    if self.file_path_str.endswith(('.parquet', '.duckdb')):
                        logger.info(f"Blob 필드 조회: {self.file_path_str.split('/')[-1]}")
                        table_expr = self._get_table_expression(conn, self.file_path_str)
                        base_query = f"SELECT * FROM {table_expr} LIMIT 1"
                        result = conn.execute(base_query)
                        columns = [desc[0] for desc in result.description]
                        return self._cache_schema(columns)
                    else:
                        logger.info(f"Blob JSON 필드 조회: {self.file_path_str.split('/')[-1]}")
                        # JSON URL의 경우 기본 필드 반환 (실제로는 parquet만 사용)
                        return self._cache_schema(["id", "name", "company", "date"])
                else:
                    # 로컬 파일인 경우 (self.file_path가 Path 객체여야 함)
                    if hasattr(self.file_path, 'resolve'):
                        abs_file_path = str(self.file_path.resolve())
                    else:
                        # 문자열인 경우 Path 객체로 변환
                        abs_file_path = str(Path(self.file_path).resolve())
                    tabular_path = self._resolve_tabular_path()

                    if tabular_path:
                        table_expr = self._get_table_expression(conn, tabular_path)
                        base_query = f"SELECT * FROM {table_expr} LIMIT 1"
                        result = conn.execute(base_query)
                        columns = [desc[0] for desc in result.description]
                        return self._cache_schema(columns)
                    else:
                        # JSON 파일의 경우 기존 로직 사용
                        base_query = self._build_base_query(conn, 1.0)  # 작은 크기로 테스트

                    # 첫 번째 레코드로 필드 확인
                    result = conn.execute(f"{base_query} LIMIT 1")
                    records = result.fetchall()

                    if not records:
                        return self._cache_schema([])

                    structure = self._detect_json_structure()
                    record = records[0]

                    if structure in ['nested_safetykorea', 'nested_data']:
                        # item 필드 분석
                        item = record[0]
                        if hasattr(item, '_asdict'):
                            return self._cache_schema(list(item._asdict().keys()))
                        elif isinstance(item, dict):
                            return self._cache_schema(list(item.keys()))
                        else:
                            return self._cache_schema([])
                    else:
                        # 일반 배열 구조
                        return self._cache_schema([desc[0] for desc in result.description])
            finally:
                conn.close()

        except Exception as e:
            logger.warning(f"필드 분석 실패: {e}")
            return []
    
    def _is_field_case_insensitive(self, field_name: str) -> bool:
        """필드가 대소문자 구분 안함인지 확인"""
        # case_insensitive_fields에 명시적으로 설정된 경우
        if field_name in self.case_config.get("case_insensitive_fields", {}):
            return self.case_config["case_insensitive_fields"][field_name]
        
        # case_sensitive_fields에 명시적으로 설정된 경우 (반대로)
        if field_name in self.case_config.get("case_sensitive_fields", {}):
            return not self.case_config["case_sensitive_fields"][field_name]  # False면 case_sensitive이므로 case_insensitive는 False
        
        # 기본값 사용
        return self.case_config.get("default_case_insensitive", False)

    @staticmethod
    def _build_date_order_expression(field_name: str, table_alias: str = "") -> str:
        """다양한 형식의 날짜 문자열을 DATE로 정렬하기 위한 표현식 생성"""
        prefix = f'{table_alias}' if table_alias else ''
        column_expr = f'{prefix}"{field_name}"'
        trimmed_expr = f"TRIM({column_expr})"

        return (
            "CASE "
            f"WHEN {column_expr} IS NULL OR {trimmed_expr} = '' THEN NULL "
            f"WHEN LENGTH({trimmed_expr}) = 8 THEN TRY_STRPTIME({column_expr}, '%Y%m%d') "
            f"WHEN LENGTH({trimmed_expr}) = 14 THEN TRY_STRPTIME({column_expr}, '%Y%m%d%H%M%S') "
            f"ELSE TRY_CAST({column_expr} AS DATE) "
            "END"
        )
    
    def _build_where_clause(self, keyword: str, search_field: str) -> tuple[str, list]:
        """검색 조건 SQL WHERE 절 생성 (파라미터 바인딩 사용)"""
        if not keyword:
            return "1=1", []  # 모든 결과 반환, 파라미터 없음

        exact_match_fields = {"business_number", "사업자등록번호", "ftc_business_number"}
        if search_field in exact_match_fields:
            cleaned_keyword = keyword.replace('-', '').replace(' ', '')
            field_aliases = [search_field]
            if search_field == "business_number":
                field_aliases.append("ftc_business_number")
            if search_field == "ftc_business_number":
                field_aliases.append("business_number")

            conditions = []
            parameters = []
            available_fields = self._get_available_fields()

            for field in field_aliases:
                if field in available_fields:
                    conditions.append(f"REPLACE(REPLACE(CAST(\"{field}\" AS VARCHAR), '-', ''), ' ', '') = ?")
                    parameters.append(cleaned_keyword)

            if conditions:
                where_clause = " OR ".join(conditions)
                return where_clause, parameters
            else:
                return "1=0", []

        # Parquet 파일 사용 여부 확인
        if self.is_url:
            # URL인 경우 (R2 URL은 parquet)
            using_parquet = self.file_path_str.endswith(('.parquet', '.duckdb'))
        else:
            # 로컬 파일인 경우 (self.file_path가 Path 객체여야 함)
            if hasattr(self.file_path, 'resolve'):
                abs_file_path = str(self.file_path.resolve())
            else:
                # 문자열인 경우 Path 객체로 변환
                abs_file_path = str(Path(self.file_path).resolve())
            parquet_path = abs_file_path.replace('.json', '.parquet')
            suffix = Path(abs_file_path).suffix.lower()
            using_parquet = (suffix == '.duckdb' and Path(abs_file_path).exists()) or Path(parquet_path).exists()
        
        # 실제 사용 가능한 필드 가져오기
        available_fields = self._get_available_fields()
        
        # 파일 타입에 따른 검색 대상 결정
        if using_parquet:
            # Parquet: 정규화된 테이블 구조, 테이블 별칭 없음
            table_alias = ""
        else:
            # JSON: 구조에 따른 테이블 별칭 결정
            structure = self._detect_json_structure()
            if structure in ['nested_safetykorea', 'nested_data']:
                table_alias = "item."
            else:
                table_alias = ""
            
        
        # **새로운 검색 필드 매핑: 업체명, 모델명, 제품명만 지원**
        field_mappings = {
            "company_name": ["업체명", "maker_name", "entrprsNm", "상호/법인명", "사업자명"],
            "model_name": ["모델명", "model_name"],
            "product_name": ["제품명", "product_name", "prductNm", "품목명"]
        }
        
        target_fields = field_mappings.get(search_field, [search_field])

        # **검색 필드 수집 - 모든 매칭 필드에서 검색**
        existing_fields = []
        for field in target_fields:
            if field in available_fields:
                existing_fields.append(field)  # 모든 매칭 필드 수집
        
        if not existing_fields:
            # 매핑된 필드가 없으면 원본 필드명으로 시도
            existing_fields = [search_field] if search_field in available_fields else []
        
        conditions = []
        parameters = []
        
        for field in existing_fields:
            # 필드별 대소문자 구분 설정 확인
            is_case_insensitive = self._is_field_case_insensitive(field)

            # **검색 패턴과 연산자 결정 - 인증번호/신고번호는 정확 매칭**
            search_pattern, operator = _get_search_pattern_and_operator(keyword, field)

            # **성능 최적화: 검색어 사전처리 - 필드별 대소문자 구분에 따라 미리 변환**
            if is_case_insensitive and operator == 'LIKE':
                search_pattern = f"%{keyword.lower()}%"

            if using_parquet:
                # Parquet: 안전한 CAST 적용
                if is_case_insensitive and operator == 'LIKE':
                    # **성능 최적화: 컬럼만 LOWER, 검색어는 이미 Python에서 변환됨**
                    conditions.append(f"LOWER(CAST({table_alias}\"{field}\" AS VARCHAR)) {operator} ?")
                    logger.info(f"필드 '{field}': 대소문자 구분 안함 (컬럼만 LOWER 적용), 연산자: {operator}")
                else:
                    # 대소문자 구분함 또는 정확 매칭: LOWER 함수 사용 안함
                    conditions.append(f"CAST({table_alias}\"{field}\" AS VARCHAR) {operator} ?")
                    logger.info(f"필드 '{field}': 대소문자 구분함 또는 정확매칭, 연산자: {operator}")
            else:
                # JSON: 복합 타입은 문자열로 변환하여 검색
                if is_case_insensitive and operator == 'LIKE':
                    # **성능 최적화: 컬럼만 LOWER, 검색어는 이미 Python에서 변환됨**
                    conditions.append(f"LOWER(CAST({table_alias}\"{field}\" AS VARCHAR)) {operator} ?")
                    logger.info(f"필드 '{field}': 대소문자 구분 안함 (컬럼만 LOWER 적용), 연산자: {operator}")
                else:
                    # 대소문자 구분함 또는 정확 매칭: LOWER 함수 사용 안함
                    conditions.append(f"CAST({table_alias}\"{field}\" AS VARCHAR) {operator} ?")
                    logger.info(f"필드 '{field}': 대소문자 구분함 또는 정확매칭, 연산자: {operator}")
            parameters.append(search_pattern)
        
        where_clause = " OR ".join(conditions) if conditions else "1=1"
        logger.info(f"'{search_field}' 검색: {len(existing_fields)}개 필드 - {existing_fields}")

        # 디버그 정보를 인스턴스 변수에 저장 (API 응답에서 사용)
        self.debug_info = {
            "search_field": search_field,
            "existing_fields": existing_fields,
            "field_count": len(existing_fields),
            "where_clause": where_clause
        }

        current_required = getattr(self, "dynamic_required_fields", [])
        for col in existing_fields:
            if col and col not in current_required:
                current_required.append(col)
        self.dynamic_required_fields = current_required

        return where_clause, parameters

    def _build_filter_conditions(self, filters: Optional[Dict[str, Any]], table_alias: str = "") -> tuple:
        """추가 필터 조건 생성

        Args:
            filters: 필터 조건 딕셔너리
                - date_range: {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
                - certification_type: ['KC', 'CE', 'FCC'] 
                - company_type: ['manufacturer', 'importer']
                - exclude_keywords: ['test', 'sample']
                - numeric_range: {'field': 'price', 'min': 100, 'max': 1000}
        
        Returns:
            tuple: (filter_conditions_string, parameters_list)
        """
        if not filters:
            return "1=1", []

        conditions = []
        parameters = []
        available_fields = self._get_available_fields()

        # 날짜 범위 필터 - 실제 존재하는 날짜 컬럼만 사용
        if 'date_range' in filters:
            date_range = filters['date_range']
            # 데이터셋별 날짜 컬럼 매핑 (우선순위 순)
            date_columns = ["인증일자", "인증변경일자", "서명일자", "인증만료일자", "완료일", "등록일자", "date", "cert_date"]
            existing_date_column = None
            for col in date_columns:
                if col in available_fields:
                    existing_date_column = col
                    break

            if existing_date_column:
                logger.info(f"날짜 필터 적용: {existing_date_column} 컬럼 사용")
                if 'start' in date_range:
                    conditions.append(f"CAST({table_alias}\"{existing_date_column}\" AS DATE) >= ?")
                    parameters.append(date_range['start'])
                if 'end' in date_range:
                    conditions.append(f"CAST({table_alias}\"{existing_date_column}\" AS DATE) <= ?")
                    parameters.append(date_range['end'])
            else:
                logger.warning("날짜 필터 스킵: 해당 데이터셋에 날짜 컬럼이 없습니다")
        
        # 인증 타입 필터 - 실제 존재하는 인증번호 컬럼만 사용
        if 'certification_type' in filters:
            cert_types = filters['certification_type']
            if cert_types:
                # 데이터셋별 인증번호 컬럼 매핑 (우선순위 순)
                cert_columns = ["인증번호", "certification_no", "license_no", "cert_no", "registration_no"]
                existing_cert_column = None
                for col in cert_columns:
                    if col in available_fields:
                        existing_cert_column = col
                        break

                if existing_cert_column:
                    logger.info(f"인증 타입 필터 적용: {existing_cert_column} 컬럼 사용")
                    # IN 절을 위한 플레이스홀더 생성
                    placeholders = ','.join(['?' for _ in cert_types])
                    conditions.append(f"UPPER(CAST({table_alias}\"{existing_cert_column}\" AS VARCHAR)) RLIKE ANY(ARRAY[{placeholders}])")
                    parameters.extend([f".*{cert_type}.*" for cert_type in cert_types])
                else:
                    logger.warning("인증 타입 필터 스킵: 해당 데이터셋에 인증번호 컬럼이 없습니다")
        
        # 업체 타입 필터 (제조업체/수입업체) - 실제 존재하는 수입자 컬럼만 사용
        if 'company_type' in filters:
            company_types = filters['company_type']
            # 데이터셋별 수입자 컬럼 매핑 (우선순위 순)
            importer_columns = ["수입자", "importer", "import_company", "importerName", "수입업체"]
            existing_importer_column = None
            for col in importer_columns:
                if col in available_fields:
                    existing_importer_column = col
                    break

            if existing_importer_column:
                logger.info(f"업체 타입 필터 적용: {existing_importer_column} 컬럼 사용")
                ct_conditions = []
                if 'manufacturer' in company_types:
                    ct_conditions.append(f"(CAST({table_alias}\"{existing_importer_column}\" AS VARCHAR) IS NULL OR CAST({table_alias}\"{existing_importer_column}\" AS VARCHAR) = '')")
                if 'importer' in company_types:
                    ct_conditions.append(f"(CAST({table_alias}\"{existing_importer_column}\" AS VARCHAR) IS NOT NULL AND CAST({table_alias}\"{existing_importer_column}\" AS VARCHAR) != '')")

                if ct_conditions:
                    conditions.append("(" + " OR ".join(ct_conditions) + ")")
            else:
                logger.warning("업체 타입 필터 스킵: 해당 데이터셋에 수입자 컬럼이 없습니다")
        
        # 제외 키워드 필터 - 실제 존재하는 제품명, 업체명 컬럼만 사용
        if 'exclude_keywords' in filters:
            exclude_keywords = filters['exclude_keywords']
            # 데이터셋별 제품명, 업체명 컬럼 매핑 (우선순위 순)
            product_columns = ["제품명", "product_name", "prductNm", "품목명", "기자재명칭"]
            company_columns = ["업체명", "company_name", "entrprsNm", "상호/법인명", "사업자명", "maker_name"]

            existing_product_column = None
            existing_company_column = None

            for col in product_columns:
                if col in available_fields:
                    existing_product_column = col
                    break

            for col in company_columns:
                if col in available_fields:
                    existing_company_column = col
                    break

            for keyword in exclude_keywords:
                exclude_conditions = []
                if existing_product_column:
                    exclude_conditions.append(f"LOWER(CAST({table_alias}\"{existing_product_column}\" AS VARCHAR)) LIKE ?")
                    parameters.append(f"%{keyword.lower()}%")
                if existing_company_column:
                    exclude_conditions.append(f"LOWER(CAST({table_alias}\"{existing_company_column}\" AS VARCHAR)) LIKE ?")
                    parameters.append(f"%{keyword.lower()}%")

                if exclude_conditions:
                    conditions.append(f"NOT ({' OR '.join(exclude_conditions)})")
                    logger.info(f"제외 키워드 필터 적용: '{keyword}' - 사용 컬럼: {[c for c in [existing_product_column, existing_company_column] if c]}")
                else:
                    logger.warning(f"제외 키워드 필터 스킵: '{keyword}' - 제품명/업체명 컬럼이 없습니다")
        
        # 숫자 범위 필터
        if 'numeric_range' in filters:
            numeric_range = filters['numeric_range']
            field = numeric_range.get('field', 'price')
            if 'min' in numeric_range:
                conditions.append(f"TRY_CAST({table_alias}\"{field}\" AS DOUBLE) >= ?")
                parameters.append(numeric_range['min'])
            if 'max' in numeric_range:
                conditions.append(f"TRY_CAST({table_alias}\"{field}\" AS DOUBLE) <= ?")
                parameters.append(numeric_range['max'])
        
        # 결과 조합
        if conditions:
            return " AND ".join(conditions), parameters
        else:
            return "1=1", []

    def get_distinct_values(self, field_name: str, limit: int = 100) -> List[Any]:
        """구조화된 데이터 파일에서 특정 필드의 DISTINCT 값을 조회"""
        tabular_path = self._resolve_tabular_path()
        if not tabular_path:
            raise ValueError("Distinct 조회는 Parquet/DuckDB 파일에서만 지원됩니다")

        conn, conn_lock = self._get_connection()
        with conn_lock:
            table_expr = self._get_table_expression(conn, tabular_path)
            query = (
                f'SELECT DISTINCT "{field_name}" FROM {table_expr} '
                f'WHERE "{field_name}" IS NOT NULL LIMIT {limit}'
            )
            rows = conn.execute(query).fetchall()
            return [row[0] for row in rows if row and row[0] is not None]

    def _get_file_size_mb(self) -> float:
        """파일 크기 (MB) 반환"""
        if self.is_url:
            # URL인 경우 기본적으로 대용량 파일로 가정 (Blob 파일은 일반적으로 큰 파일)
            logger.info(f"URL 파일은 크기를 추정: 100MB로 가정")
            return 100.0
        else:
            # 로컬 파일인 경우 실제 크기 계산 (self.file_path는 Path 객체)
            if hasattr(self.file_path, 'stat'):
                size_bytes = self.file_path.stat().st_size
                return size_bytes / (1024 * 1024)
            else:
                # 문자열인 경우 Path 객체로 변환
                file_path_obj = Path(self.file_path)
                size_bytes = file_path_obj.stat().st_size
                return size_bytes / (1024 * 1024)
    
    async def search_streaming(self,
                             keyword: Optional[str] = None,
                             search_field: str = "all",
                             limit: Optional[int] = 20,
                             page: int = 1,
                             filters: Optional[Dict[str, Any]] = None,
                             collect_results: bool = True,
                             chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
                             chunk_size: int = 1000) -> Dict[str, Any]:
        """스트리밍 방식으로 SafetyKorea 데이터 검색

        Args:
            keyword: 검색 키워드
            search_field: 검색 필드 ("all", "product_name", "company_name", etc.)
            limit: 최대 결과 개수 (None이면 전체)
            page: 페이지 번호 (1부터 시작)
            filters: 추가 필터 조건 딕셔너리
            collect_results: 결과 리스트 수집 여부 (False 면 chunk_callback으로 전달)
            chunk_callback: collect_results=False일 때 결과 청크를 처리할 콜백
            chunk_size: chunk_callback으로 전달할 배치 크기

        Returns:
            Dict: 검색 결과 및 통계 정보
        """
        
        def _execute_query():
            start_time = time.time()
            conn, conn_lock = self._get_connection()

            with conn_lock:
                # 서버사이드 페이지네이션: page와 limit으로 offset 계산
                effective_limit = None if limit is None or limit <= 0 else limit
                offset = (page - 1) * effective_limit if effective_limit else 0
                streaming_mode = not collect_results and chunk_callback is not None
                logger.info(f"📄 페이지네이션: page={page}, limit={limit}, offset={offset}, streaming={streaming_mode}")

                file_size_mb = self._get_file_size_mb()
                logger.info(f"파일 크기: {file_size_mb:.1f}MB")

                where_clause, where_parameters = self._build_where_clause(keyword, search_field)
                filter_clause, filter_parameters = self._build_filter_conditions(filters)

                try:
                    if file_size_mb > 1000:
                        logger.warning(f"대용량 파일 ({file_size_mb:.1f}MB) 감지. 외부 파일 분할 또는 스트리밍 처리 권장")

                    base_query = self._build_base_query(conn, file_size_mb)

                    tabular_path = self._resolve_tabular_path()
                    using_parquet = tabular_path is not None

                    if using_parquet:
                        # 🎯 download_fields 등 required_fields 적용
                        essential_cols = self._get_essential_columns()
                        select_clause = essential_cols if essential_cols != "*" else "*"
                        available_fields = self._get_available_fields()
                        order_by = None
                        logger.info(f"📊 성능 최적화: {len(essential_cols.split(',')) if essential_cols != '*' else '전체'}개 컬럼 선택 (dataA/{self.subcategory})")
                        logger.info("⚙️ Parquet 결과는 파일 저장 순서를 그대로 사용합니다")

                    else:
                        structure = self._detect_json_structure()
                        available_fields = self._get_available_fields()

                        if structure in ['nested_safetykorea', 'nested_data']:
                            select_clause = "item"
                            sort_candidates = ["productName", "제품명", "업체명", "모델명"]
                            order_field = next((f for f in sort_candidates if f in available_fields), None)
                            order_by = f"item.\"{order_field}\"" if order_field else "item"
                        else:
                            select_clause = "*"
                            date_candidates = [
                                "완료일", "인증일자", "인증변경일자", "서명일자", "인증만료일자",
                                "완료일자", "발급일", "만료일", "설립일",
                                "cert_date", "sign_date", "cert_chg_date",
                                "registration_date", "approval_date", "declaration_date", "recall_date",
                                "등록일", "승인일", "신고일", "리콜일", "생성일", "수정일"
                            ]
                            date_candidates.extend([
                                "신고증명서 발급일", "시험성적서 만료일", "유통기한"
                            ])
                            product_candidates = ["품목", "제품명", "product_name", "업체명", "company_name", "상호", "기자재명칭", "모델명", "model_name"]

                            date_field = next((f for f in date_candidates if f in available_fields), None)
                            product_field = next((f for f in product_candidates if f in available_fields), None)

                            order_parts = []
                            if date_field:
                                date_expr = self._build_date_order_expression(date_field)
                                order_parts.append(f'{date_expr} DESC NULLS LAST')
                            if product_field:
                                order_parts.append(f'"{product_field}" ASC')

                            if order_parts:
                                order_by = ', '.join(order_parts)
                                logger.info(f"🎯 ORDER BY 적용됨: {order_by}")
                                logger.info(f"📅 선택된 date_field: {date_field}")
                                for col in [date_field, product_field]:
                                    if col and col not in self.dynamic_required_fields:
                                        self.dynamic_required_fields.append(col)
                            else:
                                first_field = available_fields[0] if available_fields else "1"
                                order_by = f'"{first_field}"' if first_field != "1" else "1"
                                logger.warning(f"❌ ORDER BY 기본값 사용: {order_by} (날짜 필드 없음)")

                    combined_conditions = []
                    combined_parameters = []

                    if where_clause != "1=1":
                        combined_conditions.append(f"({where_clause})")
                        combined_parameters.extend(where_parameters)

                    if filter_clause != "1=1":
                        combined_conditions.append(f"({filter_clause})")
                        combined_parameters.extend(filter_parameters)

                    limit_clause = "" if effective_limit is None else f"LIMIT {effective_limit}"

                    order_clause = f"ORDER BY {order_by}" if order_by else ""

                    if combined_conditions:
                        final_where_clause = " AND ".join(combined_conditions)
                        filtered_query = f"""
                        SELECT {select_clause}, COUNT(*) OVER() as total_count
                        FROM ({base_query})
                        WHERE {final_where_clause}
                        {order_clause}
                        {limit_clause} OFFSET {offset}
                        """
                        count_query = None
                    else:
                        filtered_query = f"""
                        SELECT {select_clause}, COUNT(*) OVER() as total_count
                        FROM ({base_query})
                        {order_clause}
                        {limit_clause} OFFSET {offset}
                        """
                        count_query = None
                        combined_parameters = []

                    logger.info("DuckDB 쿼리 실행 시작...")

                    result = conn.execute(filtered_query, combined_parameters)

                    results = []
                    total_processed = 0
                    total_count_window = None
                    batch_fetch_size = chunk_size if streaming_mode else 1000
                    chunk_buffer: List[Dict[str, Any]] = [] if streaming_mode else []

                    if using_parquet:
                        structure = 'parquet'
                    else:
                        structure = self._detect_json_structure()

                    try:
                        # fetchall을 사용한 안전한 데이터 처리 (배치 로직 유지)
                        while True:
                            batch = result.fetchmany(batch_fetch_size)
                            if not batch:
                                break

                            for row in batch:
                                if using_parquet:
                                    column_names = [desc[0] for desc in result.description]
                                    record = dict(zip(column_names, row)) if row else None
                                elif structure in ['nested_safetykorea', 'nested_data']:
                                    raw_record = row[0] if row else None
                                    if raw_record:
                                        if hasattr(raw_record, '_asdict'):
                                            record = raw_record._asdict()
                                        elif isinstance(raw_record, dict):
                                            record = raw_record
                                        else:
                                            record = {"error": f"Unexpected record type: {type(raw_record)}", "content": str(raw_record)}
                                    else:
                                        record = None
                                else:
                                    column_names = [desc[0] for desc in result.description]
                                    record = dict(zip(column_names, row)) if row else None

                                if record:
                                    if isinstance(record, dict) and 'total_count' in record:
                                        total_count_window = record['total_count']
                                        del record['total_count']

                                    total_processed += 1

                                    if streaming_mode:
                                        chunk_buffer.append(record)
                                    else:
                                        results.append(record)

                                    if effective_limit is not None and total_processed >= effective_limit:
                                        break

                            if streaming_mode and chunk_callback and chunk_buffer:
                                chunk_callback(chunk_buffer.copy(), total_processed)
                                chunk_buffer.clear()

                            if effective_limit is not None and total_processed >= effective_limit:
                                break

                        # 마지막 남은 chunk_buffer 처리
                        if streaming_mode and chunk_callback and chunk_buffer:
                            chunk_callback(chunk_buffer.copy(), total_processed)
                            chunk_buffer.clear()

                    except Exception as batch_error:
                        logger.warning(f"배치 처리 중 오류: {batch_error}")

                    if count_query is None:
                        if results:
                            if total_count_window is not None:
                                total_count = total_count_window
                            else:
                                total_count = total_processed
                        else:
                            total_count = 0
                    else:
                        try:
                            count_result = conn.execute(count_query, combined_parameters).fetchall()
                            total_count = count_result[0][0] if count_result and count_result[0] else total_processed
                        except Exception as count_error:
                            logger.warning(f"카운트 쿼리 오류: {count_error}")
                            total_count = total_processed

                    processing_time = time.time() - start_time

                    if effective_limit:
                        total_pages = (total_count + effective_limit - 1) // effective_limit if effective_limit > 0 else 1
                        has_next = page < total_pages
                        items_per_page = effective_limit
                    else:
                        total_pages = 1
                        has_next = False
                        items_per_page = total_count
                    has_prev = page > 1

                    return {
                        "results": results if not streaming_mode else [],
                        "pagination": {
                            "total_count": total_count,
                            "total_pages": total_pages,
                            "current_page": page,
                            "items_per_page": items_per_page,
                            "has_next": has_next,
                            "has_prev": has_prev
                        },
                        "stats": {
                            "processed_records": total_processed,
                            "processing_time": round(processing_time, 2),
                            "records_per_second": int(total_processed / processing_time) if processing_time > 0 else 0,
                            "file_size_mb": round(file_size_mb, 1),
                            "performance_gain": round(113 / processing_time, 1) if processing_time > 0 else 0
                        },
                        "debug_info": self.debug_info if hasattr(self, 'debug_info') and self.debug_info else {},
                        "query_info": {
                            "keyword": keyword,
                            "search_field": search_field,
                            "page": page,
                            "limit": limit,
                            "offset": offset
                        }
                    }

                except Exception as e:
                    processing_time = time.time() - start_time

                    if "maximum_object_size" in str(e) or (file_size_mb > 500 and "Could not read" in str(e)):
                        error_msg = f"대용량 파일 ({file_size_mb:.1f}MB) 처리 실패. GitHub Releases 외부 저장 또는 파일 분할 필요"
                        logger.error(f"{error_msg}: {e}")

                        return {
                            "error": "large_file_processing_failed",
                            "message": error_msg,
                            "suggestion": "파일을 GitHub Releases에 업로드하고 HTTP URL로 접근하거나, 파일을 작은 단위로 분할하세요",
                            "file_size_mb": round(file_size_mb, 1),
                            "processing_time": round(processing_time, 2),
                            "alternative": "기존 ijson 방식으로 fallback 가능"
                        }
                    else:
                        logger.error(f"DuckDB 쿼리 실행 오류: {e}")
                        return {
                            "error": "query_execution_failed",
                            "message": str(e),
                            "processing_time": round(processing_time, 2)
                        }

        # 비동기 실행
        result = await asyncio.to_thread(_execute_query)
        return result
    
    def close(self):
        """연결 종료 - Connection Pool 사용으로 개별 연결 관리 불필요"""
        # **성능 최적화: Connection Pool 사용으로 개별 연결 관리 제거**
        # Connection Pool이 자동으로 연결을 관리하므로 별도 처리 불필요
        logger.info("DuckDB Connection Pool 사용 중 - 개별 연결 종료 불필요")

# 편의 함수
async def duckdb_search_large_file(file_path: str,
                                  keyword: Optional[str] = None,
                                  search_field: str = "all",
                                  limit: Optional[int] = 20,
                                  page: int = 1,
                                  filters: Optional[Dict[str, Any]] = None,
                                  category: str = None,
                                  subcategory: str = None,
                                  result_type: str = None,
                                  collect_results: bool = True,
                                  chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
                                  chunk_size: int = 1000,
                                  required_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """DuckDB를 사용한 대용량 파일 검색 (편의 함수)
    
    Args:
        file_path: SafetyKorea JSON 파일 경로
        keyword: 검색 키워드
        search_field: 검색 필드
        limit: 최대 결과 개수  
        offset: 결과 시작 위치
        filters: 추가 필터 조건
        
    Returns:
        Dict: 검색 결과
    """
    processor = DuckDBProcessor(
        file_path,
        category=category,
        subcategory=subcategory,
        result_type=result_type,
        required_fields=required_fields,
    )
    try:
        return await processor.search_streaming(
            keyword,
            search_field,
            limit,
            page,
            filters,
            collect_results,
            chunk_callback,
            chunk_size
        )
    finally:
        processor.close()

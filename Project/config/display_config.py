"""
표시 필드 및 다운로드 컬럼 설정 관리 모듈
각 카테고리별로 동적으로 필드 표시를 설정할 수 있는 시스템
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, field_validator
import json
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

SUBCATEGORY_ALIAS_MAP = {
    "approval-details": "approval",
    "rra-certification": "rra-cert",
    "rra-self-conformity": "rra-self-cert",
}


def normalize_subcategory(subcategory: Optional[str]) -> Optional[str]:
    if not subcategory:
        return subcategory
    alias = SUBCATEGORY_ALIAS_MAP.get(subcategory)
    if alias:
        logger.debug(f"서브카테고리 정규화: {subcategory} → {alias}")
    return alias or subcategory

class DisplayField(BaseModel):
    """표시할 필드 설정"""
    field: str = Field(..., description="실제 데이터 필드명")
    name: str = Field(..., description="사용자에게 표시될 한글 이름")
    width: str = Field(default="auto", description="컬럼 너비 (%, px, auto)")
    type: str = Field(default="text", description="필드 타입 (text, date, number, array, image, link)")
    format: Optional[str] = Field(default=None, description="포맷 규칙 (날짜 형식 등)")
    align: str = Field(default="left", description="정렬 방식 (left, center, right)")
    sortable: bool = Field(default=True, description="정렬 가능 여부")
    searchable: bool = Field(default=True, description="검색 가능 여부")
    
    @field_validator('width')
    @classmethod
    def validate_width(cls, v):
        if not v:
            return "auto"
        if v.endswith('%') or v.endswith('px') or v == 'auto':
            return v
        # 숫자만 입력된 경우 px 추가
        try:
            int(v)
            return f"{v}px"
        except ValueError:
            return "auto"

class SearchField(BaseModel):
    """검색 필드 설정"""
    field: str = Field(..., description="검색할 필드명 (all은 전체 검색)")
    name: str = Field(..., description="검색 옵션 표시명")
    placeholder: Optional[str] = Field(default=None, description="검색창 플레이스홀더")

class CategoryDisplayConfig(BaseModel):
    """카테고리별 표시 설정"""
    display_name: str = Field(..., description="카테고리 표시명")
    description: Optional[str] = Field(default="", description="카테고리 설명")
    
    # 표시 필드 설정
    display_fields: List[DisplayField] = Field(..., description="화면에 표시할 필드들")
    
    # 다운로드 필드 설정
    download_fields: List[str] = Field(..., description="다운로드에 포함할 필드들")
    
    # 검색 필드 설정
    search_fields: List[SearchField] = Field(..., description="검색 가능한 필드들")
    
    # 추가 설정
    default_sort_field: Optional[str] = Field(default=None, description="기본 정렬 필드")
    default_sort_order: str = Field(default="asc", description="기본 정렬 순서 (asc, desc)")
    items_per_page: int = Field(default=20, description="페이지당 항목 수")
    enable_export: bool = Field(default=True, description="엑셀 다운로드 활성화")
    
    # UI 설정
    show_summary: bool = Field(default=True, description="요약 정보 표시")
    show_pagination: bool = Field(default=True, description="페이지네이션 표시")
    
    @field_validator('default_sort_order')
    @classmethod
    def validate_sort_order(cls, v):
        return v.lower() if v.lower() in ['asc', 'desc'] else 'asc'

class DisplayConfigManager:
    """표시 설정 관리자"""
    
    def __init__(self, config_dir: str = None):
        # 기본 속성 먼저 초기화
        self._configs = {}
        self._field_settings = {}
        
        # config_dir 설정
        try:
            self.config_dir = Path(config_dir) if config_dir else Path(__file__).parent.parent / "data" / "display_configs"
            # Vercel 서버리스 환경에서 디렉토리 생성 시도 (실패해도 계속 진행)
            self.config_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            logger.warning(f"디렉토리 생성 실패 (서버리스 환경 정상): {e}")
            # fallback 경로 설정
            self.config_dir = Path("/tmp") if Path("/tmp").exists() else Path(".")
        
        # field_settings.json 파일 경로 설정 및 로드
        try:
            self.field_settings_path = Path(__file__).parent / "field_settings.json"
            self._field_settings = self._load_field_settings()
            logger.info(f"field_settings.json 로드 성공: {len(self._field_settings)} 항목")
        except Exception as e:
            logger.error(f"field_settings.json 로드 실패: {e}")
            self._field_settings = {}
        
        # 설정 로드 실패해도 계속 진행
        try:
            self._load_all_configs()
        except Exception as e:
            logger.error(f"설정 파일 로드 실패, 기본값으로 계속 진행: {e}")
            self._configs = {}
    
    def _get_config_path(self, category: str, subcategory: str) -> Path:
        """설정 파일 경로 생성"""
        normalized_subcategory = normalize_subcategory(subcategory)
        return self.config_dir / f"{category}_{normalized_subcategory}.json"
    
    def _load_field_settings(self) -> Dict[str, Any]:
        """field_settings.json 파일 로드"""
        try:
            if not hasattr(self, 'field_settings_path'):
                logger.error("field_settings_path가 초기화되지 않음")
                return {}
                
            if self.field_settings_path and self.field_settings_path.exists():
                logger.info(f"field_settings.json 파일 로드 시도: {self.field_settings_path}")
                with open(self.field_settings_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"field_settings.json 로드 성공: {len(data)} 최상위 키")
                    return data
            else:
                logger.warning(f"field_settings.json 파일이 존재하지 않음: {getattr(self, 'field_settings_path', 'undefined')}")
                return {}
        except Exception as e:
            logger.error(f"field_settings.json 로드 실패: {e}", exc_info=True)
            return {}
    
    def _save_field_settings(self):
        """field_settings.json 파일 저장"""
        try:
            with open(self.field_settings_path, 'w', encoding='utf-8') as f:
                json.dump(self._field_settings, f, ensure_ascii=False, indent=2)
            logger.info("field_settings.json 저장 성공")
        except Exception as e:
            logger.error(f"field_settings.json 저장 실패: {e}")
            raise
    
    def _load_all_configs(self):
        """모든 설정 파일 로드 (field_settings.json 우선)"""
        # 먼저 field_settings.json에서 설정 로드
        self._load_configs_from_field_settings()
        
        # 기존 display_configs 폴더의 설정도 로드 (호환성)
        try:
            for config_file in self.config_dir.glob("*.json"):
                try:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                        
                    # 파일명에서 카테고리 정보 추출
                    filename = config_file.stem
                    parts = filename.split('_', 1)
                    if len(parts) == 2:
                        category, subcategory = parts
                        key = f"{category}/{subcategory}"
                        # field_settings.json에 없는 경우만 로드
                        if key not in self._configs:
                            self._configs[key] = CategoryDisplayConfig(**config_data)
                            logger.info(f"설정 로드 성공 (레거시): {key}")
                        
                except Exception as e:
                    logger.error(f"설정 파일 로드 실패 {config_file}: {e}")
                    
        except Exception as e:
            logger.error(f"설정 디렉토리 스캔 실패: {e}")
    
    def _load_configs_from_field_settings(self):
        """field_settings.json에서 설정 로드"""
        if not self._field_settings:
            return
            
        try:
            # 설정_정보 섹션 제외하고 처리
            for category_key, category_data in self._field_settings.items():
                if category_key.startswith('설정_'):
                    continue
                    
                for subcategory_key, subcategory_data in category_data.items():
                    if isinstance(subcategory_data, dict):
                        # dataC의 경우 3중 중첩 구조 처리
                        if category_key == 'dataC' and subcategory_key in ['success', 'failed']:
                            # dataC.success.safetykorea, dataC.failed.safetykorea 등 처리
                            for final_key, final_data in subcategory_data.items():
                                if isinstance(final_data, dict) and 'category_info' in final_data:
                                    key = f"{category_key}/{subcategory_key}/{final_key}"
                                    config = self._convert_field_settings_to_config(final_data)
                                    self._configs[key] = config
                                    logger.info(f"설정 로드 성공 (field_settings 3-level): {key}")
                        else:
                            # 기존 2중 중첩 구조 처리
                            if 'category_info' in subcategory_data:
                                key = f"{category_key}/{subcategory_key}"
                                config = self._convert_field_settings_to_config(subcategory_data)
                                self._configs[key] = config
                                logger.info(f"설정 로드 성공 (field_settings 2-level): {key}")
                        
        except Exception as e:
            logger.error(f"field_settings.json 설정 변환 실패: {e}")
    
    def _convert_field_settings_to_config(self, data: Dict[str, Any]) -> CategoryDisplayConfig:
        """field_settings.json 형식을 CategoryDisplayConfig로 변환"""
        # 기본 정보
        category_info = data.get('category_info', {})
        display_name = category_info.get('display_name', '')
        description = category_info.get('description', '')
        
        # 표시 필드 변환
        display_fields = []
        for field_data in data.get('display_fields', []):
            if isinstance(field_data, dict):
                display_fields.append(DisplayField(**field_data))
            else:
                # 문자열 형태의 필드명만 있는 경우
                display_fields.append(DisplayField(
                    field=str(field_data),
                    name=str(field_data)
                ))
        
        # 다운로드 필드
        download_fields = data.get('download_fields', [])
        
        # 검색 필드 변환
        search_fields = []
        for field_data in data.get('search_fields', []):
            if isinstance(field_data, dict):
                search_fields.append(SearchField(**field_data))
        
        # UI 설정
        ui_settings = data.get('ui_settings', {})
        
        return CategoryDisplayConfig(
            display_name=display_name,
            description=description,
            display_fields=display_fields,
            download_fields=download_fields,
            search_fields=search_fields,
            default_sort_field=ui_settings.get('default_sort_field'),
            default_sort_order=ui_settings.get('default_sort_order', 'asc'),
            items_per_page=ui_settings.get('items_per_page', 20),
            enable_export=ui_settings.get('enable_export', True),
            show_summary=ui_settings.get('show_summary', True),
            show_pagination=ui_settings.get('show_pagination', True)
        )
    
    def get_config(self, category: str, subcategory: str) -> CategoryDisplayConfig:
        """특정 카테고리의 설정 반환"""
        try:
            normalized_subcategory = normalize_subcategory(subcategory)
            key = f"{category}/{normalized_subcategory}"
            logger.info(f"설정 조회 요청: {key}")
            
            # 기본 속성들이 초기화되어 있는지 확인
            if not hasattr(self, '_configs'):
                logger.warning("_configs 초기화되지 않음, 빈 dict로 초기화")
                self._configs = {}
            if not hasattr(self, '_field_settings'):
                logger.warning("_field_settings 초기화되지 않음, 빈 dict로 초기화")
                self._field_settings = {}
            
            # field_settings.json 파일이 변경된 경우에만 다시 로드
            try:
                if hasattr(self, 'field_settings_path') and self.field_settings_path and self.field_settings_path.exists():
                    current_mtime = self.field_settings_path.stat().st_mtime
                    if not hasattr(self, '_last_mtime') or current_mtime != self._last_mtime:
                        self._last_mtime = current_mtime
                        self._field_settings = self._load_field_settings()
                        self._configs.clear()  # 캐시 클리어
                        self._load_configs_from_field_settings()
            except Exception as e:
                logger.error(f"field_settings.json 재로드 실패: {e}")
            
            # 캐시된 설정이 있으면 반환
            if key in self._configs:
                logger.info(f"캐시에서 설정 반환: {key}")
                return self._configs[key]
            
            # 파일에서 로드 시도
            try:
                config_path = self._get_config_path(category, normalized_subcategory)
                if config_path.exists():
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                    config = CategoryDisplayConfig(**config_data)
                    self._configs[key] = config
                    logger.info(f"파일에서 설정 로드 완료: {key}")
                    return config
            except Exception as e:
                logger.error(f"설정 파일 로드 실패 {config_path}: {e}")
            
            # 기본 설정 생성 (파일 저장 실패해도 설정은 반환)
            logger.info(f"기본 설정 생성 중: {key}")
            default_config = self._create_default_config(category, normalized_subcategory)
            
            # Vercel 환경에서 파일 저장 실패할 수 있지만 무시
            try:
                self.save_config(category, normalized_subcategory, default_config)
                logger.info(f"기본 설정 저장 완료: {category}/{normalized_subcategory}")
            except Exception as e:
                logger.warning(f"기본 설정 저장 실패 (서버리스 환경 정상): {e}")
            
            # 캐시에 저장하고 반환
            self._configs[key] = default_config
            logger.info(f"기본 설정 반환: {key}")
            return default_config
            
        except Exception as e:
            logger.error(f"get_config 전체 실패: {category}/{subcategory} - {e}", exc_info=True)
            # 최후의 수단: 최소한의 기본 설정 반환
            return CategoryDisplayConfig(
                display_name=f"{category.upper()} - {subcategory.replace('-', ' ').title()}",
                description="오류로 인해 기본 설정이 적용되었습니다.",
                display_fields=[DisplayField(field="id", name="ID", width="100%")],
                download_fields=["id"],
                search_fields=[SearchField(field="all", name="전체", placeholder="검색")],
                default_sort_field="id"
            )
    
    def _create_default_config(self, category: str, subcategory: str) -> CategoryDisplayConfig:
        """
        데이터 파일 접근 없이 안전하게 기본 설정을 생성.
        field_settings.json의 정보를 우선 활용하고, 없으면 최소한의 기본값만 제공.
        """
        logger.info(f"'{category}/{subcategory}'에 대한 기본 설정을 안전하게 생성 중...")
        
        # 1. field_settings.json에 정의된 정보가 있다면 그것을 사용
        if (category in self._field_settings and 
            subcategory in self._field_settings[category]):
            
            logger.info(f"field_settings.json에서 설정 발견: {category}/{subcategory}")
            settings_data = self._field_settings[category][subcategory]
            return self._convert_field_settings_to_config(settings_data)
            
        # 1.5. category/subcategory에서 찾지 못했다면 루트 레벨에서도 확인 (fallback)
        elif subcategory in self._field_settings:
            logger.info(f"field_settings.json 루트에서 설정 발견: {subcategory}")
            settings_data = self._field_settings[subcategory]
            return self._convert_field_settings_to_config(settings_data)

        # 2. field_settings.json에도 정보가 없다면, 가장 기본적인 '안전한' 설정 반환
        logger.info(f"field_settings.json에 정보가 없음. 기본 안전 설정 생성: {category}/{subcategory}")
        return CategoryDisplayConfig(
            display_name=f"{category.upper()} - {subcategory.replace('-', ' ').title()}",
            description="기본 설정이 적용되었습니다. 관리자 페이지에서 설정을 완료하거나 환경변수를 확인해주세요.",
            display_fields=[
                DisplayField(field="id", name="ID", width="10%"),
                DisplayField(field="name", name="이름", width="30%"),
                DisplayField(field="company", name="업체", width="25%"),
                DisplayField(field="date", name="날짜", width="15%", type="date"),
                DisplayField(field="status", name="상태", width="20%")
            ],
            download_fields=["id", "name", "company", "date", "status", "description"],
            search_fields=[
                SearchField(field="all", name="전체", placeholder="전체 필드에서 검색"),
                SearchField(field="name", name="이름", placeholder="이름으로 검색"),
                SearchField(field="company", name="업체명", placeholder="업체명으로 검색")
            ],
            default_sort_field="id"
        )
    
    def save_config(self, category: str, subcategory: str, config: CategoryDisplayConfig):
        """설정을 field_settings.json에 저장"""
        try:
            # field_settings.json에 저장
            if category not in self._field_settings:
                self._field_settings[category] = {}
            
            # 설정을 field_settings 형식으로 변환
            field_settings_data = {
                'category_info': {
                    'display_name': config.display_name,
                    'description': config.description,
                    'icon': 'folder'  # 기본 아이콘
                },
                'display_fields': [field.model_dump() for field in config.display_fields],
                'download_fields': config.download_fields,
                'search_fields': [field.model_dump() for field in config.search_fields],
                'ui_settings': {
                    'default_sort_field': config.default_sort_field,
                    'default_sort_order': config.default_sort_order,
                    'items_per_page': config.items_per_page,
                    'enable_export': config.enable_export,
                    'show_summary': config.show_summary,
                    'show_pagination': config.show_pagination
                }
            }
            
            self._field_settings[category][subcategory] = field_settings_data
            self._save_field_settings()
            
            # 캐시 업데이트
            key = f"{category}/{subcategory}"
            self._configs[key] = config
            
            logger.info(f"설정 저장 성공 (field_settings): {key}")
            
        except Exception as e:
            logger.error(f"설정 저장 실패: {e}")
            raise
    
    def update_config(self, category: str, subcategory: str, updates: Dict[str, Any]):
        """설정 부분 업데이트"""
        current_config = self.get_config(category, subcategory)
        if not current_config:
            raise ValueError(f"설정을 찾을 수 없습니다: {category}/{subcategory}")
        
        # 업데이트 적용
        config_dict = current_config.model_dump()
        config_dict.update(updates)
        
        # 유효성 검증
        updated_config = CategoryDisplayConfig(**config_dict)
        
        # 저장
        self.save_config(category, subcategory, updated_config)
        
        return updated_config
    
    def delete_config(self, category: str, subcategory: str):
        """설정 삭제"""
        key = f"{category}/{subcategory}"
        
        try:
            # field_settings.json에서 삭제
            if (category in self._field_settings and 
                subcategory in self._field_settings[category]):
                del self._field_settings[category][subcategory]
                
                # 카테고리가 비어있으면 삭제
                if not self._field_settings[category]:
                    del self._field_settings[category]
                    
                self._save_field_settings()
            
            # 레거시 파일도 삭제
            config_path = self._get_config_path(category, subcategory)
            if config_path.exists():
                config_path.unlink()
            
            # 캐시에서 삭제
            if key in self._configs:
                del self._configs[key]
                
            logger.info(f"설정 삭제 성공: {key}")
            
        except Exception as e:
            logger.error(f"설정 삭제 실패: {e}")
            raise
    
    def list_configs(self) -> Dict[str, CategoryDisplayConfig]:
        """모든 설정 목록 반환"""
        return self._configs.copy()
    
    def export_client_config(self, category: str, subcategory: str) -> Dict[str, Any]:
        """클라이언트에서 사용할 설정 데이터 반환"""
        config = self.get_config(category, subcategory)
        if not config:
            return {}
        
        return {
            "displayName": config.display_name,
            "description": config.description,
            "displayFields": [field.model_dump() for field in config.display_fields],
            "downloadFields": config.download_fields,
            "searchFields": [field.model_dump() for field in config.search_fields],
            "defaultSort": {
                "field": config.default_sort_field,
                "order": config.default_sort_order
            },
            "pagination": {
                "itemsPerPage": config.items_per_page
            },
            "features": {
                "enableExport": config.enable_export,
                "showSummary": config.show_summary,
                "showPagination": config.show_pagination
            }
        }
    
    def validate_fields_against_data(self, category: str, subcategory: str) -> Dict[str, Any]:
        """설정된 필드가 실제 데이터와 일치하는지 검증"""
        from api.main import get_data_file_path
        
        config = self.get_config(category, subcategory)
        if not config:
            return {"valid": False, "error": "설정을 찾을 수 없습니다"}
        
        data_file_path = get_data_file_path(category, subcategory)
        if not data_file_path:
            return {"valid": False, "error": "데이터 파일 URL을 찾을 수 없습니다"}
        
        try:
            data_file_str = str(data_file_path)
            is_remote = data_file_str.startswith(('https://', 'http://'))
            is_tabular = data_file_str.lower().endswith(('.parquet', '.duckdb'))

            if is_tabular:
                from core.duckdb_processor import DuckDBProcessor

                effective_subcategory = normalize_subcategory(subcategory)
                processor = DuckDBProcessor(
                    data_file_str,
                    category=category,
                    subcategory=effective_subcategory
                )
                try:
                    actual_fields = set(processor.get_available_fields())
                finally:
                    processor.close()
            else:
                if is_remote:
                    file_size_mb = 100.0
                else:
                    local_path = Path(data_file_str)
                    file_size_mb = local_path.stat().st_size / (1024 * 1024) if local_path.exists() else 0

                if file_size_mb > 50:
                    from core.large_file_processor import get_large_file_metadata
                    import asyncio
                    metadata = asyncio.run(get_large_file_metadata(data_file_path))
                    actual_fields = set(metadata.get("available_fields", []))
                else:
                    with open(data_file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    if data.get("data") and len(data["data"]) > 0:
                        first_record = data["data"][0]
                        if "resultData" in first_record:
                            first_record = first_record["resultData"]
                        actual_fields = set(first_record.keys())
                    else:
                        actual_fields = set()
            
            # 설정된 필드들 검증
            display_fields = set(field.field for field in config.display_fields)
            download_fields = set(config.download_fields)
            
            missing_display = display_fields - actual_fields
            missing_download = download_fields - actual_fields
            
            return {
                "valid": len(missing_display) == 0 and len(missing_download) == 0,
                "actual_fields": list(actual_fields),
                "missing_display_fields": list(missing_display),
                "missing_download_fields": list(missing_download),
                "suggestions": {
                    "new_fields": list(actual_fields - display_fields - download_fields)
                }
            }
            
        except Exception as e:
            return {"valid": False, "error": f"검증 중 오류 발생: {str(e)}"}

# 전역 설정 관리자 인스턴스 - Vercel 서버리스 환경에서 임시 비활성화
# display_config_manager = DisplayConfigManager()

def get_display_config_manager():
    """Lazy loading으로 DisplayConfigManager 인스턴스 생성"""
    try:
        return DisplayConfigManager()
    except Exception as e:
        logger.warning(f"DisplayConfigManager 초기화 실패 (서버리스 환경): {e}")
        return None

# 전역 설정 관리자 인스턴스 생성
display_config_manager = DisplayConfigManager()

"""
검색 설정 관리 모듈
카테고리 및 검색 필드를 동적으로 관리할 수 있도록 설계
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel
import json
import os

class SearchFieldConfig(BaseModel):
    """검색 가능한 필드 설정"""
    field: str
    name: str
    type: str  # text, date, number, array, select
    searchable: bool = True
    filterable: bool = True
    sortable: bool = False
    options: List[str] = []  # select 타입의 경우 선택 옵션

class CategoryConfig(BaseModel):
    """카테고리 설정"""
    id: str
    name: str
    description: str = ""
    color: str = "#3498db"  # UI에서 사용할 색상
    icon: str = ""  # 아이콘 클래스명
    parent_id: Optional[str] = None  # 계층형 카테고리 지원

class SearchConfig(BaseModel):
    """전체 검색 설정"""
    categories: List[CategoryConfig]
    search_fields: List[SearchFieldConfig]
    default_sort_field: str = "date"
    default_sort_order: str = "desc"
    items_per_page: int = 20
    max_items_per_page: int = 100

class SearchConfigManager:
    """검색 설정 관리자"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or "/tmp/search_config.json"
        self._config = None
        self._load_config()
    
    def _load_config(self):
        """설정 파일 로드"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    self._config = SearchConfig(**config_data)
            except Exception as e:
                print(f"설정 파일 로드 실패: {e}")
                self._config = self._get_default_config()
        else:
            self._config = self._get_default_config()
            self.save_config()
    
    def _get_default_config(self) -> SearchConfig:
        """기본 설정 반환"""
        return SearchConfig(
            categories=[
                CategoryConfig(
                    id="tech",
                    name="기술",
                    description="IT, 소프트웨어, 하드웨어 관련 데이터",
                    color="#e74c3c",
                    icon="fas fa-laptop-code"
                ),
                CategoryConfig(
                    id="economy",
                    name="경제",
                    description="경제, 금융, 비즈니스 관련 데이터",
                    color="#f39c12",
                    icon="fas fa-chart-line"
                ),
                CategoryConfig(
                    id="society",
                    name="사회",
                    description="사회, 정치, 법률 관련 데이터",
                    color="#27ae60",
                    icon="fas fa-users"
                ),
                CategoryConfig(
                    id="culture",
                    name="문화",
                    description="문화, 예술, 엔터테인먼트 관련 데이터",
                    color="#9b59b6",
                    icon="fas fa-palette"
                )
            ],
            search_fields=[
                SearchFieldConfig(
                    field="title",
                    name="제목",
                    type="text",
                    searchable=True,
                    filterable=False,
                    sortable=True
                ),
                SearchFieldConfig(
                    field="content",
                    name="내용",
                    type="text",
                    searchable=True,
                    filterable=False,
                    sortable=False
                ),
                SearchFieldConfig(
                    field="category",
                    name="카테고리",
                    type="select",
                    searchable=False,
                    filterable=True,
                    sortable=True,
                    options=["tech", "economy", "society", "culture"]
                ),
                SearchFieldConfig(
                    field="date",
                    name="날짜",
                    type="date",
                    searchable=False,
                    filterable=True,
                    sortable=True
                ),
                SearchFieldConfig(
                    field="tags",
                    name="태그",
                    type="array",
                    searchable=True,
                    filterable=True,
                    sortable=False
                ),
                SearchFieldConfig(
                    field="priority",
                    name="우선순위",
                    type="select",
                    searchable=False,
                    filterable=True,
                    sortable=True,
                    options=["high", "medium", "low"]
                )
            ]
        )
    
    def get_config(self) -> SearchConfig:
        """현재 설정 반환"""
        return self._config
    
    def get_categories(self) -> List[CategoryConfig]:
        """카테고리 목록 반환"""
        return self._config.categories
    
    def get_search_fields(self) -> List[SearchFieldConfig]:
        """검색 필드 목록 반환"""
        return self._config.search_fields
    
    def get_filterable_fields(self) -> List[SearchFieldConfig]:
        """필터 가능한 필드 목록 반환"""
        return [field for field in self._config.search_fields if field.filterable]
    
    def get_searchable_fields(self) -> List[SearchFieldConfig]:
        """검색 가능한 필드 목록 반환"""
        return [field for field in self._config.search_fields if field.searchable]
    
    def add_category(self, category: CategoryConfig):
        """카테고리 추가"""
        self._config.categories.append(category)
        self.save_config()
    
    def update_category(self, category_id: str, category: CategoryConfig):
        """카테고리 수정"""
        for i, cat in enumerate(self._config.categories):
            if cat.id == category_id:
                self._config.categories[i] = category
                break
        self.save_config()
    
    def remove_category(self, category_id: str):
        """카테고리 삭제"""
        self._config.categories = [
            cat for cat in self._config.categories 
            if cat.id != category_id
        ]
        self.save_config()
    
    def add_search_field(self, field: SearchFieldConfig):
        """검색 필드 추가"""
        self._config.search_fields.append(field)
        self.save_config()
    
    def update_search_field(self, field_name: str, field: SearchFieldConfig):
        """검색 필드 수정"""
        for i, f in enumerate(self._config.search_fields):
            if f.field == field_name:
                self._config.search_fields[i] = field
                break
        self.save_config()
    
    def remove_search_field(self, field_name: str):
        """검색 필드 삭제"""
        self._config.search_fields = [
            f for f in self._config.search_fields 
            if f.field != field_name
        ]
        self.save_config()
    
    def save_config(self):
        """설정을 파일에 저장"""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self._config.model_dump(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"설정 파일 저장 실패: {e}")
    
    def export_client_config(self) -> Dict[str, Any]:
        """클라이언트에서 사용할 설정 데이터 반환"""
        return {
            "categories": [cat.model_dump() for cat in self._config.categories],
            "searchFields": [field.model_dump() for field in self._config.search_fields],
            "defaultSort": {
                "field": self._config.default_sort_field,
                "order": self._config.default_sort_order
            },
            "pagination": {
                "itemsPerPage": self._config.items_per_page,
                "maxItemsPerPage": self._config.max_items_per_page
            }
        }

# 전역 설정 관리자 인스턴스
search_config_manager = SearchConfigManager()
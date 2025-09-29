"""
클라이언트 사이드 검색 엔진
동적 필터링과 유연한 검색 조건을 지원
"""

from typing import List, Dict, Any, Optional, Callable
import re
from datetime import datetime
from dataclasses import dataclass

@dataclass
class SearchFilter:
    """검색 필터 클래스"""
    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, in, not_in, contains, regex
    value: Any
    case_sensitive: bool = False

@dataclass
class SearchSort:
    """정렬 조건 클래스"""
    field: str
    order: str = "asc"  # asc, desc

class DynamicSearchEngine:
    """동적 검색 엔진"""
    
    def __init__(self):
        self.data: List[Dict[str, Any]] = []
        self.search_fields: List[str] = []
        self.custom_filters: Dict[str, Callable] = {}
    
    def load_data(self, data: List[Dict[str, Any]]):
        """데이터 로드"""
        self.data = data
    
    def set_search_fields(self, fields: List[str]):
        """검색 대상 필드 설정"""
        self.search_fields = fields
    
    def add_custom_filter(self, name: str, filter_func: Callable):
        """커스텀 필터 함수 추가"""
        self.custom_filters[name] = filter_func
    
    def search(self, 
               keyword: Optional[str] = None,
               filters: Optional[List[SearchFilter]] = None,
               sort: Optional[List[SearchSort]] = None,
               limit: Optional[int] = None,
               offset: Optional[int] = 0) -> Dict[str, Any]:
        """
        검색 실행
        
        Args:
            keyword: 키워드 검색
            filters: 필터 조건 목록
            sort: 정렬 조건 목록
            limit: 결과 제한 수
            offset: 시작 인덱스
            
        Returns:
            검색 결과 딕셔너리
        """
        filtered_data = self.data.copy()
        
        # 키워드 검색
        if keyword:
            filtered_data = self._apply_keyword_search(filtered_data, keyword)
        
        # 필터 적용
        if filters:
            for filter_condition in filters:
                filtered_data = self._apply_filter(filtered_data, filter_condition)
        
        # 정렬 적용
        if sort:
            filtered_data = self._apply_sort(filtered_data, sort)
        
        # 페이지네이션
        total_count = len(filtered_data)
        if offset:
            filtered_data = filtered_data[offset:]
        if limit:
            filtered_data = filtered_data[:limit]
        
        return {
            "total_count": len(self.data),
            "filtered_count": total_count,
            "results": filtered_data,
            "has_more": total_count > (offset + len(filtered_data))
        }
    
    def _apply_keyword_search(self, data: List[Dict[str, Any]], keyword: str) -> List[Dict[str, Any]]:
        """키워드 검색 적용"""
        if not self.search_fields:
            return data
        
        keyword_lower = keyword.lower()
        result = []
        
        for item in data:
            found = False
            for field in self.search_fields:
                field_value = self._get_nested_value(item, field)
                if self._match_keyword(field_value, keyword_lower):
                    found = True
                    break
            if found:
                result.append(item)
        
        return result
    
    def _match_keyword(self, value: Any, keyword: str) -> bool:
        """키워드 매칭 검사"""
        if value is None:
            return False
        
        if isinstance(value, str):
            return keyword in value.lower()
        elif isinstance(value, list):
            return any(keyword in str(item).lower() for item in value)
        else:
            return keyword in str(value).lower()
    
    def _apply_filter(self, data: List[Dict[str, Any]], filter_condition: SearchFilter) -> List[Dict[str, Any]]:
        """필터 조건 적용"""
        result = []
        
        for item in data:
            field_value = self._get_nested_value(item, filter_condition.field)
            
            if self._evaluate_filter(field_value, filter_condition):
                result.append(item)
        
        return result
    
    def _evaluate_filter(self, value: Any, filter_condition: SearchFilter) -> bool:
        """필터 조건 평가"""
        filter_value = filter_condition.value
        operator = filter_condition.operator
        
        # 문자열 비교시 대소문자 처리
        if isinstance(value, str) and isinstance(filter_value, str) and not filter_condition.case_sensitive:
            value = value.lower()
            filter_value = filter_value.lower()
        
        try:
            if operator == "eq":
                return value == filter_value
            elif operator == "ne":
                return value != filter_value
            elif operator == "gt":
                return self._safe_compare(value, filter_value, lambda x, y: x > y)
            elif operator == "lt":
                return self._safe_compare(value, filter_value, lambda x, y: x < y)
            elif operator == "gte":
                return self._safe_compare(value, filter_value, lambda x, y: x >= y)
            elif operator == "lte":
                return self._safe_compare(value, filter_value, lambda x, y: x <= y)
            elif operator == "in":
                return value in filter_value if isinstance(filter_value, (list, tuple)) else False
            elif operator == "not_in":
                return value not in filter_value if isinstance(filter_value, (list, tuple)) else True
            elif operator == "contains":
                return str(filter_value) in str(value) if value is not None else False
            elif operator == "regex":
                return bool(re.search(str(filter_value), str(value))) if value is not None else False
            elif operator in self.custom_filters:
                return self.custom_filters[operator](value, filter_value)
            else:
                return False
        except Exception:
            return False
    
    def _safe_compare(self, value1: Any, value2: Any, compare_func: Callable) -> bool:
        """안전한 비교 함수"""
        try:
            # 날짜 문자열 비교
            if isinstance(value1, str) and isinstance(value2, str):
                # ISO 날짜 형식 감지
                if re.match(r'\d{4}-\d{2}-\d{2}', value1) and re.match(r'\d{4}-\d{2}-\d{2}', value2):
                    return compare_func(value1, value2)
            
            # 숫자 비교
            if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
                return compare_func(value1, value2)
            
            # 문자열 비교
            return compare_func(str(value1), str(value2))
        except Exception:
            return False
    
    def _apply_sort(self, data: List[Dict[str, Any]], sort_conditions: List[SearchSort]) -> List[Dict[str, Any]]:
        """정렬 적용"""
        def sort_key(item):
            keys = []
            for sort_condition in sort_conditions:
                value = self._get_nested_value(item, sort_condition.field)
                
                # None 값 처리
                if value is None:
                    value = ""
                
                # 날짜 문자열을 정렬 가능한 형태로 변환
                if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                    try:
                        value = datetime.strptime(value[:10], '%Y-%m-%d').timestamp()
                    except ValueError:
                        pass
                
                # 역순 정렬을 위한 처리
                if sort_condition.order == "desc":
                    if isinstance(value, (int, float)):
                        value = -value
                    elif isinstance(value, str):
                        # 문자열 역순 정렬을 위한 변환
                        value = "".join(chr(ord('z') - ord(c) + ord('a')) if 'a' <= c <= 'z' else c for c in value.lower())
                
                keys.append(value)
            
            return keys
        
        try:
            return sorted(data, key=sort_key)
        except Exception:
            # 정렬 실패시 원본 데이터 반환
            return data
    
    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """중첩된 필드 값 가져오기 (예: "user.profile.name")"""
        try:
            value = data
            for key in field_path.split('.'):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None
                if value is None:
                    break
            return value
        except Exception:
            return None
    
    def get_field_values(self, field: str) -> List[Any]:
        """특정 필드의 모든 고유 값 반환 (필터 옵션 생성용)"""
        values = set()
        for item in self.data:
            value = self._get_nested_value(item, field)
            if value is not None:
                if isinstance(value, list):
                    values.update(value)
                else:
                    values.add(value)
        return sorted(list(values))
    
    def get_statistics(self) -> Dict[str, Any]:
        """데이터 통계 정보 반환"""
        if not self.data:
            return {}
        
        stats = {
            "total_items": len(self.data),
            "field_stats": {}
        }
        
        # 각 필드별 통계
        sample_item = self.data[0]
        for field in sample_item.keys():
            field_values = [self._get_nested_value(item, field) for item in self.data]
            field_values = [v for v in field_values if v is not None]
            
            if field_values:
                stats["field_stats"][field] = {
                    "count": len(field_values),
                    "unique_count": len(set(str(v) for v in field_values)),
                    "type": type(field_values[0]).__name__
                }
        
        return stats
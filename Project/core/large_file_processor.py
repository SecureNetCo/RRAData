"""
대용량 JSON 파일 처리 최적화 모듈
스트리밍 JSON 파싱과 메모리 효율적인 검색 기능 제공
"""

import ijson
import asyncio
import aiofiles
from typing import Dict, List, Any, Optional, AsyncIterator, Callable, Union
from pathlib import Path
import json
import gc
from dataclasses import dataclass
from datetime import datetime
import logging
import weakref
import os
from functools import lru_cache
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchContext:
    """검색 컨텍스트 클래스"""
    keyword: Optional[str] = None
    search_field: Optional[str] = "all"
    filters: Optional[Dict[str, Any]] = None
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    case_sensitive: bool = False

@dataclass
class ProcessingStats:
    """처리 통계 클래스"""
    total_records: int = 0
    processed_records: int = 0
    matched_records: int = 0
    processing_time: float = 0.0
    memory_peak_mb: float = 0.0
    
class MemoryMonitor:
    """메모리 사용량 모니터링"""
    
    def __init__(self):
        self.peak_memory = 0
        
    def get_memory_mb(self) -> float:
        """현재 메모리 사용량 (MB) - 간소화된 버전"""
        try:
            import resource
            memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # macOS/Linux 호환
            if memory_mb < 1:  # Linux는 KB 단위
                memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024
            self.peak_memory = max(self.peak_memory, memory_mb)
            return memory_mb
        except:
            # fallback - 대략적인 추정
            return 50.0
    
    def reset(self):
        """통계 리셋"""
        self.peak_memory = 0

class LargeFileProcessor:
    """대용량 JSON 파일 처리기"""
    
    def __init__(self, file_path: Union[str, Path], cache_size: int = 1000):
        self.file_path = Path(file_path)
        self.cache_size = cache_size
        self.memory_monitor = MemoryMonitor()
        self._metadata_cache: Optional[Dict] = None
        
        # 약한 참조를 사용한 결과 캐시
        self._result_cache = weakref.WeakValueDictionary()
        
        # 파일 존재 여부 확인
        if not self.file_path.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {self.file_path}")
    
    @lru_cache(maxsize=1)
    def get_file_info(self) -> Dict[str, Any]:
        """파일 정보 반환"""
        stat = self.file_path.stat()
        return {
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / 1024 / 1024, 2),
            "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "readable": os.access(self.file_path, os.R_OK)
        }
    
    async def estimate_record_count(self) -> int:
        """레코드 수 추정 (샘플링 기반)"""
        try:
            sample_size = min(1024 * 1024, self.get_file_info()["size_bytes"])  # 1MB 샘플
            
            async with aiofiles.open(self.file_path, 'r', encoding='utf-8-sig') as file:
                sample_data = await file.read(sample_size)
                
            # 간단한 패턴 매칭으로 레코드 수 추정
            sample_str = sample_data
            result_data_count = sample_str.count('"resultData"')
            
            if result_data_count > 0:
                total_size = self.get_file_info()["size_bytes"]
                estimated_count = int((result_data_count * total_size) / sample_size)
                logger.info(f"추정 레코드 수: {estimated_count:,}")
                return estimated_count
            
            return 0
            
        except Exception as e:
            logger.warning(f"레코드 수 추정 실패: {e}")
            return 0
    
    async def stream_records(self, 
                           max_records: Optional[int] = None,
                           progress_callback: Optional[Callable[[int, int], None]] = None) -> AsyncIterator[Dict[str, Any]]:
        """
        스트리밍 방식으로 레코드 순회
        
        Args:
            max_records: 최대 레코드 수 제한
            progress_callback: 진행률 콜백 함수 (processed_count, total_estimated)
        """
        start_time = time.time()
        processed_count = 0
        estimated_total = await self.estimate_record_count() if progress_callback else 0
        
        try:
            # 동기 파일 읽기로 단순화 (BOM 처리)
            with open(self.file_path, 'r', encoding='utf-8-sig') as file:
                # ijson을 사용한 스트리밍 파싱
                parser = ijson.parse(file)
                current_record = {}
                in_result_data = False
                result_data_depth = 0
                
                for prefix, event, value in parser:
                    try:
                        # resultData 객체 시작
                        if event == 'start_map' and prefix.endswith('.resultData'):
                            in_result_data = True
                            result_data_depth = 1
                            current_record = {}
                            
                        elif in_result_data:
                            if event == 'start_map':
                                result_data_depth += 1
                            elif event == 'end_map':
                                result_data_depth -= 1
                                
                                # resultData 객체 완료
                                if result_data_depth == 0:
                                    in_result_data = False
                                    if current_record:
                                        processed_count += 1
                                        
                                        # 진행률 콜백
                                        if progress_callback and processed_count % 100 == 0:
                                            progress_callback(processed_count, estimated_total)
                                        
                                        yield current_record
                                        
                                        # 최대 레코드 수 체크
                                        if max_records and processed_count >= max_records:
                                            return
                                        
                                        # 메모리 정리 및 비동기 제어권 양보
                                        if processed_count % 100 == 0:
                                            await asyncio.sleep(0)
                                        if processed_count % 1000 == 0:
                                            gc.collect()
                                            
                            elif event in ('string', 'number', 'boolean', 'null') and result_data_depth == 1:
                                # 단순 필드
                                field_name = prefix.split('.')[-1]
                                current_record[field_name] = value
                                
                            elif event == 'start_array' and result_data_depth == 1:
                                # 배열 필드 시작
                                field_name = prefix.split('.')[-1]
                                current_record[field_name] = []
                                
                            elif event in ('string', 'number', 'boolean') and '.item' in prefix and result_data_depth > 1:
                                # 배열 아이템
                                field_path = prefix.split('.')
                                if len(field_path) >= 2:
                                    field_name = field_path[-2]
                                    if field_name in current_record and isinstance(current_record[field_name], list):
                                        current_record[field_name].append(value)
                                        
                    except Exception as e:
                        logger.warning(f"레코드 파싱 오류: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"스트리밍 오류: {e}")
            raise
        finally:
            processing_time = time.time() - start_time
            logger.info(f"스트리밍 완료: {processed_count:,}개 레코드, {processing_time:.2f}초")
    
    
    async def search_streaming(self, context: SearchContext) -> Dict[str, Any]:
        """
        스트리밍 방식 검색
        
        Args:
            context: 검색 컨텍스트
            
        Returns:
            검색 결과
        """
        start_time = time.time()
        self.memory_monitor.reset()
        
        stats = ProcessingStats()
        results = []
        current_offset = 0
        
        try:
            async for record in self.stream_records():
                stats.total_records += 1
                stats.processed_records += 1
                
                # 검색 조건 확인
                if self._matches_criteria(record, context):
                    stats.matched_records += 1
                    
                    # 오프셋 처리
                    if current_offset < context.offset:
                        current_offset += 1
                        continue
                    
                    # 결과 추가
                    results.append(record)
                    
                    # 제한 수 체크
                    if context.limit and len(results) >= context.limit:
                        break
                
                # 메모리 모니터링
                if stats.processed_records % 1000 == 0:
                    memory_mb = self.memory_monitor.get_memory_mb()
                    if memory_mb > 500:  # 500MB 제한
                        logger.warning(f"메모리 사용량 높음: {memory_mb:.1f}MB")
                        gc.collect()
            
            stats.processing_time = time.time() - start_time
            stats.memory_peak_mb = self.memory_monitor.peak_memory
            
            return {
                "total_count": stats.total_records,
                "filtered_count": stats.matched_records,
                "results": results,
                "stats": {
                    "processed_records": stats.processed_records,
                    "processing_time": round(stats.processing_time, 2),
                    "memory_peak_mb": round(stats.memory_peak_mb, 2),
                    "records_per_second": int(stats.processed_records / stats.processing_time) if stats.processing_time > 0 else 0
                },
                "has_more": stats.matched_records > (context.offset + len(results))
            }
            
        except Exception as e:
            logger.error(f"스트리밍 검색 오류: {e}")
            raise
    
    def _matches_criteria(self, record: Dict[str, Any], context: SearchContext) -> bool:
        """레코드가 검색 조건에 맞는지 확인"""
        try:
            # 키워드 검색
            if context.keyword:
                if not self._keyword_match(record, context.keyword, context.search_field, context.case_sensitive):
                    return False
            
            # 필터 적용
            if context.filters:
                if not self._filter_match(record, context.filters):
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"조건 매칭 오류: {e}")
            return False
    
    def _keyword_match(self, record: Dict[str, Any], keyword: str, search_field: str, case_sensitive: bool) -> bool:
        """키워드 매칭"""
        if not keyword:
            return True
        
        search_keyword = keyword if case_sensitive else keyword.lower()
        
        if search_field == "all":
            # 모든 필드에서 검색
            return self._search_in_all_fields(record, search_keyword, case_sensitive)
        else:
            # 특정 필드에서 검색
            return self._search_in_specific_field(record, search_keyword, search_field, case_sensitive)
    
    def _search_in_all_fields(self, record: Dict[str, Any], keyword: str, case_sensitive: bool) -> bool:
        """모든 필드에서 키워드 검색"""
        for key, value in record.items():
            if self._value_contains_keyword(value, keyword, case_sensitive):
                return True
        return False
    
    def _search_in_specific_field(self, record: Dict[str, Any], keyword: str, field: str, case_sensitive: bool) -> bool:
        """특정 필드에서 키워드 검색"""
        # 필드명 매핑
        field_mappings = {
            "company_name": ["makerName", "importerName", "company_name", "manufacturer"],
            "business_number": ["business_number", "registration_number", "사업자등록번호"],
            "product_name": ["productName", "product_name", "equipment_name", "item_name"],
            "model_name": ["modelName", "model_name", "model_number"],
            "certification_number": ["certNum", "certification_number", "approval_number"]
        }
        
        search_fields = field_mappings.get(field, [field])
        
        for search_field in search_fields:
            if search_field in record:
                value = record[search_field]
                if self._value_contains_keyword(value, keyword, case_sensitive):
                    return True
        
        return False
    
    def _value_contains_keyword(self, value: Any, keyword: str, case_sensitive: bool) -> bool:
        """값에 키워드가 포함되어 있는지 확인"""
        if value is None:
            return False
        
        if isinstance(value, str):
            search_value = value if case_sensitive else value.lower()
            return keyword in search_value
        elif isinstance(value, list):
            for item in value:
                if self._value_contains_keyword(item, keyword, case_sensitive):
                    return True
        else:
            search_value = str(value)
            if not case_sensitive:
                search_value = search_value.lower()
            return keyword in search_value
        
        return False
    
    def _filter_match(self, record: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """필터 조건 매칭"""
        for filter_key, filter_value in filters.items():
            if filter_key not in record:
                return False
            
            record_value = record[filter_key]
            
            if isinstance(filter_value, list):
                if record_value not in filter_value:
                    return False
            else:
                if record_value != filter_value:
                    return False
        
        return True
    
    async def get_metadata(self, force_refresh: bool = False) -> Dict[str, Any]:
        """메타데이터 추출 (캐시됨)"""
        if self._metadata_cache and not force_refresh:
            return self._metadata_cache
        
        try:
            file_info = self.get_file_info()
            estimated_count = await self.estimate_record_count()
            
            # 샘플 레코드로 필드 정보 추출
            sample_fields = set()
            sample_count = 0
            
            async for record in self.stream_records(max_records=10):
                if record:  # 빈 레코드 체크
                    sample_fields.update(record.keys())
                    sample_count += 1
            
            self._metadata_cache = {
                "file_info": file_info,
                "estimated_record_count": estimated_count,
                "sample_record_count": sample_count,
                "available_fields": sorted(list(sample_fields)),
                "last_updated": datetime.now().isoformat()
            }
            
            return self._metadata_cache
            
        except Exception as e:
            logger.error(f"메타데이터 추출 오류: {e}")
            return {
                "file_info": self.get_file_info(),
                "estimated_record_count": 0,
                "available_fields": [],
                "error": str(e)
            }
    
    async def get_field_samples(self, field_name: str, limit: int = 100) -> List[Any]:
        """특정 필드의 샘플 값들 반환"""
        samples = set()
        
        try:
            async for record in self.stream_records(max_records=limit * 10):
                if field_name in record:
                    value = record[field_name]
                    if value is not None:
                        if isinstance(value, (str, int, float)):
                            samples.add(value)
                        elif isinstance(value, list):
                            for item in value[:5]:  # 배열의 첫 5개만
                                if isinstance(item, (str, int, float)):
                                    samples.add(item)
                        if len(samples) >= limit:
                            break
            
            return sorted(list(samples))
            
        except Exception as e:
            logger.error(f"필드 샘플 추출 오류: {e}")
            return []
    
    def clear_cache(self):
        """캐시 클리어"""
        self._metadata_cache = None
        self._result_cache.clear()
        self.get_file_info.cache_clear()
        gc.collect()

# 전역 프로세서 인스턴스 관리
_processors: Dict[str, LargeFileProcessor] = {}

def get_processor(file_path: Union[str, Path]) -> LargeFileProcessor:
    """프로세서 인스턴스 반환 (싱글톤 패턴)"""
    path_str = str(file_path)
    
    if path_str not in _processors:
        _processors[path_str] = LargeFileProcessor(file_path)
    
    return _processors[path_str]

def clear_all_processors():
    """모든 프로세서 인스턴스 클리어"""
    for processor in _processors.values():
        processor.clear_cache()
    _processors.clear()
    gc.collect()

# 편의 함수들
async def stream_search_large_file(file_path: Union[str, Path], 
                                 keyword: Optional[str] = None,
                                 search_field: str = "all",
                                 filters: Optional[Dict[str, Any]] = None,
                                 limit: Optional[int] = 100,
                                 offset: int = 0) -> Dict[str, Any]:
    """대용량 파일 스트리밍 검색 편의 함수"""
    processor = get_processor(file_path)
    context = SearchContext(
        keyword=keyword,
        search_field=search_field,
        filters=filters,
        limit=limit,
        offset=offset
    )
    return await processor.search_streaming(context)

async def get_large_file_metadata(file_path: Union[str, Path]) -> Dict[str, Any]:
    """대용량 파일 메타데이터 반환 편의 함수"""
    processor = get_processor(file_path)
    return await processor.get_metadata()
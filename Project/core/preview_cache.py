"""
대용량 파일용 프리뷰 캐시 시스템
첫 페이지 빠른 로딩과 인덱스 파일 생성/관리
"""

import json
import time
import asyncio
import aiofiles
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging
import hashlib
import gzip
import pickle

logger = logging.getLogger(__name__)

@dataclass
class PreviewCache:
    """프리뷰 캐시 데이터 구조"""
    file_path: str
    file_hash: str
    created_at: datetime
    updated_at: datetime
    total_records: int
    sample_records: List[Dict[str, Any]]  # 첫 페이지용 샘플 데이터
    field_info: Dict[str, Any]  # 필드 정보
    search_index: Optional[Dict[str, List[int]]] = None  # 검색 인덱스 (키워드 -> 레코드 위치)

@dataclass
class IndexEntry:
    """인덱스 엔트리"""
    position: int  # 파일 내 위치
    record_id: int  # 레코드 ID
    keywords: List[str]  # 검색 키워드들
    key_fields: Dict[str, Any]  # 주요 필드 값들

class PreviewCacheManager:
    """프리뷰 캐시 관리자"""
    
    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path(__file__).parent.parent / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        self.index_dir = self.cache_dir / "indexes"
        self.index_dir.mkdir(exist_ok=True)
        
        # 캐시 설정
        self.preview_size = 100  # 프리뷰용 레코드 수
        self.index_chunk_size = 1000  # 인덱스 청크 크기
        self.cache_ttl = timedelta(hours=24)  # 캐시 유효 시간
    
    def _get_file_hash(self, file_path: Path) -> str:
        """파일 해시 계산 (수정 시간 + 크기 기반)"""
        stat = file_path.stat()
        content = f"{file_path.name}:{stat.st_size}:{stat.st_mtime}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _get_cache_path(self, file_path: Path) -> Path:
        """캐시 파일 경로 생성"""
        file_hash = self._get_file_hash(file_path)
        return self.cache_dir / f"{file_path.stem}_{file_hash}.cache"
    
    def _get_index_path(self, file_path: Path) -> Path:
        """인덱스 파일 경로 생성"""
        file_hash = self._get_file_hash(file_path)
        return self.index_dir / f"{file_path.stem}_{file_hash}.idx"
    
    async def get_preview_cache(self, file_path: Path) -> Optional[PreviewCache]:
        """프리뷰 캐시 조회"""
        cache_path = self._get_cache_path(file_path)
        
        if not cache_path.exists():
            return None
        
        try:
            async with aiofiles.open(cache_path, 'rb') as f:
                content = await f.read()
                
            # gzip 압축 해제 후 pickle 로드
            decompressed = gzip.decompress(content)
            cache_data = pickle.loads(decompressed)
            
            # TTL 검사
            cache = PreviewCache(**cache_data)
            if datetime.now() - cache.updated_at > self.cache_ttl:
                logger.info(f"캐시 만료: {file_path.name}")
                cache_path.unlink()  # 만료된 캐시 삭제
                return None
            
            # 파일 변경 검사
            current_hash = self._get_file_hash(file_path)
            if cache.file_hash != current_hash:
                logger.info(f"파일 변경 감지: {file_path.name}")
                cache_path.unlink()  # 변경된 파일 캐시 삭제
                return None
            
            logger.info(f"캐시 적중: {file_path.name} ({cache.total_records:,}개 레코드)")
            return cache
            
        except Exception as e:
            logger.warning(f"캐시 로드 실패: {e}")
            if cache_path.exists():
                cache_path.unlink()
            return None
    
    async def create_preview_cache(self, file_path: Path, 
                                 progress_callback: Optional[callable] = None) -> PreviewCache:
        """프리뷰 캐시 생성"""
        logger.info(f"캐시 생성 시작: {file_path.name}")
        start_time = time.time()
        
        try:
            # 대용량 파일 처리기를 사용하여 샘플 데이터 추출
            from .large_file_processor import get_processor
            
            processor = get_processor(file_path)
            
            # 메타데이터 수집
            metadata = await processor.get_metadata()
            
            # 샘플 레코드 수집
            sample_records = []
            field_stats = {}
            record_count = 0
            
            # 프로그레스 콜백 설정
            def update_progress(current, total):
                if progress_callback:
                    progress_callback(f"캐시 생성 중... {current:,}/{total:,}", 
                                    int((current / max(total, 1)) * 100))
            
            async for record in processor.stream_records(
                max_records=self.preview_size * 2,  # 여유분 확보
                progress_callback=update_progress
            ):
                if len(sample_records) < self.preview_size:
                    sample_records.append(record)
                
                # 필드 통계 수집
                for key, value in record.items():
                    if key not in field_stats:
                        field_stats[key] = {
                            'type': type(value).__name__,
                            'samples': set(),
                            'null_count': 0
                        }
                    
                    if value is None or value == "":
                        field_stats[key]['null_count'] += 1
                    else:
                        # 샘플 값 저장 (최대 10개)
                        if len(field_stats[key]['samples']) < 10:
                            field_stats[key]['samples'].add(str(value)[:100])
                
                record_count += 1
                
                # 메모리 절약을 위한 주기적 정리
                if record_count % 1000 == 0:
                    await asyncio.sleep(0)
            
            # 필드 정보 정리
            field_info = {}
            for key, stats in field_stats.items():
                field_info[key] = {
                    'type': stats['type'],
                    'samples': list(stats['samples']),
                    'null_count': stats['null_count'],
                    'null_ratio': stats['null_count'] / max(record_count, 1)
                }
            
            # 캐시 객체 생성
            file_hash = self._get_file_hash(file_path)
            cache = PreviewCache(
                file_path=str(file_path),
                file_hash=file_hash,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                total_records=metadata.get('estimated_record_count', record_count),
                sample_records=sample_records,
                field_info=field_info
            )
            
            # 캐시 저장
            await self._save_cache(cache)
            
            elapsed_time = time.time() - start_time
            logger.info(f"캐시 생성 완료: {file_path.name} "
                       f"({len(sample_records):,}개 샘플, {elapsed_time:.2f}초)")
            
            return cache
            
        except Exception as e:
            logger.error(f"캐시 생성 실패: {file_path.name} - {e}")
            raise
    
    async def _save_cache(self, cache: PreviewCache):
        """캐시 저장"""
        cache_path = self._get_cache_path(Path(cache.file_path))
        
        try:
            # pickle 직렬화 후 gzip 압축
            cache_data = asdict(cache)
            pickled_data = pickle.dumps(cache_data)
            compressed_data = gzip.compress(pickled_data)
            
            async with aiofiles.open(cache_path, 'wb') as f:
                await f.write(compressed_data)
            
            logger.info(f"캐시 저장 완료: {cache_path.name} "
                       f"({len(compressed_data):,} bytes)")
            
        except Exception as e:
            logger.error(f"캐시 저장 실패: {e}")
            raise
    
    async def get_or_create_preview(self, file_path: Path, 
                                  progress_callback: Optional[callable] = None) -> PreviewCache:
        """프리뷰 캐시 조회 또는 생성"""
        
        # 기존 캐시 확인
        cache = await self.get_preview_cache(file_path)
        if cache:
            return cache
        
        # 캐시가 없으면 생성
        return await self.create_preview_cache(file_path, progress_callback)
    
    async def create_search_index(self, file_path: Path, 
                                cache: PreviewCache = None,
                                progress_callback: Optional[callable] = None) -> Dict[str, List[int]]:
        """검색 인덱스 생성"""
        
        if cache is None:
            cache = await self.get_preview_cache(file_path)
            if not cache:
                cache = await self.create_preview_cache(file_path, progress_callback)
        
        index_path = self._get_index_path(file_path)
        
        # 기존 인덱스 확인
        if index_path.exists():
            try:
                async with aiofiles.open(index_path, 'rb') as f:
                    content = await f.read()
                decompressed = gzip.decompress(content)
                search_index = pickle.loads(decompressed)
                
                logger.info(f"검색 인덱스 로드: {index_path.name}")
                return search_index
                
            except Exception as e:
                logger.warning(f"기존 인덱스 로드 실패: {e}")
        
        # 새 인덱스 생성
        logger.info(f"검색 인덱스 생성 시작: {file_path.name}")
        start_time = time.time()
        
        search_index = {}
        
        try:
            from .large_file_processor import get_processor
            processor = get_processor(file_path)
            
            record_position = 0
            
            async for record in processor.stream_records():
                # 검색 가능한 키워드 추출
                keywords = self._extract_keywords(record)
                
                # 각 키워드별로 레코드 위치 인덱싱
                for keyword in keywords:
                    keyword_lower = keyword.lower()
                    if keyword_lower not in search_index:
                        search_index[keyword_lower] = []
                    search_index[keyword_lower].append(record_position)
                
                record_position += 1
                
                # 진행률 업데이트
                if progress_callback and record_position % 1000 == 0:
                    progress_callback(f"인덱스 생성 중... {record_position:,}개 처리", 
                                    min(50 + int(record_position / 1000), 90))
                
                # 메모리 관리
                if record_position % 10000 == 0:
                    await asyncio.sleep(0)
            
            # 인덱스 저장
            pickled_data = pickle.dumps(search_index)
            compressed_data = gzip.compress(pickled_data)
            
            async with aiofiles.open(index_path, 'wb') as f:
                await f.write(compressed_data)
            
            elapsed_time = time.time() - start_time
            logger.info(f"검색 인덱스 생성 완료: {file_path.name} "
                       f"({len(search_index):,}개 키워드, {elapsed_time:.2f}초)")
            
            return search_index
            
        except Exception as e:
            logger.error(f"검색 인덱스 생성 실패: {e}")
            raise
    
    def _extract_keywords(self, record: Dict[str, Any]) -> List[str]:
        """레코드에서 검색 키워드 추출"""
        keywords = []
        
        # 주요 검색 필드들
        search_fields = [
            'productName', 'makerName', 'importerName', 'modelName', 
            'certNum', 'brandName', 'categoryName'
        ]
        
        for field in search_fields:
            value = record.get(field)
            if value and isinstance(value, str):
                # 값 자체를 키워드로 추가
                keywords.append(value.strip())
                
                # 공백으로 구분된 단어들도 추가
                words = value.split()
                keywords.extend([word.strip() for word in words if len(word.strip()) > 1])
        
        return list(set(keywords))  # 중복 제거
    
    async def cleanup_old_caches(self, max_age_hours: int = 168):  # 7일
        """오래된 캐시 정리"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        cleaned_count = 0
        
        try:
            for cache_file in self.cache_dir.glob("*.cache"):
                if cache_file.stat().st_mtime < cutoff_time.timestamp():
                    cache_file.unlink()
                    cleaned_count += 1
            
            for index_file in self.index_dir.glob("*.idx"):
                if index_file.stat().st_mtime < cutoff_time.timestamp():
                    index_file.unlink()
                    cleaned_count += 1
            
            if cleaned_count > 0:
                logger.info(f"오래된 캐시 {cleaned_count}개 정리 완료")
                
        except Exception as e:
            logger.warning(f"캐시 정리 중 오류: {e}")

# 전역 캐시 매니저 인스턴스
preview_cache_manager = PreviewCacheManager()
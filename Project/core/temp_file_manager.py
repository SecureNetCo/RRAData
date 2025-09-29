"""
Vercel 서버리스 환경의 임시 파일 관리 모듈
512MB /tmp 제한 대응
"""

import os
import uuid
import time
import json
import asyncio
from typing import Dict, Optional, List
from pathlib import Path
from datetime import datetime, timedelta
import shutil

class TempFileManager:
    """임시 파일 관리자"""
    
    def __init__(self, base_dir: str = "/tmp/datapage"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # 활성 파일 추적
        self.active_files: Dict[str, Dict] = {}
        self.max_file_age = 3600  # 1시간
        self.max_total_size = 400 * 1024 * 1024  # 400MB (여유분 확보)
        
        # 메타데이터 파일
        self.metadata_file = self.base_dir / "metadata.json"
        self._load_metadata()
    
    def _load_metadata(self):
        """메타데이터 로드"""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    self.active_files = json.load(f)
        except Exception as e:
            print(f"메타데이터 로드 실패: {e}")
            self.active_files = {}
    
    def _save_metadata(self):
        """메타데이터 저장"""
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.active_files, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"메타데이터 저장 실패: {e}")
    
    def generate_temp_id(self, user_session: str = None) -> str:
        """임시 파일 ID 생성"""
        temp_id = str(uuid.uuid4())
        
        # 사용자별 격리를 위한 세션 정보 포함
        if user_session:
            temp_id = f"{user_session}_{temp_id}"
        
        return temp_id
    
    def create_temp_file(self, temp_id: str, file_type: str = "xlsx") -> Path:
        """임시 파일 생성"""
        # 기존 파일 정리
        self.cleanup_old_files()
        
        # 용량 체크
        if not self._check_space_available():
            self.emergency_cleanup()
        
        # 파일 경로 생성
        filename = f"{temp_id}.{file_type}"
        file_path = self.base_dir / filename
        
        # 메타데이터 추가
        self.active_files[temp_id] = {
            "file_path": str(file_path),
            "created_at": datetime.now().isoformat(),
            "file_type": file_type,
            "status": "creating",
            "size": 0
        }
        
        self._save_metadata()
        return file_path
    
    def update_file_status(self, temp_id: str, status: str, size: int = None, progress: int = None, message: str = None, processed_count: int = None, total_count: int = None):
        """파일 상태 및 진행률 업데이트"""
        if temp_id in self.active_files:
            self.active_files[temp_id]["status"] = status
            if size is not None:
                self.active_files[temp_id]["size"] = size
            if progress is not None:
                self.active_files[temp_id]["progress"] = progress
            if message is not None:
                self.active_files[temp_id]["message"] = message
            if processed_count is not None:
                self.active_files[temp_id]["processed_count"] = processed_count
            if total_count is not None:
                self.active_files[temp_id]["total_count"] = total_count
                
            # 예상 남은 시간 계산 (처리 속도 기반)
            if processed_count is not None and total_count is not None and processed_count > 0:
                elapsed_time = (datetime.now() - datetime.fromisoformat(self.active_files[temp_id]["created_at"])).total_seconds()
                processing_rate = processed_count / elapsed_time if elapsed_time > 0 else 0
                if processing_rate > 0 and processed_count < total_count:
                    remaining_records = total_count - processed_count
                    estimated_time_remaining = int(remaining_records / processing_rate)
                    self.active_files[temp_id]["estimated_time_remaining"] = estimated_time_remaining
                    
            self.active_files[temp_id]["updated_at"] = datetime.now().isoformat()
            self._save_metadata()
    
    def get_file_info(self, temp_id: str) -> Optional[Dict]:
        """파일 정보 조회"""
        return self.active_files.get(temp_id)
    
    def get_file_path(self, temp_id: str) -> Optional[Path]:
        """파일 경로 조회"""
        file_info = self.get_file_info(temp_id)
        if file_info:
            path = Path(file_info["file_path"])
            if path.exists():
                return path
        return None
    
    def delete_temp_file(self, temp_id: str) -> bool:
        """임시 파일 삭제"""
        try:
            file_info = self.active_files.get(temp_id)
            if file_info:
                file_path = Path(file_info["file_path"])
                if file_path.exists():
                    file_path.unlink()
                
                # 메타데이터에서 제거
                del self.active_files[temp_id]
                self._save_metadata()
                
                print(f"임시 파일 삭제됨: {temp_id}")
                return True
        except Exception as e:
            print(f"파일 삭제 실패 {temp_id}: {e}")
        
        return False
    
    def cleanup_old_files(self):
        """오래된 파일 정리"""
        current_time = datetime.now()
        expired_files = []
        
        for temp_id, file_info in self.active_files.items():
            try:
                created_at = datetime.fromisoformat(file_info["created_at"])
                age = (current_time - created_at).total_seconds()
                
                if age > self.max_file_age:
                    expired_files.append(temp_id)
            except Exception as e:
                print(f"파일 나이 계산 실패 {temp_id}: {e}")
                expired_files.append(temp_id)
        
        # 만료된 파일들 삭제
        for temp_id in expired_files:
            self.delete_temp_file(temp_id)
        
        if expired_files:
            print(f"만료된 파일 {len(expired_files)}개 정리 완료")
    
    def emergency_cleanup(self):
        """긴급 정리 (용량 부족시)"""
        print("긴급 정리 시작...")
        
        # 상태별 우선순위로 정리
        # 1. 실패한 파일들
        failed_files = [
            temp_id for temp_id, info in self.active_files.items()
            if info.get("status") == "failed"
        ]
        
        for temp_id in failed_files:
            self.delete_temp_file(temp_id)
        
        # 2. 오래된 파일부터 정리
        sorted_files = sorted(
            self.active_files.items(),
            key=lambda x: x[1].get("created_at", "")
        )
        
        current_size = self._get_total_size()
        target_size = self.max_total_size * 0.7  # 70%까지 정리
        
        for temp_id, file_info in sorted_files:
            if current_size <= target_size:
                break
            
            file_size = file_info.get("size", 0)
            if self.delete_temp_file(temp_id):
                current_size -= file_size
        
        print(f"긴급 정리 완료. 현재 사용량: {current_size / 1024 / 1024:.1f}MB")
    
    def _check_space_available(self, required_size: int = 50 * 1024 * 1024) -> bool:
        """사용 가능한 공간 체크 (기본 50MB 여유분)"""
        current_size = self._get_total_size()
        return (current_size + required_size) < self.max_total_size
    
    def _get_total_size(self) -> int:
        """현재 총 사용량 계산"""
        total_size = 0
        
        for file_info in self.active_files.values():
            file_path = Path(file_info["file_path"])
            if file_path.exists():
                total_size += file_path.stat().st_size
        
        return total_size
    
    def get_statistics(self) -> Dict:
        """사용량 통계"""
        total_size = self._get_total_size()
        file_count = len(self.active_files)
        
        status_count = {}
        for file_info in self.active_files.values():
            status = file_info.get("status", "unknown")
            status_count[status] = status_count.get(status, 0) + 1
        
        return {
            "total_files": file_count,
            "total_size_mb": round(total_size / 1024 / 1024, 2),
            "usage_percent": round((total_size / self.max_total_size) * 100, 1),
            "max_size_mb": round(self.max_total_size / 1024 / 1024, 2),
            "status_distribution": status_count,
            "space_available": self._check_space_available()
        }
    
    async def scheduled_cleanup(self):
        """주기적 정리 (백그라운드 태스크)"""
        while True:
            try:
                self.cleanup_old_files()
                
                # 5분마다 실행
                await asyncio.sleep(300)
            except Exception as e:
                print(f"주기적 정리 오류: {e}")
                await asyncio.sleep(60)  # 오류시 1분 후 재시도

# 전역 인스턴스
temp_file_manager = TempFileManager()
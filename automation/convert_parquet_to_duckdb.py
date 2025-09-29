#!/usr/bin/env python3
"""Parquet → DuckDB 변환 스크립트.

`Project/parquet` 디렉토리 이하의 모든 `.parquet` 파일을 찾아 동일한
상대 경로 구조로 `Project/duckdb` 디렉토리에 `.duckdb` 파일로 변환합니다.
각 DuckDB 파일에는 원본 파일명을 테이블 이름으로 사용한 단일 테이블이 생성됩니다.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ModuleNotFoundError as exc:
    print("[오류] duckdb 파이썬 모듈을 찾을 수 없습니다. `pip install duckdb`로 설치해주세요.")
    raise SystemExit(1) from exc


def escape_identifier(identifier: str) -> str:
    """DuckDB 식별자 이스케이프 (double quote wrapping)."""
    return '"' + identifier.replace('"', '""') + '"'

REPO_ROOT = Path(__file__).resolve().parents[1]
PARQUET_ROOT = REPO_ROOT / "Project" / "parquet"
DUCKDB_ROOT = REPO_ROOT / "Project" / "duckdb"


def discover_parquet_files(source_dir: Path) -> list[Path]:
    """Return every `.parquet` file under ``source_dir`` (sorted for determinism)."""
    return sorted(source_dir.rglob("*.parquet"))


def ensure_directory(path: Path) -> None:
    """Create ``path`` if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def to_duckdb_path(parquet_path: Path, source_dir: Path, target_dir: Path) -> Path:
    """Map a parquet path to its DuckDB output path while mirroring subdirectories."""
    relative = parquet_path.relative_to(source_dir)
    return target_dir / relative.with_suffix(".duckdb")


def materialize_duckdb(parquet_path: Path, duckdb_path: Path) -> None:
    """Create a DuckDB database containing the parquet contents as a single table."""
    ensure_directory(duckdb_path.parent)

    if duckdb_path.exists():
        duckdb_path.unlink()

    table_identifier = escape_identifier(parquet_path.stem)

    with duckdb.connect(str(duckdb_path)) as conn:
        conn.execute(
            f"CREATE TABLE {table_identifier} AS SELECT * FROM read_parquet(?)",
            [str(parquet_path)],
        )



def convert_all(parquet_root: Path, duckdb_root: Path) -> None:
    parquet_files = discover_parquet_files(parquet_root)

    if not parquet_files:
        print(f"[경고] 변환할 parquet 파일이 없습니다: {parquet_root}")
        return

    ensure_directory(duckdb_root)

    for parquet_path in parquet_files:
        duckdb_path = to_duckdb_path(parquet_path, parquet_root, duckdb_root)
        print(f"[변환] {parquet_path.relative_to(parquet_root)} → {duckdb_path.relative_to(duckdb_root)}")
        materialize_duckdb(parquet_path, duckdb_path)

    print(f"[완료] 총 {len(parquet_files)}개 파일 변환")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert every parquet file to an individual DuckDB database.")
    parser.add_argument(
        "--parquet-root",
        type=Path,
        default=PARQUET_ROOT,
        help="입력 parquet 디렉토리 (기본: Project/parquet)",
    )
    parser.add_argument(
        "--duckdb-root",
        type=Path,
        default=DUCKDB_ROOT,
        help="출력 DuckDB 디렉토리 (기본: Project/duckdb)",
    )

    args = parser.parse_args(argv)

    if not args.parquet_root.exists():
        parser.error(f"입력 경로가 존재하지 않습니다: {args.parquet_root}")

    convert_all(args.parquet_root, args.duckdb_root)
    return 0


if __name__ == "__main__":
    sys.exit(main())

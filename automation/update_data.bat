@echo off
REM 데이터 업데이트 간편 실행 스크립트 (Windows용)

echo DataPage 데이터 자동 업데이트 시작...
echo 현재 위치: %cd%
echo =

REM automation 폴더로 이동 후 Python 스크립트 실행
cd /d "%~dp0"

python --version >nul 2>&1
if %errorlevel% == 0 (
    python auto_blob_update.py
) else (
    echo [오류] Python이 설치되지 않았거나 PATH에 없습니다.
    echo Python을 설치하고 PATH에 추가한 후 다시 실행해주세요.
    pause
    exit /b 1
)

echo.
echo 데이터 업데이트 완료!
echo 새로운 데이터가 곧 서비스에 반영됩니다.
pause
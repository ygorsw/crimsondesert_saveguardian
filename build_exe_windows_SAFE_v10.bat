@echo off
setlocal EnableExtensions
cd /d "%~dp0"

title Build EXE - Crimson Desert Save Guardian SAFE V10.1

set "SCRIPT=%~dp0crimson_desert_save_guardian_safe_v10_1.py"
set "DIST=%~dp0"
set "WORK=%~dp0_build_safe_v10_1"
set "SPEC=%~dp0_spec_safe_v10_1"
set "NAME=CrimsonDesertSaveGuardian_SAFE_v10_1"

echo ============================================
echo SCRIPT = [%SCRIPT%]
echo DIST   = [%DIST%]
echo WORK   = [%WORK%]
echo SPEC   = [%SPEC%]
echo ============================================

if not exist "%SCRIPT%" (
    echo ERRO: arquivo Python nao encontrado:
    echo %SCRIPT%
    pause
    exit /b 1
)

if exist "%WORK%" rmdir /s /q "%WORK%"
if exist "%SPEC%" rmdir /s /q "%SPEC%"
if exist "%DIST%%NAME%.exe" del /f /q "%DIST%%NAME%.exe"

echo.
echo Instalando dependencias...
python -m pip install --upgrade pip
python -m pip install customtkinter pyinstaller pystray pillow

echo.
echo Gerando EXE SAFE V10.1...
python -m PyInstaller --noconfirm --onefile --windowed --name="%NAME%" --distpath="%DIST%" --workpath="%WORK%" --specpath="%SPEC%" --hidden-import=pystray --hidden-import=PIL --hidden-import=PIL.Image --hidden-import=PIL.ImageDraw --collect-all pystray --collect-all PIL "%SCRIPT%"

echo.
if exist "%DIST%%NAME%.exe" (
    echo SUCESSO: "%DIST%%NAME%.exe"
) else (
    echo FALHA: EXE nao foi criado.
)
pause
endlocal

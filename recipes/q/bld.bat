@echo off
set qlib=%PREFIX%\Lib\q
mkdir %qlib%
mkdir %PREFIX%\Scripts
xcopy /E /I /Y %SRC_DIR%\* %qlib%

echo ^
set QHOME=%PREFIX%\Lib\q^

%%QHOME%%\w32\q.exe %%*> %PREFIX%\Scripts\q.bat

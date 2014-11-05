@echo off

set qlib=%PREFIX%\Lib\q
mkdir %qlib%
xcopy /E /I /Y %SRC_DIR%\* %qlib%

> %PREFIX%\Scripts\q.bat (
    @echo set QHOME=/opt/anaconda1anaconda2anaconda3/Lib/q
    @echo %%QHOME%%\w32\q %*
)

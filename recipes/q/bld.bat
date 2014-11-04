@echo off
xcopy /E /I /Y %SRC_DIR% %SCRIPTS%\Q
echo set QHOME=%%ANACONDA_ENVS%%\%%CONDA_DEFAULT_ENV%%\Scripts\Q> %SCRIPTS%\q.bat
echo %%ANACONDA_ENVS%%\%%CONDA_DEFAULT_ENV%%\Scripts\Q\w32\q.exe>> %SCRIPTS%\q.bat

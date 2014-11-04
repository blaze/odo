@echo off
xcopy /E /I /Y %SRC_DIR% %SCRIPTS%\q
echo set QHOME=%%ANACONDA_ENVS%%\%%CONDA_DEFAULT_ENV%%\Scripts\q> %SCRIPTS%\q.bat
echo %%ANACONDA_ENVS%%\%%CONDA_DEFAULT_ENV%%\Scripts\q\w32\q.exe>> %SCRIPTS%\q.bat

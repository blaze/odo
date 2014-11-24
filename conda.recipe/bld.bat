@echo off

SET BLD_DIR=%CD%
cd /D "%RECIPE_DIR%\.."
FOR /F "delims=" %%i IN ('git describe --tags') DO set BLAZE_VERSION=%%i
echo %BLAZE_VERSION% | %PYTHON% %SRC_DIR%\conda.recipe\version.py > tmp
set result=<tmp

del tmp
echo %result%>__conda_version__.txt
"%PYTHON%" setup.py install

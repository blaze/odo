@echo off

SET BLD_DIR=%CD%
cd /D "%RECIPE_DIR%\.."
%PYTHON% setup.py install
cd /D %BLD_DIR%

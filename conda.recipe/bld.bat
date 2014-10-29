
SET BLD_DIR=%CD%
cd /D "%RECIPE_DIR%\.."

"%PYTHON%" setup.py install

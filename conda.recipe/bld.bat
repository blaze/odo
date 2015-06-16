@echo off

conda remove markupsafe --yes
conda install --force markupsafe --yes
%PYTHON% setup.py --quiet install

#!/bin/bash
conda create -n kdbpy_test --file requirements.txt --yes
source activate kdbpy_test
pip install -r piprequirements.txt
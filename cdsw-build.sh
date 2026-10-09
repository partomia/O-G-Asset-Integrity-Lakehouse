#!/bin/bash
# Runs during every Cloudera AI model build: the model (serve/predict.py) needs only the slim
# serving set; the jobs install requirements.txt themselves (job ogx-setup-data).
pip3 install -r requirements-model.txt

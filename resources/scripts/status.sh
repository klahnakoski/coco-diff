#!/usr/bin/env bash


export PYTHONPATH=.:vendor
cd ~/coco-diff
python ./coco/status.py  --settings=./resources/config/status.json >& /dev/null < /dev/null

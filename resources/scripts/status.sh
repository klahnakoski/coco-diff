#!/usr/bin/env bash


export PYTHONPATH=.
cd ~/coco-diff
python ./coco/status.py  >& /dev/null < /dev/null

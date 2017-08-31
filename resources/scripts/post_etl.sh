#!/usr/bin/env bash


export PYTHONPATH=.
cd ~/coco-diff
python ./coco/post_etl.py --settings=./resources/config/post_etl.json

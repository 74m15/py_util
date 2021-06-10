#!/usr/bin/env sh

export PYTHONPATH=../src
export CONFIGPATH=../conf

python3 -m util.batch -b util.task.TaskManager -c ../conf/config.json -l ../conf/logging.conf $*

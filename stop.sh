#!/bin/bash

keyword="java"

pids=$(ps aux | grep $keyword | grep -v grep | awk '{print $2}')

for pid in $pids; do
    kill -9 $pid
done

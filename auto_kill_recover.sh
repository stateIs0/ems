#!/bin/bash
date



path=$1

pid=$(ps aux | grep java | grep $path | grep -v grep | awk '{print $2}')

echo "pid $pid"

if [ -n "$pid" ]; then
  echo "正在杀死进程，PID: $pid"
  kill -9 "$pid"
  echo "已杀死进程，PID: $pid"
else
  echo "未找到匹配的进程，无需杀死"
  exit 1
fi

kill -9 $pid

echo "kill $pid $path"

echo "开始休眠1..."
sleep 3

echo "结束休眠1.，，."

current=$(date +%s)

cd /Users/cxs/ty60/simple-stream

current=$(date +%s)

echo $current-application.log

java -jar $path > $current-application.log 2>&1 &

echo "启动完毕....." $path $current-application.log


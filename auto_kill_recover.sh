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

echo "开始休眠..."
current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
echo  $current_datetime

sleep 10

current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
echo  $current_datetime
echo "结束休眠.，，."

current=$(date +%s)



# 检查系统类型
if [ "$os_type" == "Linux" ]; then
    echo "This is Linux."
    cd /root/ems
elif [ "$os_type" == "Darwin" ]; then
    echo "This is macOS."
    cd ~/ty60/ems
else
    echo "This is " $os_type
    cd /root/ems
fi


current=$(date +%s)

echo $current-application.log

java -jar $path > $current-application.log 2>&1 &

echo "启动完毕....." $path $current-application.log

current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
echo  $current_datetime
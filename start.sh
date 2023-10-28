#!/bin/bash
date
current=$(date +%s)

path=$1
os_name=$(uname -s)

# 判断操作系统类型
if [ "$os_name" = "Linux" ]; then
  cd /root/ems/ems
  echo "这是Linux操作系统"
elif [ "$os_name" = "Darwin" ]; then
  cd /Users/cxs/ty60/simple-stream
  echo "这是macOS操作系统"
else
  echo "未知操作系统"
fi


nohup java -jar -Xmx2G -Xms2G -Dems.test.env=true  $path > $current-application.log 2>&1  &
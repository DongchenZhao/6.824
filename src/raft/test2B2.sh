#!/bin/bash

# 清空目标目录
rm -rf /home/zdc/test2B/*
mkdir -p /home/zdc/test2B

# 运行测试20次
for run in {1..100}; do
    echo "start test No.${run}"
    timestamp=$(date +%s)
    log_file="/home/zdc/test2B/${run}-${timestamp}.log"
    go test -race -run TestBackup2B -trace trace"${run}".out > "$log_file" 2>&1

    # 检查结果是否包含FAIL
    tail -n 10 "$log_file" | grep -q "FAIL"
    if [ $? -eq 0 ]; then
        mv "$log_file" "/home/zdc/test2B/${run}-${timestamp}-fail.log"
    fi
done

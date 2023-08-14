go build -race -buildmode=plugin ../mrapps/wc.go

rm mr-*

workerCnt=11

# go run -race mrsequential.go wc.so pg-*.txt
go run -race mrcoordinator.go pg-*.txt &
# 循环启动workerCnt个进程
for ((i=0; i<workerCnt; i++)); do
    # 在后台执行go run -race mrworker.go wc.so命令
    go run -race mrworker.go wc.so &
done

# 等待所有后台进程完成
wait

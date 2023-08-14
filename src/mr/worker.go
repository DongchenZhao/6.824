package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerStruct struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	taskId  int
	nReduce int
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Printf("Worker start\n")
	WorkerReq("task", []string{}, mapf, reducef)
}

func resHandler(res *MsgRes, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Printf("Coordinator responses: %v\n", res)
	if res.Type == "wait" {
		// 等待1秒，继续请求
		time.Sleep(time.Second)
		WorkerReq("task", []string{}, mapf, reducef)
	} else if res.Type == "mapTask" {
		// 执行map任务
		taskId, _ := strconv.Atoi(res.Content[0])
		nReduce, _ := strconv.Atoi(res.Content[1])
		filename := res.Content[2]
		log.Printf("mapTask: %v, %v, %v\n", taskId, nReduce, filename)
		mapTask(filename, taskId, nReduce, mapf)
		// map任务完成后，向coordinator发送mapFinish请求
		WorkerReq("mapFinish", []string{strconv.Itoa(taskId)}, mapf, reducef)
	} else if res.Type == "reduceTask" {
		taskId, _ := strconv.Atoi(res.Content[0])
		reduceTask(taskId, reducef)
		// reduce任务完成后，向coordinator发送reduceFinish请求
		WorkerReq("reduceFinish", []string{strconv.Itoa(taskId)}, mapf, reducef)
	} else if res.Type == "ok" {
		//继续申请任务
		WorkerReq("task", []string{}, mapf, reducef)
	} else {
		// 未知响应，打印MsgRes
		log.Printf("unknown response type: %v\n", res.Type)
	}
}

func WorkerReq(msgType string, msgContent []string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Printf("WorkerReq: %v, %v\n", msgType, msgContent)
	req := MsgReq{}
	req.Type = msgType
	req.Content = msgContent
	res := MsgRes{}
	ok := call("Coordinator.ReqHandler", &req, &res)
	if ok { // 处理响应
		resHandler(&res, mapf, reducef)
	} else {
		log.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func mapTask(filename string, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := []KeyValue{}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// 将中间结果写入文件，每行都是json格式的KeyValue
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", taskId, i)
		//如果存在oname文件，删除
		if _, err := os.Stat(oname); err == nil {
			os.Remove(oname)
		}
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate {
			if ihash(kv.Key)%nReduce == i {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v, filename %v", kv, oname)
				}
			}
		}
		ofile.Close()
	}
}

func reduceTask(taskId int, reducef func(string, []string) string) {
	// 读取mr-X-Y, X为taskId，Y为reduceId,读取所有Y和taskId相同的文件，将所有文件的内容读取到内存中
	// 将所有内容解析成KeyValue
	// 按照key排序

	// 读取文件
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatalf("cannot read dir")
	}
	var intermediate []KeyValue
	pattern := regexp.MustCompile("^mr-\\d+-\\d+$")
	for _, file := range files {
		// 判断文件格式是否为mr-X-Y,其中X和Y为数字
		if pattern.MatchString(file.Name()) {
			//task, _ := strconv.Atoi(file.Name()[3:4])
			reduce, _ := strconv.Atoi(file.Name()[5:])
			if reduce == taskId {
				// log.Printf("reduce task %v read file: %v\n", taskId, file.Name())
				f, err := os.Open(file.Name())
				if err != nil {
					log.Fatalf("cannot open %v", file.Name())
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				f.Close()
			}
		}
	}
	// 排序
	sort.Sort(ByKey(intermediate))

	// 将所有key相同的value放到一个数组中
	var values []string
	var lastKey string
	for _, kv := range intermediate {
		if kv.Key == lastKey {
			values = append(values, kv.Value)
		} else {
			if lastKey != "" {
				// 调用reducef函数
				output := reducef(lastKey, values)
				// 将结果写入文件
				oname := fmt.Sprintf("mr-out-%v-tmp", taskId)
				ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				fmt.Fprintf(ofile, "%v %v\n", lastKey, output)
				ofile.Close()
			}
			values = []string{kv.Value}
			lastKey = kv.Key
		}
	}
	if len(values) != 0 {
		// 调用reducef函数
		output := reducef(lastKey, values)
		// 将结果写入文件
		oname := fmt.Sprintf("mr-out-%v-tmp", taskId)
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		fmt.Fprintf(ofile, "%v %v\n", lastKey, output)
		ofile.Close()
	}

	//如果存在旧文件，删除
	filename := fmt.Sprintf("mr-out-%v", taskId)
	if _, err := os.Stat(filename); err == nil {
		log.Printf("reduce task %v delete file: %v\n", taskId, filename)
		os.Remove(filename)
	}

	//重命名-tmp文件
	oname := fmt.Sprintf("mr-out-%v-tmp", taskId)
	os.Rename(oname, filename)
}

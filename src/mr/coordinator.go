package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus struct {
	started   bool  // 数组下标为任务id，值为true表示已开始
	finished  bool  // 数组下标为任务id，值为true表示已完成
	startTime int64 // time.Now().Unix() 0表示未开始
}

type Coordinator struct {
	mapTaskStatus        []TaskStatus //下标为map任务id
	reduceTaskStatus     []TaskStatus //下标为reduce任务id
	mapTaskStatusLock    sync.Mutex
	reduceTaskStatusLock sync.Mutex
	files                []string
	nReduce              int
}

func (c *Coordinator) ReqHandler(req *MsgReq, res *MsgRes) error {
	if req.Type == "task" { // 申请任务，根据当前完成进度分配map或reduce任务
		if !c.allMapTaskDone() {
			//申请map任务
			taskId := c.startAMapTask()
			if taskId == -1 { // 没有申请到（全部任务都已开始且未超时），让worker继续等待
				res.Type = "wait"
			} else { // 申请到了map任务，返回任务id和文件名
				res.Type = "mapTask"
				res.Content = []string{strconv.Itoa(taskId), strconv.Itoa(c.nReduce), c.files[taskId]}
			}
		} else { //申请reduce任务
			taskId := c.startAReduceTask()
			if taskId == -1 { // 没有申请到（全部任务都已开始且未超时），让worker继续等待
				res.Type = "wait"
			} else { // 申请到了reduce任务，返回任务id和文件名
				res.Type = "reduceTask"
				res.Content = []string{strconv.Itoa(taskId)}
			}
		}
	} else if req.Type == "mapFinish" {
		taskId, _ := strconv.Atoi(req.Content[0])
		c.mapTaskStatusLock.Lock()
		c.mapTaskStatus[taskId].finished = true
		c.mapTaskStatusLock.Unlock()
		res.Type = "ok"
		res.Content = []string{"mapFinish received"}
		log.Printf("mapFinish: %v\n", taskId)
	} else if req.Type == "reduceFinish" {
		taskId, _ := strconv.Atoi(req.Content[0])
		c.reduceTaskStatusLock.Lock()
		c.reduceTaskStatus[taskId].finished = true
		c.reduceTaskStatusLock.Unlock()
		res.Type = "ok"
		res.Content = []string{"reduceFinish received"}
		log.Printf("reduceFinish: %v\n", taskId)
	} else { // 未知请求，打印MsgRes
		log.Fatalf("unknown request type: %v\n", req.Type)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 如果所有map任务都完成，返回true
func (c *Coordinator) allMapTaskDone() bool {
	c.mapTaskStatusLock.Lock()
	defer c.mapTaskStatusLock.Unlock()
	if c.mapTaskStatus == nil || len(c.mapTaskStatus) == 0 {
		return false
	}
	for i := 0; i < len(c.mapTaskStatus); i++ {
		if !c.mapTaskStatus[i].finished {
			return false
		}
	}
	return true
}

// 如果所有reduce任务都完成，返回true
func (c *Coordinator) allReduceTaskDone() bool {
	c.reduceTaskStatusLock.Lock()
	defer c.reduceTaskStatusLock.Unlock()
	if c.reduceTaskStatus == nil || len(c.reduceTaskStatus) == 0 {
		return false
	}
	for i := 0; i < len(c.reduceTaskStatus); i++ {
		if !c.reduceTaskStatus[i].finished {
			return false
		}
	}
	return true
}

// 返回一个未开始或超时的map任务id，并将这个map任务标记为开始
func (c *Coordinator) startAMapTask() int {
	c.mapTaskStatusLock.Lock()
	defer c.mapTaskStatusLock.Unlock()
	if c.mapTaskStatus == nil || len(c.mapTaskStatus) == 0 {
		return -1
	}
	for i := 0; i < len(c.mapTaskStatus); i++ {
		if !c.mapTaskStatus[i].started || time.Now().Unix()-c.mapTaskStatus[i].startTime > 10 {
			c.mapTaskStatus[i].started = true
			c.mapTaskStatus[i].startTime = time.Now().Unix()
			return i
		}
	}
	return -1
}

// 返回一个未开始或超时的reduce任务id，并将这个reduce任务标记为开始
func (c *Coordinator) startAReduceTask() int {
	c.reduceTaskStatusLock.Lock()
	defer c.reduceTaskStatusLock.Unlock()
	if c.reduceTaskStatus == nil || len(c.reduceTaskStatus) == 0 {
		return -1
	}
	for i := 0; i < len(c.reduceTaskStatus); i++ {
		if !c.reduceTaskStatus[i].started || time.Now().Unix()-c.reduceTaskStatus[i].startTime > 10 {
			c.reduceTaskStatus[i].started = true
			c.reduceTaskStatus[i].startTime = time.Now().Unix()
			return i
		}
	}
	return -1
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.allReduceTaskDone()
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.mapTaskStatusLock = sync.Mutex{}
	c.reduceTaskStatusLock = sync.Mutex{}
	for i := 0; i < len(files); i++ {
		c.mapTaskStatus = append(c.mapTaskStatus, TaskStatus{false, false, 0})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus = append(c.reduceTaskStatus, TaskStatus{false, false, 0})
	}
	c.server()
	return &c
}

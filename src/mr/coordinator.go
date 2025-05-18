package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"time"
)

type Coordinator struct {
	// 任务相关
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMap        int
	
	// 互斥锁
	mu          sync.Mutex
	
	// 任务完成状态
	mapDone     bool
	reduceDone  bool
}

// 请求任务RPC处理函数
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查Map任务是否都已完成
	if !c.mapDone {
		// 分配Map任务
		for i := range c.mapTasks {
			if c.mapTasks[i].State == TaskIdle {
				c.mapTasks[i].State = TaskInProgress
				c.mapTasks[i].StartTime = time.Now().Unix()
				reply.Task = c.mapTasks[i]
				return nil
			}
		}
		// 检查是否有超时的Map任务
		for i := range c.mapTasks {
			if c.mapTasks[i].State == TaskInProgress && 
			   time.Now().Unix() - c.mapTasks[i].StartTime > 10 {
				c.mapTasks[i].StartTime = time.Now().Unix()
				reply.Task = c.mapTasks[i]
				return nil
			}
		}
		// 所有Map任务都在进行中
		reply.Task = Task{TaskType: NoTask}
		return nil
	}

	// Map任务已完成，分配Reduce任务
	if !c.reduceDone {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == TaskIdle {
				c.reduceTasks[i].State = TaskInProgress
				c.reduceTasks[i].StartTime = time.Now().Unix()
				reply.Task = c.reduceTasks[i]
				return nil
			}
		}
		// 检查是否有超时的Reduce任务
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == TaskInProgress && 
			   time.Now().Unix() - c.reduceTasks[i].StartTime > 10 {
				c.reduceTasks[i].StartTime = time.Now().Unix()
				reply.Task = c.reduceTasks[i]
				return nil
			}
		}
		// 所有Reduce任务都在进行中
		reply.Task = Task{TaskType: NoTask}
		return nil
	}

	// 所有任务都已完成
	reply.Task = Task{TaskType: ExitTask}
	return nil
}

// 完成任务RPC处理函数
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if args.TaskId < len(c.mapTasks) {
			c.mapTasks[args.TaskId].State = TaskCompleted
			// 检查是否所有Map任务都已完成
			c.mapDone = true
			for _, task := range c.mapTasks {
				if task.State != TaskCompleted {
					c.mapDone = false
					break
				}
			}
		}
	} else if args.TaskType == ReduceTask {
		if args.TaskId < len(c.reduceTasks) {
			c.reduceTasks[args.TaskId].State = TaskCompleted
			// 检查是否所有Reduce任务都已完成
			c.reduceDone = true
			for _, task := range c.reduceTasks {
				if task.State != TaskCompleted {
					c.reduceDone = false
					break
				}
			}
		}
	}

	reply.Success = true
	return nil
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapDone && c.reduceDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
		mapDone: false,
		reduceDone: false,
	}

	// 初始化Map任务
	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i] = Task{
			TaskType: MapTask,
			TaskId:   i,
			FileName: file,
			NReduce:  nReduce,
			NMap:     len(files),
			State:    TaskIdle,
		}
	}

	// 初始化Reduce任务
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskType: ReduceTask,
			TaskId:   i,
			NReduce:  nReduce,
			NMap:     len(files),
			State:    TaskIdle,
		}
	}

	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

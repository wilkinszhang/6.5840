package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 任务类型
const (
	MapTask = iota
	ReduceTask
	NoTask
	ExitTask
)

// 任务状态
const (
	TaskIdle = iota
	TaskInProgress
	TaskCompleted
)

// 任务结构体
type Task struct {
	TaskType    int      // MapTask, ReduceTask, NoTask, ExitTask
	TaskId      int      // 任务ID
	FileName    string   // 输入文件名（Map任务使用）
	NReduce     int      // Reduce任务数量
	NMap        int      // Map任务数量
	State       int      // 任务状态
	StartTime   int64    // 任务开始时间
}

// 请求任务参数
type RequestTaskArgs struct {
	WorkerId int
}

// 请求任务响应
type RequestTaskReply struct {
	Task Task
}

// 完成任务参数
type CompleteTaskArgs struct {
	TaskId   int
	TaskType int
	WorkerId int
}

// 完成任务响应
type CompleteTaskReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

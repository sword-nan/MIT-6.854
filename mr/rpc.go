package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// type Task[T MapTask | ReduceTask] struct {
// 	task T
// }

// worker 请求任务的参数
type RequestArgs struct {
}

// worker 请求任务，完成任务后的返回值
type RequestReply struct {
	NReduce int
	Task    Task // MapTask / ReduceTask
}

type ResponseArgs struct {
	TaskID int
	Files  []string // 文件名
}

type ResponseReply struct {
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

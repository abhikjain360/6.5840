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

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	NONE
)

type Task struct {
	Type TaskType
	File string
	Id   int
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	Done    bool
	Wait    bool
	Task    Task
	Attempt int
	N       int
}

type DoneTaskArgs struct {
	Task    Task
	Attempt int
}

type DoneTaskReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

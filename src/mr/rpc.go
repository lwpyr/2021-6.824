package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
)
import "strconv"

const (
	READY = iota
	PROCESSING
	COMPLETED
)

const (
	MAP = iota
	REDUCE
	WAIT
	DONE
)

type Task struct {
	ID int
	State int
	Type int
	FileName string
	NReduce int
	ReduceIndex int
	ReduceRange int
	CancelTimer chan struct{}
	Lock sync.Mutex
	Notify chan *Task
}

// Add your RPC definitions here.
type AskTask struct {
	ID int
}

type ReplyTask struct {
	ID int
	State int
	Type int
	FileName string
	NReduce int
	ReduceIndex int
	ReduceRange int
}

type ReportTask struct {
	Type int
	ID int
}

type AckReport struct {
	KeepServe bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

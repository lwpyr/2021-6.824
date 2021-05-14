package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	PreparePhase = iota
	MapPhase
	ReducePhase
	CompletePhase
)

type Coordinator struct {
	// Your definitions here.
	Phase int
	Files []string

	TaskNum int
	NReduce int

	MapTasks         map[int]*Task
	ReduceTasks      map[int]*Task
	TaskQueue        []*Task

	TaskCompleted       int

	TaskWatcher chan *Task
	Lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *AskTask, reply *ReplyTask) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	fmt.Printf("Recieve ask task %v\n", args)
	defer fmt.Printf("Send task %v\n", reply)
	if c.Phase == MapPhase {
		if len(c.TaskQueue) > 0 {
			t := c.TaskQueue[0]
			reply.Type = MAP
			reply.FileName = t.FileName
			reply.ID = t.ID
			reply.NReduce = c.NReduce
			c.TaskQueue = c.TaskQueue[1:]
			t.State = PROCESSING
			go c.WatchTimeout(t)
		} else {
			reply.Type = WAIT
		}
	} else if c.Phase == ReducePhase {
		if len(c.TaskQueue) > 0 {
			t := c.TaskQueue[0]
			reply.Type = REDUCE
			reply.ID = t.ID
			reply.ReduceIndex = t.ReduceIndex
			reply.ReduceRange = t.ReduceRange
			c.TaskQueue = c.TaskQueue[1:]
			t.State = PROCESSING
			go c.WatchTimeout(t)
		} else {
			reply.Type = WAIT
		}
	} else {
		reply.Type = DONE
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTask, reply *AckReport) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	fmt.Printf("Recieve report %v\n", args)
	defer fmt.Printf("Send ack %v\n", reply)
	var t *Task
	if args.Type == MAP {
		t = c.MapTasks[args.ID]
	} else if args.Type == REDUCE {
		t = c.ReduceTasks[args.ID]
	}
	if t != nil {
		if t.State == PROCESSING {
			t.State = COMPLETED
			t.CancelTimer <- struct{}{}
			c.TaskCompleted++
			if c.TaskCompleted == c.TaskNum {
				c.TransformNext()
			}
		}
	}
	reply.KeepServe = c.Phase != CompletePhase
	return nil
}

func (c *Coordinator) WatchTimeout(t *Task) {
	select {
	case <- time.NewTimer(5 * time.Second).C:
		log.Printf("Task %d - %d Timeout\n", t.Type, t.ID)
		c.Lock.Lock()
		defer c.Lock.Unlock()
		if t.State == PROCESSING {
			t.State = READY
			c.TaskQueue = append(c.TaskQueue, t)
		}
	case <- t.CancelTimer:
		return
	}
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
	log.Println("listen on " + sockname)
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.Lock.Lock()
	defer c.Lock.Unlock()
	return c.Phase == CompletePhase
}

func (c *Coordinator) TransformNext() {
	// Your code here.
	switch c.Phase {
	case PreparePhase:
		c.TaskNum = len(c.Files)
		c.TaskCompleted = 0
		for i:=0; i<c.TaskNum; i++ {
			t := &Task{
				ID:          i,
				State:       READY,
				Type:        MAP,
				FileName:    c.Files[i],
				NReduce:     c.NReduce,
				CancelTimer: make(chan struct{}),
				Notify:      c.TaskWatcher,
			}
			c.TaskQueue = append(c.TaskQueue, t)
			c.MapTasks[i] = t
		}
		c.Phase = MapPhase
	case MapPhase:
		c.TaskNum = c.NReduce
		c.TaskCompleted = 0
		for i:=0; i<c.TaskNum; i++ {
			t := &Task{
				ID:          i,
				State:       READY,
				Type:        REDUCE,
				ReduceIndex: i,
				ReduceRange: len(c.Files),
				CancelTimer: make(chan struct{}),
				Notify:      c.TaskWatcher,
			}
			c.TaskQueue = append(c.TaskQueue, t)
			c.ReduceTasks[i] = t
		}
		c.Phase = ReducePhase
	case ReducePhase:
		c.Phase = CompletePhase
	}
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase:         PreparePhase,
		Files:         files,
		TaskNum:       0,
		NReduce:       nReduce,
		MapTasks:      make(map[int]*Task),
		ReduceTasks:   make(map[int]*Task),
		TaskQueue:     []*Task{},
		TaskCompleted: 0,
		TaskWatcher:   make(chan*Task),
		Lock:          sync.Mutex{},
	}
	c.TransformNext()
	// Your code here.
	c.server()
	return &c
}

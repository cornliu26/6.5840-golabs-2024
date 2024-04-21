package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskStatus int

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Index   int
	NReduce int
	Type    TaskType
	Input   []string
	Output  []string
}

type TaskMeta struct {
	mu        sync.RWMutex
	startTime int64
	status    TaskStatus
	task      *Task
}

type Coordinator struct {
	nReduce            int
	tempFiles          [][]string
	taskQueue          chan *Task
	taskMeta           []TaskMeta
	notCompletedMap    int32
	notCompletedReduce int32
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	select {
	case task := <-c.taskQueue:
		fmt.Printf("assign task: idx[%v] type[%v]\n", task.Index, task.Type)
		taskMeta := &c.taskMeta[task.Index]

		taskMeta.mu.Lock()
		taskMeta.startTime = time.Now().UnixMilli()
		taskMeta.status = InProgress
		taskMeta.task = task
		taskMeta.mu.Unlock()

		*reply = *task
	default:
		*reply = Task{
			Type: Wait,
		}
	}
	return nil
}

func (c *Coordinator) CompleteTask(task *Task, args *ExampleArgs) error {
	fmt.Printf("complete task: idx[%v] type[%v]\n", task.Index, task.Type)
	taskMeta := &c.taskMeta[task.Index]

	taskMeta.mu.Lock()
	taskMeta.status = Completed
	taskMeta.mu.Unlock()

	switch task.Type {
	case Map:
		for idx, file := range task.Output {
			c.tempFiles[idx] = append(c.tempFiles[idx], file)
		}
		notCompleted := atomic.AddInt32(&c.notCompletedMap, -1)
		if notCompleted == 0 {
			c.initReduceTask()
		}
	case Reduce:
		completed := atomic.AddInt32(&c.notCompletedReduce, -1)
		if completed == 0 {
			c.initExitTask()
		}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := atomic.LoadInt32(&c.notCompletedReduce) == 0
	time.Sleep(time.Second * 3)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:            nReduce,
		tempFiles:          make([][]string, nReduce),
		taskQueue:          make(chan *Task, nReduce+len(files)),
		taskMeta:           make([]TaskMeta, nReduce),
		notCompletedMap:    int32(len(files)),
		notCompletedReduce: int32(nReduce),
	}
	println("init Coordinator")
	c.initMapTask(files)
	c.server()
	go c.checkTaskStatus()
	return &c
}

func (c *Coordinator) initMapTask(files []string) {
	for idx, file := range files {
		task := Task{
			Index:   idx,
			NReduce: c.nReduce,
			Type:    Map,
			Input:   []string{file},
		}
		c.taskQueue <- &task
	}
}

func (c *Coordinator) initReduceTask() {
	for idx := 0; idx < c.nReduce; idx++ {
		task := Task{
			Index:   idx,
			NReduce: c.nReduce,
			Type:    Reduce,
			Input:   c.tempFiles[idx],
		}
		c.taskQueue <- &task
	}
}

func (c *Coordinator) initExitTask() {
	for idx := 0; idx < c.nReduce; idx++ {
		task := Task{
			Type: Exit,
		}
		c.taskQueue <- &task
	}
}

func (c *Coordinator) checkTaskStatus() {
	for {
		time.Sleep(time.Second)
		for i := range c.taskMeta {
			taskMeta := &c.taskMeta[i]

			taskMeta.mu.RLock()
			timedOut := taskMeta.status == InProgress && time.Now().UnixMilli()-taskMeta.startTime > 10000
			taskMeta.mu.RUnlock()

			if timedOut {
				fmt.Printf("task %v timeout, restart it\n", taskMeta.task.Index)

				taskMeta.mu.Lock()
				if taskMeta.status == InProgress && time.Now().UnixMilli()-taskMeta.startTime > 10000 {
					taskMeta.status = Idle
					c.taskQueue <- taskMeta.task
				}
				taskMeta.mu.Unlock()
			}
		}
	}
}

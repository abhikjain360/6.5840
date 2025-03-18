package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap    int
	tasks   []Task
	status  map[Task]status
	mu      sync.Mutex
	wg      sync.WaitGroup
	done    bool
}

type status struct {
	attempt int
	type_   statusType
}

type statusType int

const (
	QUEUED statusType = iota
	RUNNING
	DONE
)

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.done {
		reply.Done = true
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.tasks) > 0 {
		// pop from tasks
		reply.Task = c.tasks[len(c.tasks)-1]
		c.tasks = c.tasks[:len(c.tasks)-1]
		switch reply.Task.Type {
		case MAP:
			reply.N = c.nReduce
		case REDUCE:
			reply.N = c.nMap
		}

		// update status
		status := c.status[reply.Task]
		status.type_ = RUNNING
		c.status[reply.Task] = status

		// pass the attempt number
		reply.Attempt = status.attempt

		// timeout for the attempt
		go c.timeout(reply.Task, status.attempt)

		return nil
	}

	reply.Wait = true
	return nil
}

func (c *Coordinator) timeout(task Task, attempt int) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.status[task]
	// timeout while this task was running and we didn't assign it to new worker
	if status.type_ == RUNNING && status.attempt == attempt {
		// queue the task for the next attempt
		c.queue(task, attempt+1)
	}
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	if c.done {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.status[args.Task]
	// didn't timeout
	if status.type_ == RUNNING && status.attempt == args.Attempt {
		// mark the task as done
		status.type_ = DONE
		c.status[args.Task] = status
		c.wg.Done()
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	return c.done
}

func (c *Coordinator) wait() {
	// wait for map tasks to finish
	c.wg.Wait()

	// build reduce tasks
	for i := range c.nReduce {
		task := Task{
			Type: REDUCE,
			Id:   i,
		}
		c.queue(task, 0)
		c.wg.Add(1)
	}

	// wait for reduce tasks to finish
	c.wg.Wait()

	// all done
	c.done = true
}

func (c *Coordinator) queue(task Task, attempt int) {
	c.tasks = append(c.tasks, task)
	c.status[task] = status{attempt: attempt, type_: QUEUED}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
		tasks:   []Task{},
		status:  make(map[Task]status),
		done:    false,
	}

	// fill map tasks
	for i, file := range files {
		task := Task{
			Type: MAP,
			File: file,
			Id:   i,
		}
		c.queue(task, 0)
		c.wg.Add(1)
	}

	go c.wait()

	c.server()
	return &c
}

package mr

import (
	"errors"
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
	mapMutex        sync.Mutex
	reduceMutex     sync.Mutex
	fetched         map[int]bool
	reduceAllocated map[int]bool
	nMap            int
	nReduce         int
	mapTasks        map[int]bool
	reduceTasks     map[int]bool
	reduceNumber    int
	mapFinished     bool
	reduceFinished  bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MapTaskForWorker(args *ExampleArgs, reply *MapReply) error {
	for i, filename := range os.Args[1:] {
		c.mapMutex.Lock()
		b := c.fetched[i]
		c.mapMutex.Unlock()
		if b {
			continue
		}
		//fmt.Println(filename)
		c.mapMutex.Lock()
		c.fetched[i] = true
		c.mapMutex.Unlock()
		reply.Filename = filename
		reply.NReduce = c.nReduce
		reply.MapNumber = i
		return nil
	}

	return errors.New("all map tasks finished")
}

func (c *Coordinator) MapDone(args *MapArgs, reply *ExampleReply) error {
	mapNumber := args.MapNumber
	c.mapTasks[mapNumber] = true
	return nil
}

func (c *Coordinator) MapServer() {
	for {
		flag := true
		for i := 0; i < c.nMap; i++ {
			if !c.fetched[i] {
				flag = false
				break
			}
			if !c.mapTasks[i] {
				time.Sleep(10 * time.Second)
				if !c.mapTasks[i] {
					c.fetched[i] = false
					flag = false
					break
				}
			}
		}
		if flag {
			c.mapFinished = true
		}
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) ReduceTaskForWorker(args *ExampleArgs, reply *ReduceReply) error {
	if !c.mapFinished {
		return errors.New("please await all map tasks finished")
	}

	for i := 0; i < c.nReduce; i++ {
		c.reduceMutex.Lock()
		b := c.reduceAllocated[i]
		c.reduceMutex.Unlock()
		if b {
			continue
		}
		c.reduceMutex.Lock()
		c.reduceAllocated[i] = true
		c.reduceMutex.Unlock()
		reply.ReduceNumber = i
		return nil
	}

	return errors.New("all reduce tasks finished")
}

func (c *Coordinator) ReduceDone(args *ReduceArgs, reply *ExampleReply) error {
	reduceNumber := args.ReduceNumber
	c.reduceTasks[reduceNumber] = true
	return nil
}

func (c *Coordinator) ReduceServer() {
	for {
		flag := true
		for i := 0; i < c.nReduce; i++ {
			if !c.reduceAllocated[i] {
				flag = false
				break
			}
			if !c.reduceTasks[i] {
				time.Sleep(10 * time.Second)
				if !c.reduceTasks[i] {
					c.reduceAllocated[i] = false
					flag = false
					break
				}
			}
		}
		if flag {
			c.reduceFinished = true
		}
		time.Sleep(time.Second)
	}
}

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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mapMutex.Lock()
	c.reduceMutex.Lock()
	if c.mapFinished && c.reduceFinished {
		ret = true
	}
	c.mapMutex.Unlock()
	c.reduceMutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fetched = make(map[int]bool)
	c.reduceAllocated = make(map[int]bool)
	c.mapTasks = make(map[int]bool)
	c.reduceTasks = make(map[int]bool)
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapFinished = false
	c.reduceFinished = false
	go c.MapServer()
	go c.ReduceServer()
	c.server()
	return &c
}

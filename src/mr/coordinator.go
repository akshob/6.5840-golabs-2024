package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)


type Coordinator struct {
	files []string
	nReduce int
	assignedFiles map[string]bool
	doneMap map[string]bool
	doneReduce map[string]bool
	mapWorkers map[string]string
	mapWorkersTime map[string]int64
	reduceWorkers map[string]string
	reduceWorkersTime map[string]int64
}

func (c *Coordinator) Assignment(args *WorkerArgs, reply *WorkerReply) error {
	nextUnassignedFileForMap := c.pickNextUnassignedFileForMap()
	if nextUnassignedFileForMap != "" {
		fmt.Printf("Assigning file %v to worker %v\n", nextUnassignedFileForMap, args.WorkerId)
		c.mapWorkers[args.WorkerId] = nextUnassignedFileForMap
		c.mapWorkersTime[args.WorkerId] = time.Now().Unix()
		reply.File = nextUnassignedFileForMap
		reply.WorkType = "map"
		reply.NReduce = c.nReduce
		return nil
	}

	for _, file := range c.files {
		if !c.doneMap[file] {
			reply.File = ""
			return nil
		}
	}

	nextUnassignedReduce := c.pickNextUnassignedReduce()
	if nextUnassignedReduce != "" {
		fmt.Printf("Assigning reduce %v to worker %v\n", nextUnassignedReduce, args.WorkerId)
		c.reduceWorkers[args.WorkerId] = nextUnassignedReduce
		c.reduceWorkersTime[args.WorkerId] = time.Now().Unix()
		reply.File = nextUnassignedReduce
		reply.WorkType = "reduce"
		reply.NReduce = c.nReduce
		return nil
	}
	reply.File = ""
	return nil
}

func (c *Coordinator) DoneMap(args *WorkerArgs, reply *WorkerReply) error {
	fmt.Printf("Worker %v has finished processing file %v\n", args.WorkerId, c.mapWorkers[args.WorkerId])
	c.doneMap[c.mapWorkers[args.WorkerId]] = true
	return nil
}

func (c *Coordinator) DoneReduce(args *WorkerArgs, reply *WorkerReply) error {
	fmt.Printf("Worker %v has finished processing reduce %v\n", args.WorkerId, c.reduceWorkers[args.WorkerId])
	c.doneReduce[c.reduceWorkers[args.WorkerId]] = true
	return nil
}

func (c *Coordinator) pickNextUnassignedFileForMap() string {
	for _, file := range c.files {
		if !c.assignedFiles[file] {
			c.assignedFiles[file] = true
			return file
		}
	}
	return ""
}

func (c *Coordinator) pickNextUnassignedReduce() string {
	for i := 0; i < c.nReduce; i++ {
		iAsString := fmt.Sprintf("%d", i)
		if !c.doneReduce[iAsString] {
			c.doneReduce[iAsString] = true
			return iAsString
		}
	}
	return ""
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for workerID, timestamp := range c.mapWorkersTime {
		if time.Now().Unix()-timestamp > 10 {
			file := c.mapWorkers[workerID]
			if !c.doneMap[file] {
				c.assignedFiles[file] = false
			}
			delete(c.mapWorkers, workerID)
			delete(c.mapWorkersTime, workerID)
		}
	}

	for _, file := range c.files {
		if !c.doneMap[file] {
			return false
		}
	} // all files are done with map phase

	for workerID, timestamp := range c.reduceWorkersTime {
		if time.Now().Unix()-timestamp > 10 {
			file := c.reduceWorkers[workerID]
			c.doneReduce[file] = false
			delete(c.reduceWorkers, workerID)
		}
	}

	for i := 0; i < c.nReduce; i++ {
		iAsString := fmt.Sprintf("%d", i)
		if !c.doneReduce[iAsString] {
			return false
		}
	} // all files are done with reduce phase

	return true
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
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		assignedFiles: make(map[string]bool),
		doneMap: make(map[string]bool),
		doneReduce: make(map[string]bool),
		mapWorkers: make(map[string]string),
		mapWorkersTime: make(map[string]int64),
		reduceWorkers: make(map[string]string),
		reduceWorkersTime: make(map[string]int64),
	}
	fmt.Printf("Starting coordinator server\n")

	for _, file := range files {
		c.assignedFiles[file] = false
		c.doneMap[file] = false
	}

	for i := 0; i < nReduce; i++ {
		iAsString := fmt.Sprintf("%d", i)
		c.doneReduce[iAsString] = false
	}
	
	c.server()
	return &c
}

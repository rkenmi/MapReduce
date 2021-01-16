package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const UNINITIALIZED = -1
const (
	NOT_STARTED = iota
	IN_PROGRESS
	DONE
)

type Master struct {
	// Your definitions here.
	nReduce int

	mapCompleted    bool
	reduceCompleted bool

	tasks       []string
	mapTable    map[string]int
	reduceTable map[int]int

	// Lock
	mu *sync.Mutex
}

/*
	Helper function to get index of element in slice
*/
func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1
}

func IsMapProcessFinished(m *Master) bool {
	m.mu.Lock()
	var mapProcessIsFinished = m.mapCompleted
	m.mu.Unlock()
	return mapProcessIsFinished
}

/*
	Synchronize Map table and find the next available task to serve to the worker.
*/
func ServeMapTask(m *Master, taskRequest *Task) {
	m.mu.Lock()
	for filename, status := range m.mapTable {
		if status == NOT_STARTED {
			m.mapTable[filename] = IN_PROGRESS
			taskId := indexOf(filename, m.tasks)
			taskRequest.Filename = filename
			taskRequest.Acknowledged = true
			taskRequest.MapTaskId = taskId

			fmt.Println("Sent map task id " + strconv.Itoa(taskId) + ", file name: " + filename)

			// Wait for 10 secs and retry file if worker doesn't finish it by then
			// Spin this function up in another thread
			go func(filename string) {
				time.Sleep(time.Second * 10)
				m.mu.Lock()
				if m.mapTable[filename] != DONE {
					fmt.Println("Timeout for: " + filename)
					m.mapTable[filename] = NOT_STARTED
				}
				m.mu.Unlock()
			}(filename)

			break
		}
	}
	m.mu.Unlock()
}

/*
	Synchronize Reduce table and find the next available task to serve to the worker.
*/
func ServeReduceTask(m *Master, taskRequest *Task) {
	m.mu.Lock()
	for reduceId, status := range m.reduceTable {
		if status == NOT_STARTED {
			m.reduceTable[reduceId] = IN_PROGRESS
			taskRequest.ReduceTaskId = reduceId
			taskRequest.Acknowledged = true

			fmt.Println("Sent reduce task: " + strconv.Itoa(reduceId) + " for work")

			// Wait for 10 secs and retry file if worker doesn't finish it by then
			// Spin this function up in another thread
			go func(taskId int) {
				time.Sleep(time.Second * 10)
				m.mu.Lock()
				if m.reduceTable[taskId] != DONE {
					fmt.Println("Timeout for reduce task: " + strconv.Itoa(taskId))
					m.reduceTable[taskId] = NOT_STARTED
				}
				m.mu.Unlock()
			}(reduceId)

			break
		}
	}
	m.mu.Unlock()
}

// START RPC Handlers

/*
	Update the Map table with the latest job state
*/
func (m *Master) UpdateMapTask(request *MapStatusRequest, response *MapStatusAck) error {
	m.mu.Lock()
	m.mapTable[request.Filename] = request.Status
	m.mu.Unlock()
	response.Acknowledged = true
	fmt.Println("Received completion for " + request.Filename)
	return nil
}

/*
	Update the Reduce table with the latest job state
*/
func (m *Master) UpdateReduceTask(request *ReduceStatusRequest, response *ReduceStatusAck) error {
	m.mu.Lock()
	m.reduceTable[request.ReduceId] = request.Status
	m.mu.Unlock()
	response.Acknowledged = true
	fmt.Println("Received completion for " + strconv.Itoa(request.ReduceId))
	return nil
}

/*
	Serve a task to a worker
*/
func (m *Master) ServeTask(args *TaskArgs, taskRequest *Task) error {
	// These values are static
	taskRequest.NReduce = m.nReduce
	taskRequest.NMap = len(m.tasks)

	taskRequest.MapTaskId = UNINITIALIZED
	taskRequest.ReduceTaskId = UNINITIALIZED
	if !IsMapProcessFinished(m) {
		ServeMapTask(m, taskRequest)
	} else {
		ServeReduceTask(m, taskRequest)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.mapCompleted && m.reduceCompleted
}

// END RPC Handlers

/*
	Poll every second if the Map or Reduce process is completed.
	We only lock the mutex when we update the completed flags for Map and Reduce
	Since the DONE state is final, it's okay if this thread reads inconsistent values of the Map table or Reduce table.
	Once the completed flag for a certain process is set, we don't worry about that process anymore.
*/
func PollForCompletedTasksAndUpdateState(m *Master) {
	for {
		isComplete := 0
		for _, status := range m.mapTable {
			if status == DONE {
				isComplete += 1
			}
		}

		if isComplete == len(m.mapTable) {
			m.mu.Lock()
			m.mapCompleted = true
			m.mu.Unlock()
			fmt.Println("=== MAP is complete ===")
			break
		}
		time.Sleep(time.Second)
	}
	for {
		isComplete := 0
		for _, status := range m.reduceTable {
			if status == DONE {
				isComplete += 1
			}
		}

		if isComplete == len(m.reduceTable) {
			m.mu.Lock()
			m.reduceCompleted = true
			m.mu.Unlock()
			fmt.Println("=== REDUCE is complete ===")
			break
		}
		time.Sleep(time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapTable = make(map[string]int)
	m.reduceTable = make(map[int]int)

	// Initialize tables
	for _, file := range files {
		m.mapTable[file] = NOT_STARTED
	}
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		m.reduceTable[reduceId] = NOT_STARTED
	}

	m.tasks = files
	m.nReduce = nReduce

	// Lock to maintain synchrony
	m.mu = &sync.Mutex{}

	m.server()

	// Spin up the poll in another thread, which will check and record if Map and Reduce is complete
	go PollForCompletedTasksAndUpdateState(&m)

	return &m
}

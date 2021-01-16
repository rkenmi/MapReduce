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

type TaskArgs struct {
}

type Task struct {
	NReduce      int
	NMap         int
	Filename     string
	MapTaskId    int
	ReduceTaskId int
	Acknowledged bool
}

type MapStatusRequest struct {
	Filename string
	Status   int
}

type MapStatusAck struct {
	Acknowledged bool
}

type MapStatus struct {
	Done bool
}

type ReduceStatusRequest struct {
	ReduceId int
	Status   int
}

type ReduceStatusAck struct {
	Acknowledged bool
}

type ReduceStatus struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// This function is used to mimic a worker failure, used for debugging.
func CauseIntermittentWorkerFailure() bool {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	if r1.Intn(10) < 2 {
		return true
	}
	return false
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := RequestTask()
		if task.MapTaskId == UNINITIALIZED {
			// Move on to the Reduce job
			break
		}
		if !task.Acknowledged {
			time.Sleep(time.Second)
			continue
		}

		fmt.Println("Worker: Picked up a Map task: " + strconv.Itoa(task.MapTaskId))
		Map(task, mapf)
	}

	for {
		task := RequestTask()
		if !task.Acknowledged {
			time.Sleep(time.Second)
			continue
		}
		fmt.Println("Worker: Picked up a Reduce task " + strconv.Itoa(task.ReduceTaskId))
		Reduce(task.NMap, task.ReduceTaskId, reducef)
	}
}

func Map(task Task, mapf func(string, string) []KeyValue) {
	// read input file,
	// pass it to Map,
	// accumulate the intermediate Map output.

	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	StoreIntermediate(task.MapTaskId, task.NReduce, kva)
	SendMapJobStatus(filename, DONE)
}

func Reduce(nMap int, reduceTaskId int, reducef func(string, []string) string) {
	oname := "mr-out-" + strconv.Itoa(reduceTaskId)
	ofile, _ := os.Create(oname)

	intermediate, _ := OpenIntermediate(nMap, reduceTaskId)
	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-Y, where Y is the reduce task id
	//
	i := 0

	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err := ofile.Close()
	if err != nil {
		fmt.Println(err)
	}
	SendReduceJobStatus(reduceTaskId, DONE)
}

// Store intermediate file in format 'mr-X-Y', where X is map task id and Y is reduce task id
// Y is derived by calling the hash partition on the key of the intermediate key-value pairs
func StoreIntermediate(mapTaskId int, nReduce int, intermediate []KeyValue) {
	reduceTable := make(map[int][]KeyValue)

	for _, kv := range intermediate {
		reduceTaskId := ihash(kv.Key) % nReduce
		reduceTable[reduceTaskId] = append(reduceTable[reduceTaskId], kv)
	}
	for reduceId, kvSlices := range reduceTable {
		intermediateFilename := "mr-" + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(reduceId)
		temp, err := ioutil.TempFile("", intermediateFilename)
		if err != nil {
			temp, _ = os.Create(intermediateFilename)
		}

		enc := json.NewEncoder(temp)
		for _, kv := range kvSlices {
			enc.Encode(&kv)
		}
		temp.Close()
		os.Rename(temp.Name(), intermediateFilename)
	}
}

func OpenIntermediate(nMap int, reduceTaskId int) ([]KeyValue, error) {
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskId)
		file, err := os.Open(iname)
		if err != nil {
			return nil, err
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva, nil
}

func RequestTask() Task {
	task := Task{}
	call("Master.ServeTask", TaskArgs{}, &task)
	return task
}

func SendReduceJobStatus(reduceId int, status int) bool {
	response := &ReduceStatusAck{}
	call("Master.UpdateReduceTask", ReduceStatusRequest{
		ReduceId: reduceId,
		Status:   status,
	}, response)

	return response.Acknowledged
}

func SendMapJobStatus(filename string, status int) bool {
	response := &MapStatusAck{}
	call("Master.UpdateMapTask", MapStatusRequest{
		Filename: filename,
		Status:   status,
	}, response)

	return response.Acknowledged
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := getTask()

		if reply.Done {
			fmt.Println("all tasks done")
			return
		}

		if reply.Wait {
			fmt.Println("waiting for task")
			time.Sleep(3 * time.Second)
			continue
		}

		fmt.Printf("received task: %v\n", reply.Task)

		if reply.Task.Type == MAP {
			doMap(reply, mapf)
		} else if reply.Task.Type == REDUCE {
			doReduce(reply, reducef)
		}

		doneTask(reply.Task, reply.Attempt)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doReduce(reply GetTaskReply, reducef func(string, []string) string) {
	agg := make(map[string][]string)

	for mapId := range reply.N {
		filename := fmt.Sprintf("mr-%d-%d", mapId, reply.Task.Id)
		content, err := os.ReadFile(filename)
		if err != nil {
			fmt.Printf("failed to read file %s: %v\n", filename, err)
			continue
		}

		kva := []KeyValue{}
		err = json.Unmarshal(content, &kva)
		if err != nil {
			fmt.Printf("failed to unmarshal file %s: %v\n", filename, err)
			continue
		}

		for _, kv := range kva {
			agg[kv.Key] = append(agg[kv.Key], kv.Value)
		}
	}

	filename := fmt.Sprintf("mr-out-%d", reply.Task.Id)
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("failed to create file %s: %v\n", filename, err)
		return
	}
	defer file.Close()

	for key, values := range agg {
		result := reducef(key, values)
		fmt.Fprintf(file, "%s %s\n", key, result)
	}
}

func doMap(reply GetTaskReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(reply.Task.File)
	if err != nil {
		fmt.Printf("failed to read file %s: %v\n", reply.Task.File, err)
		return
	}

	kva := mapf(reply.Task.File, string(content))

	imt := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % reply.N
		imt[reduceId] = append(imt[reduceId], kv)
	}

	for reduceId := range reply.N {
		filename := fmt.Sprintf("mr-%d-%d", reply.Task.Id, reduceId)
		file, err := os.CreateTemp("/tmp", filename)
		if err != nil {
			fmt.Printf("failed to create file %s: %v\n", filename, err)
			return
		}
		defer file.Close()

		var kva []KeyValue
		kva, ok := imt[reduceId]
		if !ok {
			kva = []KeyValue{}
		}

		result, err := json.Marshal(kva)
		if err != nil {
			fmt.Printf("failed to encode file %s: %v\n", filename, err)
			return
		}

		_, err = file.Write(result)
		if err != nil {
			fmt.Printf("failed to encode file %s: %v\n", filename, err)
		}

		err = os.Rename(file.Name(), filename)
		if err != nil {
			fmt.Printf("failed to rename file %s: %v\n", filename, err)
		}
	}
}

func getTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Println("failed to get task")
	}
	return reply
}

func doneTask(task Task, attempt int) {
	args := DoneTaskArgs{Task: task, Attempt: attempt}
	reply := DoneTaskReply{}
	ok := call("Coordinator.DoneTask", &args, &reply)
	if !ok {
		fmt.Println("failed to done task")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

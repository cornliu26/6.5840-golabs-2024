package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		task, ok := getTask()

		if !ok {
			time.Sleep(time.Second)
		}

		switch task.Type {
		case Map:
			mapTask(mapf, task)
		case Reduce:
			reduceTask(reducef, task)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}
}

func mapTask(mapf func(string, string) []KeyValue, task Task) {
	filename := task.Input[0]
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("Failed to read file:", filename, err)
	}
	intermediates := mapf(filename, string(content))

	buffer := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediates {
		i := ihash(kv.Key) % task.NReduce
		buffer[i] = append(buffer[i], kv)
	}

	task.Output = writeTempFiles(buffer)

	completeTask(&task)
}

func reduceTask(reducef func(string, []string) string, task Task) {
	intermediate := readTempFiles(task.Input)

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.Index)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer ofile.Close()

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	completeTask(&task)
}

func writeTempFiles(buffer [][]KeyValue) []string {
	fileNum := len(buffer)
	files := make([]string, fileNum)

	for i := 0; i < fileNum; i++ {
		tempFileName := fmt.Sprintf("temp-%d-*", i)
		file, err := os.CreateTemp("", tempFileName)
		if err != nil {
			log.Fatal("Failed to create temp file:", err)
		}
		files[i] = file.Name()

		enc := json.NewEncoder(file)
		err = enc.Encode(buffer[i])
		if err != nil {
			log.Fatal("Failed to encode:", err)
		}

		err = file.Close()
		if err != nil {
			log.Fatal("Failed to close file:", err)
		}
	}

	return files
}

func readTempFiles(files []string) []KeyValue {
	intermediates := make([]KeyValue, 0)

	for _, file := range files {
		file, err := os.Open(file)
		if err != nil {
			log.Fatal("Failed to open file:", file, err)
		}

		dec := json.NewDecoder(file)
		kvs := make([]KeyValue, 0)

		err = dec.Decode(&kvs)
		if err != nil {
			log.Fatal("Failed to decode:", err)
		}

		intermediates = append(intermediates, kvs...)

		err = file.Close()
		if err != nil {
			log.Fatal("Failed to close file:", err)
		}
	}

	return intermediates
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func getTask() (Task, bool) {
	args := ExampleArgs{}
	reply := Task{}

	ok := call("Coordinator.AssignTask", &args, &reply)

	if ok {
		fmt.Printf("Get Task: idx[%v], type[%v]\n", reply.Index, reply.Type)
		return reply, true
	} else {
		fmt.Printf("Get Task failed!\n")
		return Task{}, false
	}
}

func completeTask(task *Task) bool {
	args := ExampleArgs{}
	maxRetries := 3
	retryInterval := time.Second

	for attempts := 0; attempts < maxRetries; attempts++ {
		ok := call("Coordinator.CompleteTask", &task, &args)
		if ok {
			fmt.Printf("Complete task: idx[%v], type[%v]\n", task.Index, task.Type)
			return true
		} else {
			fmt.Printf("Attempt %d to complete task failed!\n", attempts+1)
			time.Sleep(retryInterval)
		}
	}

	fmt.Printf("Complete task failed after %d attempts!\n", maxRetries)
	return false
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

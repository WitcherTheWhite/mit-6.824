package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

// map任务编号
var mapIndex = 0

// reduce任务编号

var reduceIndex = 0

var mu sync.Mutex

var wg sync.WaitGroup

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	go mapPhase(mapf)
	go reducePhase(reducef)

	for {
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// map阶段
//
func mapPhase(mapf func(string, string) []KeyValue) {
	for {
		args := ExampleArgs{}
		reply := MapReply{}
		if !call("Coordinator.MapTaskForWorker", args, &reply) {
			time.Sleep(time.Second)
			continue
		}
		filename := reply.Filename
		//fmt.Println(filename)
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
		sort.Sort(ByKey(kva))

		mapNumber := reply.MapNumber
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []KeyValue{}
			for k := i; k < j; k++ {
				values = append(values, kva[k])
			}
			reduceNumber := ihash(kva[i].Key) % reply.NReduce
			oname := "mr-" + strconv.Itoa(mapNumber) + "-" + strconv.Itoa(reduceNumber)
			ofile, _ := os.OpenFile(oname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
			enc := json.NewEncoder(ofile)
			for _, kv := range values {
				enc.Encode(&kv)
			}
			ofile.Close()
			i = j
		}

		newargs := MapArgs{mapNumber}
		newreply := ExampleReply{}
		call("Coordinator.MapDone", &newargs, &newreply)
	}
}

//
// reduce阶段
//
func reducePhase(reducef func(string, []string) string) {
	for {
		args := ExampleArgs{}
		reply := ReduceReply{}
		if !call("Coordinator.ReduceTaskForWorker", args, &reply) {
			time.Sleep(time.Second)
			continue
		}

		reduceNumber := reply.ReduceNumber
		files, err := ioutil.ReadDir(".")
		if err != nil {
			log.Fatalf("cannot open current directory")
		}
		kva := []KeyValue{}
		for _, file := range files {
			objfile := "mr..." + strconv.Itoa(reduceNumber)
			match, _ := regexp.MatchString(objfile, file.Name())
			if !match {
				continue
			}
			f, err := os.Open(file.Name())
			if err != nil {
				log.Fatalf("cannot open %v", file.Name())
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			f.Close()
		}

		sort.Sort(ByKey(kva))
		oname := "mr-out-" + strconv.Itoa(reduceNumber)
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		ofile.Close()
		newargs := ReduceArgs{reduceNumber}
		newreply := ExampleReply{}
		call("Coordinator.ReduceDone", &newargs, &newreply)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	//fmt.Println(err)
	return false
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

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

type worker struct {
	WorkerID int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	// fmt.Println("Worker ID: ", w.WorkerID)
	w.run()
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	call("Master.Register", &args, &reply)

	w.WorkerID = reply.WorkerID
}

func (w *worker) run() {
	for {
		t := w.reqTask()
		w.doTask(t)
	}
}

func (w *worker) reqTask() Task {
	args := RequestTaskArgs{}
	args.WorkerID = w.WorkerID

	reply := RequestTaskReply{}

	if !call("Master.ReqTask", &args, &reply) {
		log.Fatalln("can not get task")
	}

	// fmt.Printf("Get task %+v\n", *reply.Task)

	return *reply.Task
}

func (w *worker) reportTask(task Task, done bool) {
	args := ReportTaskArgs{}
	args.Task = &task
	args.Done = done

	reply := ReportTaskReply{}

	// fmt.Printf("report task %+v\n", args)

	call("Master.ReportTask", &args, &reply)
}

func (w *worker) doTask(task Task) {
	switch task.Phase {
	case MapPhase:
		w.doMapTask(task)
	case ReducePhase:
		w.doReduceTask(task)
	}
}

func (w *worker) doMapTask(task Task) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	kva := w.mapf(task.FileName, string(content))
	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		filename := fmt.Sprintf("mr-%d-%d", task.Seq, idx)
		f, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("json encode fail")
			}
		}
		f.Close()
	}
	w.reportTask(task, true)
}

func (w *worker) doReduceTask(task Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < task.NMap; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, task.Seq)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	outFilename := fmt.Sprintf("mr-out-%d", task.Seq)
	err := ioutil.WriteFile(outFilename, []byte(strings.Join(res, "")), 0600)
	if err != nil {
		log.Fatalf("cannot write %v", outFilename)
	}

	w.reportTask(task, true)
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

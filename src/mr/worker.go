package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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

	// Your worker implementation here.
	for {
		task := callTask() // 调用（请求）任务（RPC调用）
		switch task.State {
		case Map:
			mapTask(mapf, task)
			break
		case Reduce:
			reduceTask(reducef, task)
			break
		case Wait:
			time.Sleep(time.Duration(time.Second * 10))
			break
		case Exit:
			fmt.Printf("All of tasks have been completed, nothing to do\n")
			return
		default:
			fmt.Printf("Invalid state code, try again!\n")
		}
	}
}

// CallTask function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func callTask() *Task {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	//args.X = 99

	// declare a reply structure.
	reply := Task{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {

		//fmt.Printf("call succeed!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//masterSock()方法是在rpc.go中，返回的是UNIX域下socket的名字
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

func mapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediates := []KeyValue{}

	filename := task.InFileName
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
	intermediates = append(intermediates, kva...)

	nReduce := task.NReduce

	var mapBuff map[int]ByKey
	mapBuff = make(map[int]ByKey, nReduce)

	// 切片， R份
	for _, kv := range intermediates {
		idx := ihash(kv.Key) % nReduce
		mapBuff[idx] = append(mapBuff[idx], kv)
	}

	var localFileNames []string // 每个 Map Task 产生的 R 个中间文件名称集合

	for i := 0; i < task.NReduce; i++ {
		localFileName := writeToLocalFile(task.TaskID, i, mapBuff[i])
		localFileNames = append(localFileNames, localFileName)
	}

	task.LocalFileNames = localFileNames
	//for _, filename_ := range task.LocalFileNames {
	//	fmt.Printf("imd file name: %s\n", filename_)
	//}
	taskCompleted(task)
}

func reduceTask(reducef func(string, []string) string, task *Task) {
	//fmt.Printf("This is reduce task\n")
	intermediate := readFromLocalFile(task.LocalFileNames) // 合并 reduce task 的输入文件片集合
	sort.Sort(ByKey(intermediate))

	currentDir, _ := os.Getwd()
	//currentDir += "/out"
	tmpFile, err := ioutil.TempFile(currentDir, "mr-tmp-r-*")
	if err != nil {
		log.Fatal("Fail to creat temp file", err)
	}

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	os.Rename(tmpFile.Name(), oname)
	task.OutFileName = oname
	taskCompleted(task)
}

func taskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)
}

/**
 * taskID : 任务对应的 ID
 * i    : 当前 Map task 产生的 R 个 临时文件对应的索引
 */
func writeToLocalFile(taskID int, i int, kvs []KeyValue) string {
	currentDir, _ := os.Getwd()
	//currentDir += "/imd"
	tmpFile, err := ioutil.TempFile(currentDir, "mr-tmp-m-*")
	//fmt.Printf(tmpFile.Name())
	if err != nil {
		log.Fatal("Fail to creat temp file", err)
	}
	enc := json.NewEncoder(tmpFile)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Fail to write kv pair", err)
		}
	}
	tmpFile.Close()
	outTmpFileName := fmt.Sprintf("mr-imd-%d-%d", taskID, i)
	os.Rename(tmpFile.Name(), outTmpFileName)
	return filepath.Join(currentDir, outTmpFileName)
}

func readFromLocalFile(filePaths []string) []KeyValue {
	//for _, filename_ := range filePaths {
	//	fmt.Printf("imd file name: %s\n", filename_)
	//}
	kva := []KeyValue{}
	for _, filfilePath := range filePaths {
		file, err := os.Open(filfilePath)
		if err != nil {
			log.Fatal("Failed to open file "+filfilePath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

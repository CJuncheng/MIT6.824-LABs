package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

type TaskState int

/** state value
 * 0 : map
 * 1 : reduce
 * 2 : wait
 * 3 : exit, nothing to do
 */
const (
	Map TaskState = iota
	Reduce
	Wait
	Exit
)

// Add your RPC definitions here.
type Task struct {
	State          TaskState
	InFileName     string   // map task 要读取的文件名
	TaskID         int      // map task id 对应文件 id (M个)；reduce task id(R个)
	NReduce        int      // reduce task 的数量
	LocalFileNames []string // 对于 map task，是一个 map task 产生的 R个文件名集合; 对
	/* 对于 reduce task，是多个Map task 产生的中间文件对应该 Reduce task ID 的分片集合，
	   该reduce task 将 M 个文件片合并，排序，输出一个文件*/
	OutFileName string // 每个reduce task 的输出文件名
}

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProcess
	Completed
)

type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus // Idle, InProcess, Completed
	StartTime     time.Time
	TaskReference *Task
}

type Coordinator struct {
	// Your definitions here.
	TaskQue       chan *Task               //等待执行的task
	TaskMeta      map[int]*CoordinatorTask // Coordinator为每个task 维护一个状态信息， int 代表 task ID
	State         TaskState                // Coordinator 阶段 对应的task任务状态
	NReduce       int
	InFiles       []string   // M个文件对应 M 个 Map task
	Intermediates [][]string // [i][j]: i 表示 reduce id([0, R)), j 代表 M个 map task 的索引([0, M))。Intermediates[i] 表示 reduce task i 对应的 M个输入文件片集合
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

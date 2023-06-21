package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	ret := c.State == Exit
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQue:       make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*CoordinatorTask),
		State:         Map,
		NReduce:       nReduce,
		InFiles:       files,
		Intermediates: make([][]string, nReduce),
	}
	//fmt.Printf("task number: %d, reduce number: %d\n", len(files), nReduce)
	c.createMapTask() // 为每个文件创建一个  map task
	c.server()

	// 启动一个goroutine 检查超时的任务
	go c.checkTimeOut()
	return &c
}

func (c *Coordinator) createMapTask() {
	// 根据输入文件，每个文件是一个 map task (split)
	for idx, filename := range c.InFiles {
		task := Task{
			State:      Map,
			InFileName: filename,
			TaskID:     idx, // 为每个文件 split 分配一个 task
			NReduce:    c.NReduce,
		}
		c.TaskQue <- &task
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &task,
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) checkTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.State == Exit {
			mu.Unlock()
			return
		}
		for _, coordinatorTask := range c.TaskMeta {
			if coordinatorTask.TaskStatus == InProcess && time.Now().Sub(coordinatorTask.StartTime) > 10*time.Second {
				c.TaskQue <- coordinatorTask.TaskReference
				coordinatorTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

// coordinator等待worker调用
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQue) > 0 {
		*reply = *<-c.TaskQue
		// 记录 task 的启动时间
		c.TaskMeta[reply.TaskID].TaskStatus = InProcess
		c.TaskMeta[reply.TaskID].StartTime = time.Now()
	} else if c.State == Exit {
		*reply = Task{State: Exit}
	} else {
		*reply = Task{State: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.State != c.State || c.TaskMeta[task.TaskID].TaskStatus == Completed {
		//对于重复的结果要丢弃
		return nil
	}
	c.TaskMeta[task.TaskID].TaskStatus = Completed
	go c.processTaskRes(task)
	return nil
}

func (c *Coordinator) processTaskRes(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.State {
	case Map:
		for idx, filePath := range task.LocalFileNames {
			c.Intermediates[idx] = append(c.Intermediates[idx], filePath)
		}
		if c.allTaskCompleted() {
			// 进入 reduce 阶段
			c.createReduceTask()
			c.State = Reduce
		}
	case Reduce:
		if c.allTaskCompleted() {
			c.State = Exit
		}
	}
}

func (c *Coordinator) allTaskCompleted() bool {
	for _, coordinatorTask := range c.TaskMeta {
		if coordinatorTask.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask)
	for idx, files := range c.Intermediates {
		task := Task{
			State:          Reduce,
			NReduce:        c.NReduce,
			TaskID:         idx,
			LocalFileNames: files,
		}
		c.TaskQue <- &task
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &task,
		}
	}
}

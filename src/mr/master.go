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

type TaskStatus int

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 1000
)

const (
	TaskStatusReady    TaskStatus = 0
	TaskStatusQueue    TaskStatus = 1
	TaskStatusRunning  TaskStatus = 2
	TaskStatusFinished TaskStatus = 3
	TaskStatusError    TaskStatus = 4
)

type TaskStat struct {
	Status    TaskStatus
	WorkerID  int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	WorkerCnt int
	mu        sync.Mutex
	files     []string
	taskPhase TaskPhase
	taskCh    chan Task
	nReduce   int
	taskStats []TaskStat
	done      bool
}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

func (m *Master) initReduceTask() {
	// fmt.Println("init reduce task")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMap:     len(m.files),
		Phase:    m.taskPhase,
		Seq:      taskSeq,
	}

	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}

	return task
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	finish := true
	// fmt.Printf("%+v\n", m.taskStats)
	// fmt.Printf("%+v\n", len(m.taskCh))
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady:
			finish = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			finish = false
		case TaskStatusRunning:
			finish = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinished:
		case TaskStatusError:
			finish = false
			// fmt.Printf("fuckccckkckckckckckck")
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		}
	}

	if finish {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) regTask(args *RequestTaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.taskStats[task.Seq].StartTime = time.Now()
	m.taskStats[task.Seq].WorkerID = args.WorkerID
	m.taskStats[task.Seq].Status = TaskStatusRunning
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WorkerCnt++
	reply.WorkerID = m.WorkerCnt

	// fmt.Printf("Register WorkerID=%d\n", reply.WorkerID)

	return nil
}

func (m *Master) ReqTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	t := <-m.taskCh
	reply.Task = &t

	m.regTask(args, &t)

	// fmt.Printf("Send task %+v\n", *reply.Task)

	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// fmt.Printf("report %+v\n", args)
	if args.Done {
		m.taskStats[args.Task.Seq].Status = TaskStatusFinished
	} else {
		m.taskStats[args.Task.Seq].Status = TaskStatusError
	}

	// fmt.Printf("Report task %+v", args.Task)
	go m.schedule()

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
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.done

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.files = files
	m.nReduce = nReduce
	m.done = false
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(files))
	}

	m.initMapTask()

	m.server()
	go m.tickSchedule()

	return &m
}

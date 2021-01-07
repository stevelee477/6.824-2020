package mr

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	Phase    TaskPhase
	WorkerID int
	NReduce  int
	NMap     int
	Seq      int
}

package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

const (
	WaitCmdTimeout = time.Millisecond * 500
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type NotifyMsg struct {
	Err   Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MsgId    int64
	Seq      int64
	ClientId int64
	Op       string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	stopCh   chan bool
	data     map[string]string
	lastSeq  map[int64]int64
	notifyCh map[int64]chan NotifyMsg

	lockStart time.Time
	lockEnd   time.Time
	lockName  string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		MsgId:    nrand(),
		Seq:      args.Seq,
		Key:      args.Key,
		Op:       "Get",
		ClientId: args.ClientId,
	}
	res := kv.waitCmdApply(op)
	reply.Err = res.Err
	reply.Value = res.Value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		MsgId:    nrand(),
		Seq:      args.Seq,
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		ClientId: args.ClientId,
	}
	res := kv.waitCmdApply(op)
	reply.Err = res.Err
	return
}

func (kv *KVServer) waitCmdApply(op Op) (res NotifyMsg) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	DPrintf("wait cmd %v %v clientid %v seq %v\n", op.Op, op.Key, op.ClientId, op.Seq)

	kv.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	kv.notifyCh[op.MsgId] = ch
	kv.mu.Unlock()

	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()
	select {
	case res = <-ch:
		DPrintf("applied cmd %v %v clientid %v seq %v\n", op.Op, op.Key, op.ClientId, op.Seq)
		kv.mu.Lock()
		delete(kv.notifyCh, op.MsgId)
		kv.mu.Unlock()
		return
	case <-t.C:
		DPrintf("timeout wait cmd %v %v clientid %v seq %v\n", op.Op, op.Key, op.ClientId, op.Seq)
		res.Err = ErrApplyTimeout
		kv.mu.Lock()
		delete(kv.notifyCh, op.MsgId)
		kv.mu.Unlock()
		return
	}
}

func (kv *KVServer) applyDeamon() {
	for !kv.killed() {
		select {
		case <-kv.stopCh:
			return

		case msg := <-kv.applyCh:
			// index := msg.CommandIndex
			if !msg.CommandValid {
				//是一个Snapshot
				kv.mu.Lock()
				kv.readPersist(kv.persister.ReadSnapshot())
				kv.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)

			kv.mu.Lock()
			isRepeated := op.Seq == kv.lastSeq[op.ClientId]
			if isRepeated {
				DPrintf("duplicated %v %v\n", op.Op, op.Key)
			}
			switch op.Op {
			case "Put":
				if !isRepeated {
					kv.data[op.Key] = op.Value
					kv.lastSeq[op.ClientId] = op.Seq
					DPrintf("lastSeq %v to %v\n", op.ClientId, kv.lastSeq[op.ClientId])
				}
			case "Append":
				if !isRepeated {
					v, _ := kv.data[op.Key]
					kv.data[op.Key] = v + op.Value
					kv.lastSeq[op.ClientId] = op.Seq
					DPrintf("lastSeq %v to %v\n", op.ClientId, kv.lastSeq[op.ClientId])
				}
			case "Get":
			}

			kv.saveSnapshot(msg.CommandIndex)
			if ch, ok := kv.notifyCh[op.MsgId]; ok {
				v, _ := kv.data[op.Key]
				ch <- NotifyMsg{
					Err:   OK,
					Value: v,
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) saveSnapshot(appliedId int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate*9/10 {
		DPrintf("triggered snapshot")
		data := kv.encodeSnapshot()
		kv.rf.SaveSnapshot(appliedId, data)
	}
	return
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastSeq)
	return w.Bytes()
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string
	var lastSeq map[int64]int64

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastSeq) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastSeq = lastSeq
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.stopCh = make(chan bool, 1)
	kv.data = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.notifyCh = make(map[int64]chan NotifyMsg)
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.applyDeamon()

	return kv
}

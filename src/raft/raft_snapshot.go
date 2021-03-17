package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SaveSnapshot(appliedId int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if appliedId <= rf.lastIncludedIndex {
		return
	}

	lastLog := rf.getLogByIndex(appliedId)
	rf.log = rf.log[rf.getIndex(appliedId):]
	rf.lastIncludedIndex = appliedId
	rf.lastIncludedTerm = lastLog.Term
	persistState := rf.getPersistState()
	rf.persister.SaveStateAndSnapshot(persistState, data)
}

func (rf *Raft) sendInstallSnapshot(server int) bool {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	for {
		reply := InstallSnapshotReply{}
		ch := make(chan bool, 1)
		timer := time.NewTimer(RPCTimeout)
		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
			ch <- ok
		}()
		select {
		case <-rf.stopCh:
			return false
		case <-timer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			}
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return false
			}
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				rf.mu.Unlock()
				return false
			}
			if args.LastIncludedIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = args.LastIncludedIndex
			}
			if args.LastIncludedIndex+1 > rf.nextIndex[server] {
				rf.nextIndex[server] = args.LastIncludedIndex + 1
			}
			rf.mu.Unlock()
			return true
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d get snapshot to %v\n", rf.me, args.LastIncludedIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm || rf.role != Folllower {
		rf.stepDown(args.Term)
	}
	rf.resetElectionTimer()

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	start := args.LastIncludedIndex - rf.lastIncludedIndex
	if start >= len(rf.log) {
		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = args.LastIncludedTerm
	} else {
		rf.log = rf.log[start:]
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
}

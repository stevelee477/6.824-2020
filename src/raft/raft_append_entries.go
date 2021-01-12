package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// later use

	// PrevLogIndex int
	// PrevLogTerm  int
	// entries      []LogEntry
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("node %v at term %v response to %v\n", rf.me, rf.currentTerm, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	rf.role = Folllower
	rf.currentTerm = args.Term
	reply.Success = true
	rf.resetElectionTimer()
}

func (rf *Raft) sendAppendEntries(server int) bool {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ch := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		ch <- ok
	}()
	time.Sleep(RPCTimeout)
	if <-ch {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		}
		rf.mu.Unlock()
		return true
	}

	return false
}

func (rf *Raft) sendHeartbeat() {
	for idx := range rf.peers {
		if idx == rf.me {
			rf.resetElectionTimer()
			continue
		}
		go rf.sendAppendEntries(idx)
	}
}

func (rf *Raft) tick() {
	timer := time.NewTimer(HeartbeatTimeout)
	for {
		select {
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				// DPrintf("%v stop tick\n", rf.me)
				return
			}
			// DPrintf("Leader %v tick\n", rf.me)
			go rf.sendHeartbeat()
			timer.Reset(HeartbeatTimeout)
		case <-rf.stopCh:
			return
		}
	}
}

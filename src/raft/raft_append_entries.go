package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v at %v response to %v\n", rf.me, rf.currentTerm, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} else if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply.Success = false
	rf.resetElectionTimer()

	_, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex > lastLogIndex {
		// 缺少log
		DPrintf("%v miss log %v\n", rf.me, lastLogIndex+1)
		reply.ConflictIndex = lastLogIndex + 1 // 要求leader下一次从第一个缺少的记录传输
		reply.ConflictTerm = -1
		rf.persist()
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i, e := range rf.log {
			if e.Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		DPrintf("%v conflict log %v\n", rf.me, args.PrevLogIndex)
		// rf.log = rf.log[:args.PrevLogIndex]
		// rf.persist()
		return
	}

	reply.Success = true
	if args.PrevLogIndex+len(args.Entries) <= rf.commitIndex {
		// 重复log
		return
	}
	i := 0
	// 找到第一个冲突的log，删除之后的内容，用新的覆盖
	for ; i < len(args.Entries); i++ {
		cur := args.PrevLogIndex + 1 + i
		e := args.Entries[i]
		if cur <= lastLogIndex && rf.log[cur].Term != e.Term {
			DPrintf("%v conflict at %v origin %v after %v\n", rf.me, cur, rf.log[cur], e)
			rf.log = rf.log[:cur]
			break
		}
		if cur > lastLogIndex {
			break
		}
	}

	for ; i < len(args.Entries); i++ {
		e := args.Entries[i]
		rf.log = append(rf.log, e)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		DPrintf("%v update commitIndex %v\n", rf.me, rf.commitIndex)
		rf.apply()
	}

	rf.persist()

	if len(args.Entries) != 0 {
		DPrintf("%v got entries up to %v", rf.me, args.PrevLogIndex+len(args.Entries))
	}
}

func (rf *Raft) sendAppendEntries(server int) bool {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	if len(rf.log) > prevLogIndex {
		args.Entries = append([]LogEntry{}, rf.log[prevLogIndex+1:]...)
	}
	// DPrintf("%v append entries prevIndex %v len %v log len %v\n", server, args.PrevLogIndex, len(args.Entries), len(rf.log))
	rf.mu.Unlock()
	count := 0
	for {
		count++
		reply := AppendEntriesReply{}
		ch := make(chan bool, 1)
		timer := time.NewTimer(RPCTimeout)
		go func() {
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			ch <- ok
		}()
		select {
		case r := <-ch:
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return false
			}
			if !r {
				rf.mu.Unlock()
				continue
			}
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					rf.stepDown(reply.Term)
					rf.mu.Unlock()
					return false
				}
				if reply.ConflictIndex == 0 {
					return false
				}
				if reply.ConflictTerm == -1 {
					// 缺少log
					rf.nextIndex[server] = reply.ConflictIndex
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					// log冲突
					conflictIndex := reply.ConflictIndex
					_, lastLogIndex := rf.lastLogTermIndex()
					for i := 0; i < lastLogIndex; i++ { // 找到冲突term的最后一个log之后的index
						if rf.log[i].Term == reply.ConflictTerm {
							for i <= lastLogIndex && rf.log[i].Term == reply.ConflictTerm {
								i++
							}
							conflictIndex = i
							break
						}
					}
					rf.nextIndex[server] = conflictIndex
				}
				DPrintf("update %v nextindex %v\n", server, rf.nextIndex[server])
			} else {
				if rf.nextIndex[server] < prevLogIndex+len(args.Entries)+1 {
					DPrintf("update %v nextindex %v\n", server, prevLogIndex+len(args.Entries)+1)
					rf.nextIndex[server] = prevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
						rf.updateCommitIndex()
					}
				}
			}
			rf.mu.Unlock()
			return true
		case <-timer.C:
			return false
		}
	}
	return false
}

func (rf *Raft) appendEntries() {
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
				DPrintf("%v stop tick\n", rf.me)
				return
			}
			DPrintf("%v tick\n", rf.me)
			go rf.appendEntries()
			timer.Reset(HeartbeatTimeout)
		case <-rf.stopCh:
			return
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	majority := len(rf.peers)/2 + 1
	n := len(rf.log)
	for i := n - 1; i > rf.commitIndex; i-- {
		// 从最后的log向前寻找
		replicated := 0
		if rf.log[i].Term == rf.currentTerm {
			// 只提交当前Term的log
			for server := range rf.peers {
				if rf.matchIndex[server] >= i {
					replicated++
				}
			}
		}

		if replicated >= majority {
			DPrintf("commit %v\n", i)
			rf.commitIndex = i
			// rf.apply()
			rf.applyCond.Broadcast()
			break
		}
	}
}

func (rf *Raft) apply() {
	applied := rf.lastApplied
	logs := append([]LogEntry{}, rf.log[applied+1:rf.commitIndex+1]...)
	rf.lastApplied = rf.commitIndex

	DPrintf("%v start apply %v to %v\n", rf.me, applied+1, rf.commitIndex)
	// DPrintf("%v\n", logs)

	for idx, l := range logs {
		msg := ApplyMsg{
			Command:      l.Command,
			CommandIndex: applied + idx + 1,
			CommandValid: true,
		}
		DPrintf("%v applied log %v\n", rf.me, applied+idx+1)
		rf.applyCh <- msg
	}
}

func (rf *Raft) applyDeamon() {
	for {
		rf.applyCond.Wait()
		rf.mu.Lock()
		applied := rf.lastApplied
		logs := append([]LogEntry{}, rf.log[applied+1:rf.commitIndex+1]...)
		rf.lastApplied = rf.commitIndex

		DPrintf("%v start apply %v to %v\n", rf.me, applied+1, rf.commitIndex)
		// DPrintf("%v\n", logs)

		for idx, l := range logs {
			msg := ApplyMsg{
				Command:      l.Command,
				CommandIndex: applied + idx + 1,
				CommandValid: true,
			}
			DPrintf("%v applied log %v\n", rf.me, applied+idx+1)
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
	}
}

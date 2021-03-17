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

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// 出现了乱序AE，上一个Log比快照早
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = -1
		return
	} else if args.PrevLogIndex > lastLogIndex {
		// 缺少中间log
		DPrintf("%v miss log %v\n", rf.me, lastLogIndex+1)
		reply.ConflictIndex = lastLogIndex + 1 // 要求leader下一次从第一个缺少的记录传输
		reply.ConflictTerm = -1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogIndex+len(args.Entries) <= lastLogIndex && lastLogTerm == args.Term {
			// 重复log
			return
		} else {
			reply.Success = true
			rf.log = append(rf.log[:1], args.Entries...) // 保留 logs[0]
		}
	} else if args.PrevLogTerm != rf.getLogByIndex(args.PrevLogIndex).Term {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		reply.ConflictTerm = rf.getLogByIndex(args.PrevLogIndex).Term
		for i, e := range rf.log {
			if e.Term == reply.ConflictTerm {
				reply.ConflictIndex = i + rf.lastIncludedIndex
				break
			}
		}
		DPrintf("%v conflict log %v\n", rf.me, args.PrevLogIndex)
		return
	}

	//PrevLog相同，开始合并
	reply.Success = true
	if args.PrevLogIndex+len(args.Entries) <= lastLogIndex && lastLogTerm == args.Term {
		// 重复log
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1], args.Entries...)

	if args.LeaderCommit > rf.commitIndex { // 先判断避免AE乱序
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		DPrintf("%v update commitIndex %v\n", rf.me, rf.commitIndex)
		rf.notifyApplyCh <- struct{}{}
	}

	rf.persist()

	if len(args.Entries) != 0 {
		DPrintf("%v got entries up to %v", rf.me, args.PrevLogIndex+len(args.Entries))
	}
}

func (rf *Raft) sendAppendEntries(server int) bool {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if prevLogIndex < rf.lastIncludedIndex || prevLogIndex+1 > lastLogIndex {
		prevLogTerm = lastLogTerm
	} else {
		prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	if lastLogIndex > prevLogIndex && prevLogIndex >= rf.lastIncludedIndex {
		DPrintf("get log for %v prevLogIdx %v lastIncludeIdx %v\n", server, prevLogIndex, rf.lastIncludedIndex)
		args.Entries = append([]LogEntry{}, rf.log[rf.getIndex(prevLogIndex)+1:]...)
		DPrintf("log entried size %v\n", len(args.Entries))
	}
	// DPrintf("%v append entries prevIndex %v len %v log len %v\n", server, args.PrevLogIndex, len(args.Entries), len(rf.log))
	rf.mu.Unlock()
	for !rf.killed() {
		reply := AppendEntriesReply{}
		ch := make(chan bool, 1)
		timer := time.NewTimer(RPCTimeout)
		go func() {
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			ch <- ok
		}()
		select {
		case r := <-ch:
			if !r {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return false
			}
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					rf.stepDown(reply.Term)
					rf.mu.Unlock()
					return false
				}
				if reply.ConflictIndex != 0 {
					if reply.ConflictIndex > rf.lastIncludedIndex {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[server] = reply.ConflictIndex
						} else {
							// log冲突
							conflictIndex := reply.ConflictIndex
							for i := 0; i < len(rf.log); i++ { // 找到冲突term的最后一个log之后的index
								if rf.log[i].Term == reply.ConflictTerm {
									for i <= len(rf.log) && rf.log[i].Term == reply.ConflictTerm {
										i++
									}
									conflictIndex = i + rf.lastIncludedIndex
									break
								}
							}
							rf.nextIndex[server] = conflictIndex
						}
					} else {
						go rf.sendInstallSnapshot(server)
					}
				}
				// DPrintf("update %v nextindex %v\n", server, rf.nextIndex[server])
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
	hasCommit := false
	majority := len(rf.peers)/2 + 1
	// _, lastLogIndex := rf.lastLogTermIndex()
	for i := rf.lastIncludedIndex + len(rf.log) - 1; i > rf.commitIndex; i-- {
		// for i := lastLogIndex; i > rf.commitIndex; i-- {
		// 从最后的log向前寻找
		realIdx := rf.getIndex(i)
		if realIdx < 0 {
			break
		}
		replicated := 0
		if rf.log[realIdx].Term == rf.currentTerm {
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
			hasCommit = true
			break
		}
	}

	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) apply() {
	defer rf.applyTimer.Reset(ApplyInterval)
	rf.mu.Lock()
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastIncludedIndex {
		msgs = make([]ApplyMsg, 0, 1)
		msgs = append(msgs, ApplyMsg{
			CommandValid: false,
			Command:      "installSnapShot",
			CommandIndex: rf.lastIncludedIndex,
		})
	} else if rf.commitIndex <= rf.lastApplied {
		msgs = make([]ApplyMsg, 0)
	} else {
		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				Command:      rf.getLogByIndex(i).Command,
			})
		}
	}
	rf.mu.Unlock()

	for _, m := range msgs {
		DPrintf("%d applied %v\n", rf.me, m.CommandIndex)
		rf.mu.Lock()
		rf.lastApplied = m.CommandIndex
		rf.mu.Unlock()
		rf.applyCh <- m
	}
}

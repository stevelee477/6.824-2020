package raft

import (
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v is requested to vote %v at term %v\n", rf.me, args.CandidateId, rf.currentTerm)

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.persist()

	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == Leader {
			return
		}
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
		}
	}

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		DPrintf("%v not vote to %v\n", rf.me, args.CandidateId)
		return
	}

	rf.currentTerm = args.Term
	rf.role = Folllower
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.resetElectionTimer()
	rf.persist()
	DPrintf("%v vote to %v\n", rf.me, args.CandidateId)
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	count := 1
	for {
		count++
		timer := time.NewTimer(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			ch <- ok
		}()
		select {
		case ok := <-ch:
			rf.mu.Lock()
			if rf.role != Candidate {
				rf.mu.Unlock()
				return false
			}
			rf.mu.Unlock()
			if ok {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return ok
			}
		case <-timer.C:
			continue
		case <-rf.stopCh:
			return false
		}
	}
	return false
}

func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}

	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	DPrintf("%v at %v start election\n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.mu.Unlock()

	timer := time.NewTimer(ElectionTimeout)

	grantedCount := 1
	voteCount := 1
	votesCh := make(chan RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if idx == rf.me {
			rf.resetElectionTimer()
			continue
		}
		go func(ch chan RequestVoteReply, idx int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(idx, &args, &reply)
			ch <- reply
		}(votesCh, idx)
	}

L:
	for {
		select {
		case r := <-votesCh:
			rf.mu.Lock()
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if r.Term > args.Term {
				rf.mu.Lock()
				if r.Term > rf.currentTerm {
					rf.stepDown(r.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
			voteCount++
			if r.VoteGranted == true {
				grantedCount++
			}
			if voteCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || voteCount-grantedCount > len(rf.peers)/2 {
				break L
			}
		case <-timer.C:
			DPrintf("%v election timeout\n", rf.me)
			rf.mu.Lock()
			rf.role = Folllower
			// rf.votedFor = -1
			rf.mu.Unlock()
			return
		case <-rf.stopCh:
			return
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		DPrintf("%v failed become leader with granted %v\n", rf.me, grantedCount)
		rf.mu.Lock()
		rf.role = Folllower
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	if args.Term == rf.currentTerm && rf.role == Candidate {
		DPrintf("%v become leader\n", rf.me)
		rf.initLeader()
		// rf.resetElectionTimer() //?????
		rf.role = Leader
		go rf.tick()
	}
	rf.mu.Unlock()
}

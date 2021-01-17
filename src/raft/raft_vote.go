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
	DPrintf("%v vote to %v\n", rf.me, args.CandidateId)
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	timer := time.NewTimer(RPCTimeout)
	ch := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()
	select {
	case r := <-ch:
		return r
	case <-timer.C:
		return false
	}
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
	votesCh := make(chan bool, len(rf.peers))
	for idx := range rf.peers {
		if idx == rf.me {
			rf.resetElectionTimer()
			continue
		}
		go func(ch chan bool, idx int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(idx, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.stepDown(reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}(votesCh, idx)
	}

L:
	for {
		select {
		case r := <-votesCh:
			voteCount++
			if r == true {
				grantedCount++
				rf.resetElectionTimer()
			}
			if voteCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || voteCount-grantedCount > len(rf.peers)/2 {
				break L
			}
		case <-timer.C:
			DPrintf("%v election timeout\n", rf.me)
			return
		case <-rf.stopCh:
			return
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		DPrintf("%v failed become leader\n", rf.me)
		rf.mu.Lock()
		rf.role = Folllower
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	if args.Term == rf.currentTerm && rf.role == Candidate {
		DPrintf("%v become leader\n", rf.me)
		rf.initLeader()
		rf.role = Leader
		go rf.tick()
	}
	rf.mu.Unlock()
}

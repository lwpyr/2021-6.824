package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

//
// A Go object implementing a single Raft peer.
//
const (
	CONTROL = iota
	DATA
)
type RFLog struct {
	LogType int
	LogTerm int
	Message ApplyMsg
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State
	ID int
	State int

	// Persist
	CurrentTerm int
	VotedFor int
	Log []RFLog

	// Volatile
	CommitIndex int
	LastApplied int
	Cursor int

	// Volatile for leader
	NextIndex []int
	MatchedIndex []int

	// Volatile for candidate
	VoteCount int

	// Interface
	Inbox chan ApplyMsg

	// time stamp
	ElectionTimestamp time.Time

	// leader
	ApplyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isLeader = rf.State == LEADER
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
type PersistState struct {
	CurrentTerm int `json:"current_term"`
	VotedFor int `json:"voted_for"`
	Log []RFLog `json:"log"`
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	p := PersistState{
		CurrentTerm: rf.CurrentTerm,
		VotedFor:    rf.VotedFor,
		Log:         rf.Log,
	}
	data, _ := json.Marshal(p)
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	p := PersistState{}
	json.Unmarshal(data, &p)
	rf.CurrentTerm = p.CurrentTerm
	rf.VotedFor = p.VotedFor
	rf.Log = p.Log
	rf.Cursor = len(rf.Log) - 1
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dec := "agreed"
	if reply.VoteGranted == false {
		dec = "denied"
	}
	rf.Logf("receive vote request from[%d]: " + dec, args.CandidateId)
	if args.Term > rf.CurrentTerm {
		rf.resetFollower(args.Term)
	} else if rf.State == FOLLOWER {
		rf.resetFollower(rf.CurrentTerm)
	}
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
			(args.LastLogTerm > rf.Log[rf.Cursor].LogTerm ||
			args.LastLogTerm == rf.Log[rf.Cursor].LogTerm && args.LastLogIndex >= rf.Cursor) {
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []RFLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Cursor int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	reply.Term = args.Term
	rf.resetFollower(args.Term)
	if args.PrevLogIndex > rf.Cursor || args.PrevLogTerm != rf.Log[args.PrevLogIndex].LogTerm {
		reply.Success = false
		return
	}
	if len(args.Entries) == 0 {
		rf.Logf("receive heartbeat from %d, %v", args.LeaderId, args)
	} else {
		rf.Logf("receive appendEntries from %d, %v", args.LeaderId, args)
	}
	rf.Log = rf.Log[:args.PrevLogIndex + 1]
	rf.Log = append(rf.Log, args.Entries...)
	rf.Cursor = len(rf.Log) - 1
	if rf.CommitIndex < args.LeaderCommit {
		if rf.Cursor < args.LeaderCommit {
			rf.CommitIndex = rf.Cursor
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		rf.Logf("follower update commitIdx %d", rf.CommitIndex)
	}
	reply.Cursor = rf.Cursor
	reply.Success = true
}

func (rf *Raft) HandleElectionTimeout(last time.Time) time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.ElectionTimestamp == last {
		switch rf.State {
		case LEADER:
			rf.ElectionTimestamp = time.Now()
		case CANDIDATE, FOLLOWER:
			rf.startElection()
		}
	}
	return rf.ElectionTimestamp
}


func (rf *Raft) startElection() {
	rf.State = CANDIDATE
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.ElectionTimestamp = time.Now()
	rf.Logf("start election")

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.Cursor,
		LastLogTerm:  rf.Log[rf.Cursor].LogTerm,
	}

	rf.VoteCount = 1

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		idx := idx
		go func() {
			rf.mu.Lock()
			if rf.State != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			if rf.sendRequestVote(idx, &args, &reply) {
				rf.ReceiveVote(idx, &reply)
			}
		}()
	}
}

func (rf *Raft) ReceiveVote(idx int, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > reply.Term {
		return
	}
	if rf.CurrentTerm < reply.Term {
		rf.Logf("receive vote from %d[Term %d, Agree %t], term is higher than candidate", idx, reply.Term, reply.VoteGranted)
		rf.resetFollower(reply.Term)
	}
	if rf.State == CANDIDATE {
		if reply.VoteGranted && rf.CurrentTerm == reply.Term {
			rf.VoteCount++
			rf.Logf("receive vote from %d[Term %d, Agree %t], current voted %d", idx, reply.Term, reply.VoteGranted, rf.VoteCount)
			if rf.VoteCount == len(rf.peers) / 2 + 1 {
				go rf.StartLeaderTerm()
			}
			return
		}
	}
}

func (rf *Raft) StartLeaderTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	termPeriod := time.Duration(rand.Int() % 1500 + 1500) * time.Millisecond
	rf.State = LEADER
	rf.Logf("start leader term with a time period %d ms", termPeriod.Milliseconds())
	rf.ElectionTimestamp = time.Now()
	rf.InitPeerState()
	rf.MatchedIndex[rf.me] = rf.Cursor
	rf.StartHeartbeatTicker()
	// go rf.LeaderTicker(termPeriod)
}

func (rf *Raft) InitPeerState() {
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchedIndex = make([]int, len(rf.peers))
	for i:=0; i<len(rf.peers); i++ {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchedIndex[i] = 0
	}
}

func (rf *Raft) LeaderTicker(d time.Duration) {
	rf.Logf("start term leader ticker")
	time.Sleep(d)
	rf.Logf("term leader lease expired")
	rf.resetFollower(rf.CurrentTerm)
}

func (rf *Raft) resetFollower(term int) {
	rf.State = FOLLOWER
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.ElectionTimestamp = time.Now()
	rf.Logf("reset as follower")
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	rf.Logf("send vote request to %d, current vote count %d", server, rf.VoteCount)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if len(args.Entries) == 0 {
		rf.Logf("send heartbeat to peer[%d]", server)
	} else {
		rf.Logf("appendEntry to peer[%d], %v", server, args)
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logf("receive command %v", command)
	index := -1
	term := -1
	isLeader := rf.State == LEADER

	// Your code here (2B).
	if isLeader {
		rf.Cursor++
		index = rf.Cursor
		term = rf.CurrentTerm
		rf.MatchedIndex[rf.me] = rf.Cursor
		rf.Log = append(rf.Log, RFLog{
			LogType: DATA,
			LogTerm: rf.CurrentTerm,
			Message: ApplyMsg{
				CommandValid:  true,
				Command:       command,
				CommandIndex:  rf.Cursor,
			},
		})
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	rf.Logf("start ticker")
	rf.ElectionTimestamp = time.Now()
	last := rf.ElectionTimestamp
	rf.mu.Unlock()

	for rf.killed() == false {
		randomTime := time.Duration(rand.Int() % 250 + 250) * time.Millisecond
		time.Sleep(time.Until(last.Add(randomTime)))

		last = rf.HandleElectionTimeout(last)
	}
}

func (rf *Raft) SendHeartBeatsToPeer(idx int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != LEADER {
			rf.mu.Unlock()
			break
		}
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex,
			PrevLogIndex: rf.NextIndex[idx]-1,
			PrevLogTerm:  rf.Log[rf.NextIndex[idx]-1].LogTerm,
		}
		if rf.Cursor >= rf.NextIndex[idx] {
			args.Entries = rf.Log[rf.NextIndex[idx]: rf.Cursor+1]
		}
		rf.mu.Unlock()
		var reply AppendEntriesReply
		if rf.sendAppendEntries(idx, &args, &reply) {
			rf.ReceiveAppendResponse(idx, &args, &reply)
		} else {
			rf.mu.Lock()
			rf.Logf("failed to receive appendEntries response from %d", idx)
			rf.mu.Unlock()
		}
		randomTime := time.Duration(rand.Int() % 50 + 50) * time.Millisecond
		time.Sleep(randomTime)
	}
}

func (rf *Raft) ReceiveAppendResponse(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logf("receive appendEntries response from %d, %v", idx, reply)
	if reply.Term > rf.CurrentTerm {
		rf.resetFollower(reply.Term)
	}
	if reply.Success {
		if reply.Cursor > rf.MatchedIndex[idx] {
			rf.MatchedIndex[idx] = reply.Cursor
			rf.NextIndex[idx] = rf.MatchedIndex[idx] + 1
			rf.UpdateCommitIdx()
		}
	} else {
		rf.NextIndex[idx]--
		if rf.NextIndex[idx] == 0 {
			rf.NextIndex[idx] = 1
		}
	}
}

func (rf *Raft) UpdateCommitIdx() {
	matches := make([]int, len(rf.peers))
	for i:=0; i<len(rf.peers); i++ {
		matches[i] = rf.MatchedIndex[i]
	}
	sort.Slice(matches, func(i int, j int) bool {
		return matches[i] > matches[j]
	})
	rf.CommitIndex = matches[len(rf.peers)/2]
	rf.Logf("update commitIdx %d, from %v", rf.CommitIndex, rf.MatchedIndex)
}

func (rf *Raft) applyCron() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CommitIndex > rf.Cursor {
			panic("range error")
		}
		for rf.LastApplied < rf.CommitIndex {
			rf.LastApplied++
			rf.ApplyChan <- rf.Log[rf.LastApplied].Message
			rf.Logf("apply message %v", rf.Log[rf.LastApplied].Message)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartHeartbeatTicker() {
	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {continue}
		go rf.SendHeartBeatsToPeer(i)
	}
}

func (rf *Raft) Logf(format string, a ...interface{}) {
	var state string
	switch rf.State {
	case LEADER:
		state = "Leader"
	case FOLLOWER:
		state = "Follower"
	case CANDIDATE:
		state = "candidate"
	}
	prefix := fmt.Sprintf("Server[%d](Term-%d %s, A%d C%d P%d): ", rf.me, rf.CurrentTerm, state, rf.LastApplied, rf.CommitIndex, rf.Cursor)
	_, _ = DPrintf(prefix+format+"\n", a...)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.State = FOLLOWER

	rf.CurrentTerm = 0
	rf.Log = []RFLog{{
		LogType: DATA,
		LogTerm: 0,
		Message: ApplyMsg{
			Command:       "Raft server start",
		},
	}}
	rf.VotedFor = -1

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Cursor = 0

	rf.ElectionTimestamp = time.Now()
	rf.ApplyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCron()

	return rf
}

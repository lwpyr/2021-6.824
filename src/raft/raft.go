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
	"6.824/labgob"
	"bytes"
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

	// Your Data here (2A, 2B, 2C).
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
	CompactedLen int
	CommitIndex  int
	LastApplied  int
	Cursor       int

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

	// snapshot
	SnapshotData []byte
	IncludeIndex int
	IncludeTerm  int
}

func (rf *Raft) LogLen() int {
	return len(rf.Log) + rf.CompactedLen
}

func (rf *Raft) LogAt(idx int) *RFLog {
	return &rf.Log[idx - rf.CompactedLen]
}

func (rf *Raft) LogTermAt(idx int) int {
	if idx == rf.IncludeIndex {
		return rf.IncludeTerm
	} else if idx - rf.CompactedLen < len(rf.Log) {
		return rf.Log[idx - rf.CompactedLen].LogTerm
	} else {
		return -2
	}
}

func (rf *Raft) LogRange0(start int) []RFLog {
	return rf.Log[start- rf.CompactedLen:]
}

func (rf *Raft) LogRange1(start int, end int) []RFLog {
	return rf.Log[start - rf.CompactedLen: end - rf.CompactedLen]
}

func (rf *Raft) LogRange2(end int) []RFLog {
	return rf.Log[: end - rf.CompactedLen]
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
	CompactedLen int `json:"compacted_len"`
	IncludeIndex int `json:"include_index"`
	IncludeTerm int `json:"include_term"`
	SnapshotData []byte `json:"snapshot_data"`
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	p := PersistState{
		CurrentTerm: rf.CurrentTerm,
		VotedFor:    rf.VotedFor,
		Log:         rf.Log,
		CompactedLen: rf.CompactedLen,
		IncludeIndex: rf.IncludeIndex,
		IncludeTerm: rf.IncludeTerm,
		SnapshotData: rf.SnapshotData,
	}
	if e.Encode(p) != nil  {
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.Logf("read nil persist")
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var p PersistState
	if d.Decode(&p) != nil {
	  rf.Logf("read persist fail")
	  return
	}
	rf.Logf("read persist %+v", p)
	rf.CurrentTerm = p.CurrentTerm
	rf.VotedFor = p.VotedFor
	rf.Log = p.Log
	rf.CompactedLen = p.CompactedLen
	if p.IncludeIndex != -1 {
		rf.LastApplied = p.IncludeIndex
	}
	rf.IncludeIndex = p.IncludeIndex
	rf.IncludeTerm = p.IncludeTerm
	rf.SnapshotData = p.SnapshotData
	rf.Cursor = rf.LogLen() - 1
	rf.Logf("read persist success")
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CompactedLen > lastIncludedIndex + 1 {
		return false
	}

	if lastIncludedIndex <= rf.LastApplied && lastIncludedTerm == rf.LogTermAt(lastIncludedIndex) {
		rf.Log = rf.LogRange0(lastIncludedIndex+1)
		rf.CompactedLen = lastIncludedIndex + 1
		rf.Logf("agree to receive conditional install snapshot")
	} else {
		rf.Log = rf.Log[:0]
		rf.CompactedLen = lastIncludedIndex + 1
		rf.SnapshotData = snapshot
		rf.IncludeIndex = lastIncludedIndex
		rf.IncludeTerm = lastIncludedTerm
		rf.Cursor = lastIncludedIndex
		rf.CommitIndex = lastIncludedIndex
		rf.LastApplied = lastIncludedIndex
		rf.Logf("refuse to receive conditional install snapshot")
	}
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index + 1 > rf.CompactedLen {
		rf.IncludeTerm = rf.LogTermAt(index)
		rf.IncludeIndex = index
		rf.Log = rf.LogRange0(index+1)
		rf.CompactedLen = index + 1
		rf.SnapshotData = snapshot
	}
	rf.Logf("snapshot %d completed, %d-%d", index, rf.IncludeIndex, rf.IncludeTerm)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.CurrentTerm {
		rf.resetFollower(args.Term)
	}
	if args.Term < rf.CurrentTerm || rf.State != FOLLOWER {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
			(args.LastLogTerm > rf.LogTermAt(rf.Cursor) ||
			args.LastLogTerm == rf.LogTermAt(rf.Cursor) && args.LastLogIndex >= rf.Cursor) {
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			rf.resetFollower(rf.CurrentTerm)
		} else {
			reply.VoteGranted = false
		}
	}
	dec := "AGREE"
	if reply.VoteGranted == false {
		dec = "DENIED"
	}
	rf.Logf(dec + " vote request from peer[%d], %+v", args.CandidateId, args)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	reply.Term = args.Term
	rf.resetFollower(args.Term)
	if args.PrevLogIndex > rf.Cursor || args.PrevLogTerm != rf.LogTermAt(args.PrevLogIndex) {
		reply.Success = false
		return
	}
	if len(args.Entries) == 0 {
		rf.Logf("receive heartbeat from %d, %+v", args.LeaderId, args)
	} else {
		rf.Logf("receive appendEntries from %d, length %d", args.LeaderId, len(args.Entries))
	}
	update := false
	if args.PrevLogIndex + len(args.Entries) > rf.Cursor {
		update = true
	} else {
		for idx, log := range args.Entries {
			if log.LogTerm != rf.LogTermAt(args.PrevLogIndex + idx + 1) {
				update = true
				break
			}
		}
	}
	if update {
		rf.Log = rf.LogRange2(args.PrevLogIndex+1)
		rf.Log = append(rf.Log, args.Entries...)
		rf.Cursor = rf.LogLen() - 1
	}
	if rf.CommitIndex < args.LeaderCommit {
		if rf.Cursor < args.LeaderCommit {
			rf.CommitIndex = rf.Cursor
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		rf.applyToCommit()
		rf.Logf("follower update commitIdx %d", rf.CommitIndex)
	}
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	rf.resetFollower(args.Term)
	reply.Term = args.Term
	if args.LastIncludeIndex <= rf.LastApplied && args.LastIncludeTerm == rf.LogTermAt(args.LastIncludeIndex) {
		rf.Log = rf.LogRange0(args.LastIncludeIndex+1)
		rf.CompactedLen = args.LastIncludeIndex + 1
		rf.Logf("snapshot[index-%d, term-%d], trim log", args.LastIncludeIndex, args.LastIncludeTerm)
	} else {
		rf.Log = rf.Log[:0]
		rf.CompactedLen = args.LastIncludeIndex + 1
		rf.SnapshotData = args.Data
		rf.IncludeIndex = args.LastIncludeIndex
		rf.IncludeTerm = args.LastIncludeTerm
		rf.Cursor = args.LastIncludeIndex
		rf.CommitIndex = args.LastIncludeIndex
		rf.LastApplied = args.LastIncludeIndex
		rf.CurrentTerm = args.Term
		rf.Logf("snapshot[index-%d, term-%d], apply snapshot", args.LastIncludeIndex, args.LastIncludeTerm)
	}
}


func (rf *Raft) HandleElectionTimeout(last time.Time) time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
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
		LastLogTerm:  rf.LogTermAt(rf.Cursor),
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
			if rf.SyncRequestVote(idx, &args, &reply) {
				rf.ReceiveVote(idx, &args, &reply)
			}
		}()
	}
}

func (rf *Raft) ReceiveVote(idx int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.CurrentTerm != args.Term || rf.State != CANDIDATE {
		return
	}
	if rf.CurrentTerm < reply.Term {
		rf.Logf("receive vote from %d[Term %d, Agree %t], term is higher than candidate", idx, reply.Term, reply.VoteGranted)
		rf.resetFollower(reply.Term)
		return
	}
	if rf.State == CANDIDATE && reply.VoteGranted && rf.CurrentTerm == reply.Term {
		rf.VoteCount++
		rf.Logf("receive vote from %d[Term %d, Agree %t], current voted %d", idx, reply.Term, reply.VoteGranted, rf.VoteCount)
		if rf.VoteCount == len(rf.peers) / 2 + 1 {
			go rf.StartLeaderTerm()
		}
		return
	}
}

func (rf *Raft) StartLeaderTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
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
		rf.NextIndex[i] = rf.LogLen()
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
	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.VotedFor = -1
	}
	rf.ElectionTimestamp = time.Now()
	rf.Logf("reset as follower")
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	index := -1
	term := -1
	isLeader := rf.State == LEADER

	// Your code here (2B).
	if isLeader {
		rf.Logf("receive command %+v", command)
		rf.Cursor++
		index = rf.Cursor
		term = rf.CurrentTerm
		rf.MatchedIndex[rf.me] = rf.Cursor
		rf.Log = append(rf.Log, RFLog{
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
		if rf.NextIndex[idx] < rf.CompactedLen {
			args := InstallSnapshotArgs{
				Term:             rf.CurrentTerm,
				LeaderId:         rf.me,
				LastIncludeIndex: rf.IncludeIndex,
				LastIncludeTerm:  rf.IncludeTerm,
				Data:             rf.SnapshotData,
			}
			rf.mu.Unlock()
			var reply InstallSnapshotReply
			c := make(chan struct{}, 1)
			go rf.AsyncInstallSnapshot(idx, &args, &reply, c)
			select {
			case <-time.NewTimer(50 * time.Millisecond).C:
				rf.mu.Lock()
				if rf.State == LEADER || rf.CurrentTerm == args.Term {
					rf.Logf("failed to receive appendEntries response from %d", idx)
				}
				rf.mu.Unlock()
			case <-c:
				rf.ReceiveInstallSnapshotResponse(idx, &args, &reply)
				randomTime := time.Duration(rand.Int()%25+25) * time.Millisecond
				time.Sleep(randomTime)
			}
		} else {
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.CommitIndex,
				PrevLogIndex: rf.NextIndex[idx] - 1,
				PrevLogTerm:  rf.LogTermAt(rf.NextIndex[idx] - 1),
			}
			if rf.Cursor >= rf.NextIndex[idx] {
				args.Entries = rf.LogRange0(rf.NextIndex[idx])
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			c := make(chan struct{}, 1)
			go rf.AsyncAppendEntries(idx, &args, &reply, c)
			select {
			case <-time.NewTimer(50 * time.Millisecond).C:
				rf.mu.Lock()
				if rf.State == LEADER || rf.CurrentTerm == args.Term {
					rf.Logf("failed to receive appendEntries response from %d", idx)
				}
				rf.mu.Unlock()
			case <-c:
				rf.ReceiveAppendResponse(idx, &args, &reply)
				randomTime := time.Duration(rand.Int()%25+25) * time.Millisecond
				time.Sleep(randomTime)
			}
		}
	}
}

func (rf *Raft) ReceiveInstallSnapshotResponse(idx int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.State != LEADER || rf.CurrentTerm != args.Term {
		return
	}
	rf.Logf("response of InstallSnapshot from %d, %+v", idx, reply)
	if reply.Term > rf.CurrentTerm {
		rf.resetFollower(reply.Term)
		return
	}
	if args.LastIncludeIndex > rf.MatchedIndex[idx] {
		rf.MatchedIndex[idx] = args.LastIncludeIndex
		rf.NextIndex[idx] = rf.MatchedIndex[idx] + 1
		rf.UpdateCommitIdx()
	}
}

func (rf *Raft) ReceiveAppendResponse(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.State != LEADER || rf.CurrentTerm != args.Term {
		return
	}
	rf.Logf("receive appendEntries response from %d, %+v", idx, reply)
	if reply.Term > rf.CurrentTerm {
		rf.resetFollower(reply.Term)
		return
	}
	if reply.Success {
		followerCursor := args.PrevLogIndex + len(args.Entries)
		if followerCursor > rf.MatchedIndex[idx] {
			rf.MatchedIndex[idx] = followerCursor
			rf.NextIndex[idx] = rf.MatchedIndex[idx] + 1
			rf.UpdateCommitIdx()
		}
	} else {
		rf.NextIndex[idx] -= 50
		if rf.NextIndex[idx] < 1 {
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
	temp := matches[len(rf.peers)/2]
	if temp > rf.CommitIndex && rf.LogTermAt(temp) == rf.CurrentTerm{
		rf.CommitIndex = temp
		rf.applyToCommit()
		rf.Logf("update commitIdx %d, from %+v", rf.CommitIndex, rf.MatchedIndex)
	}
}

func (rf *Raft) applyToCommit() {
	if rf.CommitIndex > rf.Cursor || rf.LastApplied > rf.CommitIndex{
		panic("range error")
	}
	for rf.LastApplied < rf.CommitIndex {
		if rf.LastApplied == rf.IncludeIndex {
			rf.mu.Unlock()
			rf.ApplyChan <- ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.SnapshotData,
				SnapshotTerm:  rf.IncludeTerm,
				SnapshotIndex: rf.IncludeIndex,
			}
			rf.mu.Lock()
		}
		rf.LastApplied++
		rf.Logf("apply message %+v", rf.LogAt(rf.LastApplied).Message)
		log := rf.LogAt(rf.LastApplied)
		rf.mu.Unlock()
		rf.ApplyChan <- log.Message
		rf.mu.Lock()
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
		state = "Leader   "
	case FOLLOWER:
		state = "Follower "
	case CANDIDATE:
		state = "candidate"
	}
	// [No.?]RAFT(term?-role applied<committed<cursor snapshot[includeIndex(?), includeTerm(?)] nextIndex-?)
	prefix := fmt.Sprintf("[%d]RAFT(T%d-%s %d<%d<%d snapshot[IDX(%d) T(%d)], next-%v): ", rf.me, rf.CurrentTerm, state, rf.LastApplied, rf.CommitIndex, rf.Cursor, rf.IncludeIndex, rf.IncludeTerm, rf.NextIndex)
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
		LogTerm: 0,
		Message: ApplyMsg{
			Command:       "Raft server start",
		},
	}}
	rf.VotedFor = -1

	rf.CompactedLen = 0
	rf.IncludeIndex = -1
	rf.IncludeTerm = -1

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Cursor = 0

	rf.ElectionTimestamp = time.Now()
	rf.ApplyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

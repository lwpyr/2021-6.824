package raft

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
	Success bool
}

type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your Data here (2A).
	Term int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SyncRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	rf.Logf("send vote request to %d, current vote count %d", server, rf.VoteCount)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AsyncAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, c chan struct{}) {
	rf.mu.Lock()
	if len(args.Entries) == 0 {
		rf.Logf("send heartbeat to peer[%d], %+v", server, *args)
	} else {
		rf.Logf("appendEntry to peer[%d], length %d", server, len(args.Entries))
	}
	rf.mu.Unlock()
	if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		c <- struct{}{}
	}
}

func (rf *Raft) AsyncInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, c chan struct{}) {
	rf.mu.Lock()
	rf.Logf("send snap to peer[%d]", server)
	rf.mu.Unlock()
	if rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		c <- struct{}{}
	}
}

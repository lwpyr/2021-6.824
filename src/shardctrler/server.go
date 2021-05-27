package shardctrler


import (
	"6.824/raft"
	"fmt"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	STABLE = iota
	PREPARE
	EXECUTING
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	State int // STABLE -(client cmd)-> PREPARE -(all RGs ACK)-> EXECUTING -(all RGs ACK/RPC complete)-> STABLE

	configs []Config // indexed by config num
	clients map[int]int // at most once

	notifier map[int]chan OpRes
	notifierLock sync.Mutex
}

type Op struct {
	// Your data here.
	Cmd interface{}
	Timestamp int
}

type OpRes struct {
	Err Err
	Config Config
}

func (sc *ShardCtrler) Logf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%d]MGR(version-%d): ", sc.me, len(sc.configs)-1)
	_, _ = DPrintf(prefix+format+"\n", a...)
}

func (sc *ShardCtrler) LockLogf(format string, a ...interface{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	prefix := fmt.Sprintf("[%d]MGR(version-%d): ", sc.me, len(sc.configs)-1)
	_, _ = DPrintf(prefix+format+"\n", a...)
}

func (sc *ShardCtrler) SetNotifier(t int, c chan OpRes) {
	sc.notifierLock.Lock()
	defer sc.notifierLock.Unlock()

	sc.notifier[t] = c
}

func (sc *ShardCtrler) CheckNotifier(t int) (c chan OpRes, ok bool) {
	sc.notifierLock.Lock()
	defer sc.notifierLock.Unlock()
	c, ok = sc.notifier[t]
	return
}

func (sc *ShardCtrler) DeleteNotifier(t int) {
	sc.notifierLock.Lock()
	defer sc.notifierLock.Unlock()

	delete(sc.notifier, t)
}

func (sc *ShardCtrler) UniqueTimestamp() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return time.Now().Nanosecond()
}

func (sc *ShardCtrler) Submit(op Op) (reply OpRes) {
	sc.mu.Lock()
	c := make(chan OpRes, 1)
	sc.SetNotifier(op.Timestamp, c)
	defer sc.mu.Unlock()
	defer sc.DeleteNotifier(op.Timestamp)

	_, _, ok := sc.rf.Start(op)

	if ok {
		select {
		case res := <- c:
			reply = res
		case <- time.NewTimer(100 * time.Millisecond).C:
			reply.Err = ERR
		}
	} else {
		reply.Err = ERR
	}
	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	rep := sc.Submit(Op{
		Cmd:       *args,
		Timestamp: sc.UniqueTimestamp(),
	})
	reply.WrongLeader = rep.Err == ERR
	reply.Err = rep.Err
	sc.LockLogf("receive join %+v, reply %+v", args, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	rep := sc.Submit(Op{
		Cmd:       *args,
		Timestamp: sc.UniqueTimestamp(),
	})
	reply.WrongLeader = rep.Err == ERR
	reply.Err = rep.Err
	sc.LockLogf("receive leave %+v, reply %+v", args, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	rep := sc.Submit(Op{
		Cmd:       *args,
		Timestamp: sc.UniqueTimestamp(),
	})
	reply.WrongLeader = rep.Err == ERR
	reply.Err = rep.Err
	sc.LockLogf("receive move %+v, reply %+v", args, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	rep := sc.Submit(Op{
		Cmd:       *args,
		Timestamp: sc.UniqueTimestamp(),
	})
	reply.WrongLeader = rep.Err == ERR
	reply.Err = rep.Err
	reply.Config = rep.Config
	sc.LockLogf("receive query %+v, reply %+v", args, reply)
}

func (sc *ShardCtrler) ApplyCron() {
	for msg := range sc.applyCh {
		if msg.SnapshotValid {
			// no need to do that
		} else if msg.CommandValid {
			op := msg.Command.(Op)
			var res OpRes
			switch op.Cmd.(type) {
			case JoinArgs:
				join := op.Cmd.(JoinArgs)
				if val, ok := sc.clients[join.ClientID]; !ok || val < join.SerialID {
					conf := sc.configs[len(sc.configs)-1].Copy()
					conf.Num += 1
					for k, v := range join.Servers {
						conf.Groups[k] = v
					}
					conf.Balance()
					sc.configs = append(sc.configs, *conf)
				}
			case LeaveArgs:
				leave := op.Cmd.(LeaveArgs)
				if val, ok := sc.clients[leave.ClientID]; !ok || val < leave.SerialID {
					conf := sc.configs[len(sc.configs)-1].Copy()
					conf.Num += 1
					for _, k := range leave.GIDs {
						delete(conf.Groups, k)
					}
					conf.Balance()
					sc.configs = append(sc.configs, *conf)
				}
			case MoveArgs:
				move := op.Cmd.(MoveArgs)
				if val, ok := sc.clients[move.ClientID]; !ok || val < move.SerialID {
					conf := sc.configs[len(sc.configs)-1].Copy()
					conf.Num += 1
					conf.Shards[move.Shard] = move.GID
					sc.configs = append(sc.configs, *conf)
				}
			case QueryArgs:
				query := op.Cmd.(QueryArgs)
				if query.Num == -1 || query.Num >= len(sc.configs){
					query.Num = len(sc.configs) - 1
				}
				res.Config = sc.configs[query.Num]
			}
			if notifier, ok := sc.CheckNotifier(op.Timestamp); ok {
				notifier <- res
			}
		}
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(OpRes{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clients = make(map[int]int)
	sc.notifier = make(map[int]chan OpRes)

	// Your code here.
	go sc.ApplyCron()
	return sc
}

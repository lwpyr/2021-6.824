package shardkv


import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister 	 *raft.Persister

	// clerks
	smc *shardctrler.Clerk
	kvc *Clerk

	// persistence
	kv [shardctrler.NShards]map[string]string
	clients map[int]int32

	ctrlConfig   	*shardctrler.Config
	ctrlConfigNew   *shardctrler.Config
	// todo migrate state
}

func (kv *ShardKV) Logf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%d]MGR_CLIENT: ", kv.me)
	_, _ = DPrintf(prefix+format, a...)
}

type SKVServerSnapshot struct {
	Kv      [shardctrler.NShards]map[string]string `json:"kv"`
	Clients map[int]int32     					   `json:"clients"`

	Stable          bool
	CtrlConfig   	shardctrler.Config
	CtrlConfigNew   shardctrler.Config
}

func (kv *ShardKV) CheckPersist(appliedIndex int) {
	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		snapshot := SKVServerSnapshot{
			Kv:     kv.kv,
			Clients: kv.clients,
			// todo
		}
		err := e.Encode(snapshot)
		if err != nil {
			return
		}
		b := w.Bytes()
		kv.rf.Snapshot(appliedIndex, b)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) GetFinal(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) get(args *GetArgs, reply *GetReply, redirect bool) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppendFinal(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) putAppend(args *PutAppendArgs, reply *PutAppendReply, redirect bool) {
	// Your code here.
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.smc = shardctrler.MakeClerk(ctrlers)
	kv.kvc = MakeClerk(servers, make_end)

	return kv
}

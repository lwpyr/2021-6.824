package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv map[string]string
	clients map[int]int32
	clientsLock sync.Mutex

	notifier map[int]chan OpRes
	persister *raft.Persister
	notifierLock sync.Mutex
}

func (kv *KVServer) SetClients(k int, v int32) {
	kv.clientsLock.Lock()
	defer kv.clientsLock.Unlock()

	kv.clients[k] = v
}

func (kv *KVServer) CheckClients(k int) (v int32, ok bool) {
	kv.clientsLock.Lock()
	defer kv.clientsLock.Unlock()
	v, ok = kv.clients[k]
	return
}

func (kv *KVServer) DeleteClients(k int) {
	kv.clientsLock.Lock()
	defer kv.clientsLock.Unlock()

	delete(kv.clients, k)
}

func (kv *KVServer) SetNotifier(t int, c chan OpRes) {
	kv.notifierLock.Lock()
	defer kv.notifierLock.Unlock()

	kv.notifier[t] = c
}

func (kv *KVServer) CheckNotifier(t int) (c chan OpRes, ok bool) {
	kv.notifierLock.Lock()
	defer kv.notifierLock.Unlock()
	c, ok = kv.notifier[t]
	return
}

func (kv *KVServer) DeleteNotifier(t int) {
	kv.notifierLock.Lock()
	defer kv.notifierLock.Unlock()

	delete(kv.notifier, t)
}

type Op struct {
	Cmd       interface{}
	Timestamp int
}

type OpRes struct {
	Err Err
	Value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	ts := time.Now()
	t := ts.Nanosecond()
	c := make(chan OpRes, 1)
	kv.SetNotifier(t, c)
	defer kv.mu.Unlock()
	defer kv.DeleteNotifier(t)

	rec := Op{
		Cmd:       *args,
		Timestamp: t,
	}
	_, _, ok := kv.rf.Start(rec)
	if ok {
		select {
		case res := <- c:
			reply.Err = res.Err
			reply.Value = res.Value
			kv.Logf("Receive Get %+v, reply %+v", args, reply)
		case <- time.NewTimer(100 * time.Millisecond).C:
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	kv.Logf("Receive Put %+v from Client[%d]", args, args.ClientID)
	ts := time.Now()
	t := ts.Nanosecond()
	c := make(chan OpRes, 1)
	kv.SetNotifier(t, c)
	defer kv.mu.Unlock()
	defer kv.DeleteNotifier(t)

	//if val, ok := kv.clients[args.ClientID]; ok && val >= args.SerialID {
	//	reply.Err = OK
	//	return
	//}

	rec := Op{
		Cmd:       *args,
		Timestamp: t,
	}

	_, _, ok := kv.rf.Start(rec)
	if ok {
		select {
		case res := <- c:
			reply.Err = res.Err
			kv.Logf("Server Put %+v, commit value %s take %v ms, reply %+v", args, kv.kv[args.Key], time.Since(ts).Milliseconds(), reply)
		case <- time.NewTimer(100 * time.Millisecond).C:
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	// Your code here.
}

type KVServerSnapshot struct {
	Kv      map[string]string `json:"kv"`
	Clients map[int]int32     `json:"clients"`
}

func (kv *KVServer) CheckPersist(appliedIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := KVServerSnapshot{
		Kv:     kv.kv,
		Clients: kv.clients,
	}
	err := e.Encode(snapshot)
	if err != nil {
		return
	}
	b := w.Bytes()
	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.rf.Snapshot(appliedIndex, b)
	}
}

func (kv *KVServer) ApplyCron() {
	for m := range kv.applyCh {
		if m.SnapshotValid == true {
			var s KVServerSnapshot
			r := bytes.NewBuffer(m.Snapshot)
			d := labgob.NewDecoder(r)
			if d.Decode(&s) != nil {
				os.Exit(1)
			}
			kv.kv = s.Kv
			kv.clients = s.Clients
			// ignore other types of ApplyMsg
		} else if m.CommandValid == true {
			op := m.Command.(Op)
			var res OpRes
			switch op.Cmd.(type) {
			case GetArgs:
				cmd := op.Cmd.(GetArgs)
				val, ok := kv.kv[cmd.Key]
				if ok {
					res.Err = OK
					res.Value = val
				} else {
					res.Err = ErrNoKey
				}
			case PutAppendArgs:
				cmd := op.Cmd.(PutAppendArgs)
				kv.Logf("Execute %+v", cmd)
				if cmd.Op == "Put" {
					kv.kv[cmd.Key] = cmd.Value
				} else if val, ok := kv.clients[cmd.ClientID]; !ok || val < cmd.SerialID {
					val, ok := kv.kv[cmd.Key]
					if ok {
						kv.kv[cmd.Key] = val + cmd.Value
					} else {
						kv.kv[cmd.Key] = cmd.Value
					}
				}
				res.Err = OK
				kv.clients[cmd.ClientID] = cmd.SerialID
			}
			if notifier, ok := kv.CheckNotifier(op.Timestamp); ok {
				notifier <- res
			}
			kv.CheckPersist(m.CommandIndex)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Logf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%d]KVRAFT: ", kv.me)
	_, _ = DPrintf(prefix+format+"\n", a...)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.kv = make(map[string]string)
	kv.clients = make(map[int]int32)
	kv.notifier = make(map[int]chan OpRes)
	go kv.ApplyCron()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	return kv
}

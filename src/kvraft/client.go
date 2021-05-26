package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu sync.Mutex
	// You will have to modify this struct.
	ID int
	SerID int32
}

func (ck* Clerk) NextSerID() int32 {
	return atomic.AddInt32(&ck.SerID, 1)
}

func (ck *Clerk) Logf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d]CLIENT: ", ck.ID)
		log.Printf(prefix+format, a...)
	}
}

var GlobalId = 0
var GlobalIdMu sync.Mutex

func AcquireClientId() int {
	GlobalIdMu.Lock()
	defer GlobalIdMu.Unlock()
	ret := GlobalId
	GlobalId++
	return ret
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ID = AcquireClientId()
	ck.SerID = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{Key: key}
	reply := GetReply{}

	for true {
		for _, peer := range ck.servers {
			if peer.Call("KVServer.Get", &args, &reply) {
				if reply.Err == OK {
					ck.Logf("Get Success key=%s val=%s", key, reply.Value)
					return reply.Value
				} else if reply.Err == ErrNoKey {
					ck.Logf("Get key=%s - No key", key)
					return ""
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientID: ck.ID,
		SerialID: ck.NextSerID(),
	}
	reply := PutAppendReply{}

	for true {
		for _, peer := range ck.servers {
			if peer.Call("KVServer.PutAppend", &args, &reply) {
				if reply.Err == OK {
					ck.Logf("Put success %+v", args)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

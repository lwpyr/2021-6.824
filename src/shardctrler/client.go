package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"fmt"
	"log"
	"sync"
)
import "time"

var GlobalId = 0
var GlobalIdMu sync.Mutex

func AcquireClientId() int {
	GlobalIdMu.Lock()
	defer GlobalIdMu.Unlock()
	ret := GlobalId
	GlobalId++
	return ret
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	SerID int
	ClientID int
	mu sync.Mutex
}

func (ck* Clerk) NextSerID() int {
	ck.SerID++
	return ck.SerID
}

func (ck *Clerk) Logf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d]MGR_CLIENT: ", ck.ClientID)
		log.Printf(prefix+format, a...)
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.ClientID = AcquireClientId()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.Logf("Query %d", num)
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.Logf("Join %+v", servers)
	args := &JoinArgs{
		ClientID: ck.ClientID,
		SerialID: ck.NextSerID(),
	}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.Logf("Leave %+v", gids)
	args := &LeaveArgs{
		ClientID: ck.ClientID,
		SerialID: ck.NextSerID(),
	}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.Logf("Move shard[%d] -> GID[%d]", shard, gid)
	args := &MoveArgs{
		ClientID: ck.ClientID,
		SerialID: ck.NextSerID(),
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

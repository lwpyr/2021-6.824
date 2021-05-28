package shardkv

import "log"

// Debugging
const Debug = true

func init(){
	log.SetFlags(log.Lmicroseconds)
}


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

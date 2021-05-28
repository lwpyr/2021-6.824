package shardkv

import "log"

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

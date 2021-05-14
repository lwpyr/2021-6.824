package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {
	// Your worker implementation here.
	fmt.Printf("start worker %d", os.Getpid())
	for true {
		t := CallAskTask()
		switch t.Type {
		case WAIT:
			time.Sleep(1 * time.Second)
		case DONE:
			return
		case MAP:
			fmt.Printf("Execute MAP task %v\n", t.FileName)
			filename := t.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapFunc(filename, string(content))
			kvf := make([]*os.File, t.NReduce)
			kvd := make([]*json.Encoder, t.NReduce)
			for i := 0; i < t.NReduce; i++ {
				kvf[i], err = os.Create(strconv.Itoa(i)+"-"+strconv.Itoa(t.ID))
				if err != nil {
					fmt.Println(err)
				}
				kvd[i] = json.NewEncoder(kvf[i])
			}
			for _, kv := range kva {
				h := ihash(kv.Key) % t.NReduce
				kvd[h].Encode(kv)
			}
			for i := 0; i < t.NReduce; i++ {
				_ = kvf[i].Close()
			}
			re := CallReportTask(MAP, t.ID)
			if re.KeepServe == false {
				return
			}
		case REDUCE:
			fmt.Printf("Execute Reduce task %v\n", t.ReduceIndex)
			var m = map[string][]string{}
			for mid := 0; mid < t.ReduceRange; mid++ {
				f, _ := os.Open(strconv.Itoa(t.ReduceIndex)+"-"+strconv.Itoa(mid))
				decoder := json.NewDecoder(f)
				for decoder.More() {
					var kv KeyValue
					decoder.Decode(&kv)
					m[kv.Key] = append(m[kv.Key], kv.Value)
				}
			}
			ofile, err := os.Create("mr-out-"+strconv.Itoa(t.ReduceIndex))
			if err != nil {
				log.Fatalln(err)
			}
			for k, vs := range m {
				output := reduceFunc(k, vs)
				fmt.Fprintf(ofile, "%v %v\n", k, output)
			}
			re := CallReportTask(REDUCE, t.ID)
			if re.KeepServe == false {
				return
			}
		}
	}
	fmt.Printf("worker %d exit", os.Getpid())
}

func CallAskTask() *ReplyTask {
	args := AskTask{
		ID: os.Getpid(),
	}
	reply := ReplyTask{}
	call("Coordinator.GetTask", &args, &reply)
	return &reply
}

func CallReportTask(t int, id int) *AckReport {
	args := ReportTask{
		Type: t,
		ID: id,
	}
	reply := AckReport{}
	call("Coordinator.ReportTask", &args, &reply)
	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

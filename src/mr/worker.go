package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	// "time"

	"github.com/rs/xid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var id = xid.New().String()

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		AskAssignment(mapf, reducef)
		// time.Sleep(5 * time.Second)
	}

}

func AskAssignment(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := WorkerArgs{}
	args.WorkerId = id
	reply := WorkerReply{}
	ok := call("Coordinator.Assignment", &args, &reply)
	if ok {
		doAssignment(reply, mapf, reducef)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func DoneMap() {
	args := WorkerArgs{}
	args.WorkerId = id
	reply := WorkerReply{}
	ok := call("Coordinator.DoneMap", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func DoneReduce() {
	args := WorkerArgs{}
	args.WorkerId = id
	reply := WorkerReply{}
	ok := call("Coordinator.DoneReduce", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func doAssignment(reply WorkerReply, mapf func( string,  string) []KeyValue, reducef func( string,  []string) string) {
	if reply.File == "" {
		return
	}
	fmt.Printf("--reply.File %v\n", reply.File)
	fmt.Printf("--reply.WorkType %v\n", reply.WorkType)
	if reply.WorkType == "map" {
		doMapTask(reply.File, reply.NReduce, mapf)
		DoneMap()
	} else if reply.WorkType == "reduce" {
		doReduceTask(reply.File, reducef)
		DoneReduce()
	}
}

func doMapTask(filename string, nReduce int, mapf func( string,  string) []KeyValue) {
	fmt.Printf("---doMapTask %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	fmt.Printf("---Looping through key-values after map\n")
	
	encoders := make([]*json.Encoder, nReduce)
	tempFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", i, id)
		ofile, err := os.CreateTemp("", oname)
		tempFiles[i] = ofile
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}
		encoders[i] = json.NewEncoder(ofile)
	}

	for _, kv := range kva {
		reduce := ihash(kv.Key) % nReduce
		encoders[reduce].Encode(&kv)
	}
	for i, tempFile := range tempFiles {
		tempFile.Close()
		oname := tempFile.Name()
		newName := filepath.Join(".", fmt.Sprintf("mr-%v-%v", i, id))
		if _, err := os.Stat(newName); err == nil {
			// newName file exists, append the contents of oname into newName
			nf, err := os.OpenFile(newName, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("cannot open %v: %v", newName, err)
			}
			of, err := os.Open(oname)
			if err != nil {
				log.Fatalf("cannot open %v: %v", oname, err)
			}
			defer nf.Close()
			defer of.Close()
			_, err = io.Copy(nf, of)
			if err != nil {
				log.Fatalf("cannot append %v to %v: %v", oname, newName, err)
			}
		} else {
			// newName file does not exist, rename oname to newName
			err := os.Rename(oname, newName)
			if err != nil {
				log.Fatalf("cannot move %v to current directory: %v", oname, err)
			}
		}
	}
	fmt.Printf("--Done with map\n")
}

func doReduceTask(reduceChunk string, reducef func( string,  []string) string) {
	fmt.Printf("--doReduceTask %v\n", reduceChunk)

	intermediate := []KeyValue{}

	files, err := filepath.Glob(fmt.Sprintf("mr-%v-*", reduceChunk))
	if err != nil {
		log.Fatalf("cannot find files: %v", err)
	}

	// Open each file and read each line using json decode
	for _, file := range files {
		fmt.Printf("---Reading file %v\n", file)
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open file %v: %v", file, err)
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("cannot decode file %v: %v", file, err)
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceChunk)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	fmt.Printf("--Done with reduce for chunk %v\n", reduceChunk)
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

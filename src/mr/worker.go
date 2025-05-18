package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"


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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// 请求任务
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		
		if !call("Coordinator.RequestTask", &args, &reply) {
			// 如果无法联系协调者，退出
			return
		}

		task := reply.Task
		switch task.TaskType {
		case MapTask:
			// 执行Map任务
			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.FileName)
			}
			file.Close()

			kva := mapf(task.FileName, string(content))
			
			// 将中间结果写入临时文件
			intermediate := make([][]KeyValue, task.NReduce)
			for _, kv := range kva {
				r := ihash(kv.Key) % task.NReduce
				intermediate[r] = append(intermediate[r], kv)
			}

			for r := 0; r < task.NReduce; r++ {
				filename := fmt.Sprintf("mr-%d-%d", task.TaskId, r)
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("cannot create %v", filename)
				}
				enc := json.NewEncoder(file)
				for _, kv := range intermediate[r] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v", kv)
					}
				}
				file.Close()
			}

			// 通知协调者任务完成
			completeArgs := CompleteTaskArgs{
				TaskId:   task.TaskId,
				TaskType: MapTask,
			}
			completeReply := CompleteTaskReply{}
			call("Coordinator.CompleteTask", &completeArgs, &completeReply)

		case ReduceTask:
			// 执行Reduce任务
			kva := []KeyValue{}
			for m := 0; m < task.NMap; m++ {
				filename := fmt.Sprintf("mr-%d-%d", m, task.TaskId)
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			// 按键排序
			sort.Slice(kva, func(i, j int) bool {
				if kva[i].Key != kva[j].Key {
					return kva[i].Key < kva[j].Key
				}
				return kva[i].Value < kva[j].Value
			})

			// 执行Reduce
			oname := fmt.Sprintf("mr-out-%d", task.TaskId)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				// 对values进行排序以确保输出的一致性
				sort.Strings(values)
				output := reducef(kva[i].Key, values)
				// 确保输出格式正确
				if output != "" {
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				}
				i = j
			}
			ofile.Close()

			// 通知协调者任务完成
			completeArgs := CompleteTaskArgs{
				TaskId:   task.TaskId,
				TaskType: ReduceTask,
			}
			completeReply := CompleteTaskReply{}
			call("Coordinator.CompleteTask", &completeArgs, &completeReply)

		case NoTask:
			// 没有可用任务，等待一段时间
			time.Sleep(time.Second)

		case ExitTask:
			// 所有任务完成，退出
			return
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

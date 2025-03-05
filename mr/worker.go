package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type mapFunc func(string, string) []KeyValue
type reduceFunc func(string, []string) string

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func (kv KeyValue) String() string {
	return fmt.Sprintf("{Key: %s, Value: %s}", kv.Key, kv.Value)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.

// func saveIntermediaData(fileName string) {

// }

func mapTempFilesProducer(wg *sync.WaitGroup, nReduce int, items []KeyValue, hashC map[int]chan KeyValue) {
	defer wg.Done()
	for _, item := range items {
		hashValue := ihash(item.Key) % nReduce
		hashC[hashValue] <- item
	}
	// 写入后将所有 reduce ID 对应的 channel 关闭
	for _, v := range hashC {
		close(v)
	}
	// fmt.Println("所有数据处理完毕")
}

func mapTempFilesConsumer(wg *sync.WaitGroup, id int, nReduce int, hashC map[int]chan KeyValue, fileC chan<- string) {
	defer wg.Done()
	for i := 0; i < nReduce; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var items []KeyValue
			for item := range hashC[i] {
				items = append(items, item)
			}
			// TODO: 将 items 写入 json 文件中
			josnData, err := json.MarshalIndent(items, "", "    ")
			if err != nil {
				fmt.Println(err)
				return
			}
			f, err := os.CreateTemp("./", "mr-*")
			if err != nil {
				fmt.Println(err)
				return
			}
			_, err = f.Write(josnData)
			if err != nil {
				fmt.Println(err)
				return
			}
			file := fmt.Sprintf("mr-%d-%d.json", id, i)
			err = os.Rename(f.Name(), file)
			if err != nil {
				fmt.Println(err)
				return
			}
			// 文件路径通过 channel 传回处理 goroutine 中
			fileC <- file
		}(i)
	}
}

func processMapIntermediate(id int, nReduce int, items []KeyValue) []string {
	var wg = sync.WaitGroup{}
	var hashC = make(map[int]chan KeyValue)
	for i := 0; i < nReduce; i++ {
		hashC[i] = make(chan KeyValue, 10)
	}
	var fileC = make(chan string, 2)
	var files []string
	wg.Add(1)
	go mapTempFilesProducer(
		&wg,
		nReduce,
		items,
		hashC,
	)
	wg.Add(1)
	go mapTempFilesConsumer(
		&wg,
		id,
		nReduce,
		hashC,
		fileC,
	)
	go func() { // 监听文件写入完成
		wg.Wait()
		// fmt.Println("所有数据处理完毕")
		close(fileC)
	}()
	for file := range fileC {
		// fmt.Println(file)
		files = append(files, file)
	}
	// fmt.Println("所有文件写入完成")
	return files
}

func saveReduceResult(id int, kv []KeyValue) string {
	file := fmt.Sprintf("mr-out%d.txt", id)
	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer f.Close()
	for _, item := range kv {
		fmt.Fprintf(f, "%v %v\n", item.Key, item.Value)
	}
	return file
}

func readTxt(file string) (bytes []byte, err error) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()
	bytes, err = io.ReadAll(f)
	return
}

// 从 json 文件中读取 kv 对
// 通过 channel 传回
func readJson(wg *sync.WaitGroup, file string, c chan<- []KeyValue) {
	defer wg.Done()
	var items []KeyValue
	bytes, err := os.ReadFile(file)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = json.Unmarshal(bytes, &items)
	if err != nil {
		fmt.Println(err)
		return
	}
	c <- items
}

func partition(items []KeyValue, reducef reduceFunc) (result []KeyValue) {
	for i := 0; i < len(items); {
		j := i + 1
		for j < len(items) && items[j].Key == items[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, items[k].Value)
		}
		key := items[i].Key
		v := reducef(key, values)
		result = append(result, KeyValue{Key: key, Value: v})
		i = j
	}
	return
}

func reduce(files []string, reducef reduceFunc) (items []KeyValue) {
	var wg = sync.WaitGroup{}
	var itemsC = make(chan []KeyValue, 2)
	for _, file := range files {
		wg.Add(1)
		go readJson(&wg, file, itemsC)
	}
	// 常用机制
	// 生产者完成后关闭 channel
	go func() { // 监听读取是否完毕
		wg.Wait()
		close(itemsC)
	}()
	for item := range itemsC {
		items = append(items, item...)
	}
	sort.Sort(ByKey(items))
	items = partition(items, reducef)
	return
}

func _map(files []string, mapf mapFunc) (items []KeyValue) {
	for _, file := range files {
		bytes, err := readTxt(file)
		if err != nil {
			log.Fatal(err)
		}
		items = append(items, mapf(file, string(bytes))...)
	}
	return
}

func Worker(mapf mapFunc,
	reducef reduceFunc) {
	timer := time.After(time.Second * 5)
	for {
		select {
		case <-timer:
			// fmt.Println("worker timeout, exit")
			return
		default:
			// Your worker implementation here.
			var args = RequestArgs{}
			var reply = RequestReply{}
			flag := call("Coordinator.DispatchTask", &args, &reply)
			if !flag {
				// fmt.Println("call failed!")
				time.Sleep(time.Second * 2)
				continue
			}
			switch reply.Task.Type {
			case MAP:
				items := _map(reply.Task.MapTask.Files, mapf)
				files := processMapIntermediate(reply.Task.TaskID, reply.NReduce, items)
				call(
					"Coordinator.MapTaskDone",
					&ResponseArgs{
						reply.Task.TaskID,
						files,
					},
					&ResponseReply{},
				)
			case REDUCE:
				// fmt.Println(reply.Task.ReduceTask.ID)
				result := reduce(reply.Task.ReduceTask.Files, reducef)
				file := saveReduceResult(reply.Task.TaskID, result)
				call(
					"Coordinator.ReduceTaskDone",
					&ResponseArgs{
						reply.Task.TaskID,
						[]string{file},
					},
					&ResponseReply{},
				)
			case SLEEP:
				// fmt.Println("sleep")
				time.Sleep(time.Millisecond * 500)
			}
			timer = time.After(time.Second * 5)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// fmt.Println(err)
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

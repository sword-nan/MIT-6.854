package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	SLEEP
)

type MapTask struct {
	// 处理多个文件
	Files []string
}

type ReduceTask struct {
	ID int
	// 处理多个文件
	Files []string
}

type Task struct {
	TaskID int // master 每次分发任务时自增
	Type   TaskType
	MapTask
	ReduceTask
}

type TaskQueue[T MapTask | ReduceTask] struct {
	sync.Mutex
	// tasks   []T
	tasks *list.List
	// 用于传输超时任务
	timeoutC chan T
}

type Coordinator struct {
	// Your definitions here.
	// 当所有的 map 任务完成后需要进行排序任务
	// 排序期间如果有任务请求，则忽略请求
	sync.Mutex
	nReduce int
	// 任务 id
	currentID int
	// 任务队列
	mapQueue    *TaskQueue[MapTask]
	reduceQueue *TaskQueue[ReduceTask]
	// map 和 reduce 的 waitGroup
	mapWG    sync.WaitGroup
	reduceWG sync.WaitGroup
	// map 产生的临时文件
	mapTempFiles []string
	// reduce 产生的临时文件
	reduceTempFiles []string
	// 任务完成时通知的 channel
	// 和任务对应的 timer 一起进行监听
	doneC map[int]chan TaskType // lazy init
	// 记录任务的状态
	// 任务超时最终还是会返回结果，通过状态判断该结果是否可用
	isTimeout map[int]struct{} // lazy init
	// 关闭所有的协程，防止内存泄漏
	StopC chan struct{}
	// // sort 信号，所有 Map 任务完成，需要进行中间结果的合并排序
	// sortC chan struct{}
	done bool
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (t *TaskQueue[T]) getTask() (task any, ok bool) {
	t.Lock()
	defer t.Unlock()
	if t.tasks.Len() == 0 {
		return nil, false
	}
	ok = true
	front := t.tasks.Front()
	task = front.Value
	t.tasks.Remove(front)
	return
}

func (t *TaskQueue[T]) addTask(task T) {
	t.Lock()
	defer t.Unlock()
	t.tasks.PushFront(task)
	// t.tasks = append(t.tasks, task)
}

func (t *TaskQueue[T]) len() int {
	t.Lock()
	defer t.Unlock()
	return t.tasks.Len()
	// t.tasks = append(t.tasks, task)
}

// func (t *TaskQueue[T]) done() {
// 	t.Lock()
// 	defer t.Unlock()
// 	t.restNum--
// }

func (t *TaskQueue[T]) addTimeoutTask(stopC chan struct{}) {
	for {
		select {
		case task := <-t.timeoutC:
			t.addTask(task)
		case <-stopC:
			return
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func timeOut[T MapTask | ReduceTask](c *Coordinator, taskID int, task T, qTimeoutC chan T) {
	c.Lock()
	defer c.Unlock()
	// fmt.Printf("%T-Task-%d timeout\n", task, taskID)
	// fmt.Println(c.isTimeout)
	c.isTimeout[taskID] = struct{}{}
	qTimeoutC <- task
}

// 对 task 进行监听
// 超时则将其加入超时队列
// 完成则将其标记为完成
func monitorTask[T MapTask | ReduceTask](c *Coordinator, taskID int, task T, timeoutC chan T, doneC chan TaskType) {
	timer := time.After(time.Second * 10)
	for {
		select {
		case <-timer:
			timeOut(c, taskID, task, timeoutC)
			return
		case val := <-doneC:
			select {
			case <-timer: // select 随机挑选信号，可能两个都触发
				timeOut(c, taskID, task, timeoutC)
				return
			default:
				// fmt.Printf("Task-%d done\n", taskID)
				c.Lock()
				c.releaseDoneTask(taskID)
				c.Unlock()
				if val == REDUCE {
					c.reduceWG.Done()
				} else if val == MAP {
					c.mapWG.Done()
				}
				return
			}
		case <-c.StopC:
			fmt.Println("强制退出")
			return
		}
	}
}

func (c *Coordinator) monitorMapTask(taskID int, task MapTask) {
	c.Lock()
	defer c.Unlock()
	c.doneC[taskID] = make(chan TaskType)
	go monitorTask(
		c,
		taskID,
		task,
		c.mapQueue.timeoutC,
		c.doneC[taskID],
	)
}

func (c *Coordinator) monitorReduceTask(taskID int, task ReduceTask) {
	c.Lock()
	defer c.Unlock()
	c.doneC[taskID] = make(chan TaskType)
	go monitorTask(
		c,
		taskID,
		task,
		c.reduceQueue.timeoutC,
		c.doneC[taskID],
	)
}

func (c *Coordinator) DispatchTask(args *RequestArgs, reply *RequestReply) (err error) {
	// 确保初始化是线程安全的
	c.Lock()
	if c.doneC == nil {
		c.doneC = make(map[int]chan TaskType)
	}
	if c.isTimeout == nil {
		c.isTimeout = make(map[int]struct{})
	}
	c.Unlock()
	task, taskID, err := c.getTask()
	switch t := task.(type) {
	case MapTask:
		c.monitorMapTask(
			taskID,
			t,
		)
		reply.NReduce = c.nReduce
		reply.Task = Task{
			Type:    MAP,
			TaskID:  taskID,
			MapTask: t,
		}
	case ReduceTask:
		c.monitorReduceTask(
			taskID,
			t,
		)
		reply.NReduce = c.nReduce
		reply.Task = Task{
			Type:       REDUCE,
			TaskID:     taskID,
			ReduceTask: t,
		}
	case nil:
		reply.Task = Task{
			Type: SLEEP,
		}
	default:
		err = fmt.Errorf("unknown task type")
	}
	return
}

func (c *Coordinator) addMapTask(task MapTask) {
	c.mapQueue.addTask(task)
	c.mapWG.Add(1)
}

func (c *Coordinator) addReduceTask(task ReduceTask) {
	c.reduceQueue.addTask(task)
	c.reduceWG.Add(1)
}

func (c *Coordinator) getTask() (any, int, error) {
	c.Lock()
	defer c.Unlock()
	if t, ok := c.mapQueue.getTask(); ok {
		return c.assignTask(t)
	} else if t, ok := c.reduceQueue.getTask(); ok {
		return c.assignTask(t)
	} else {
		// fmt.Println("no task")
		// err := fmt.Errorf("no task")
		// err = nil
		return nil, 0, nil
	}
}

func (c *Coordinator) assignTask(task any) (any, int, error) {
	c.currentID++
	return task, c.currentID, nil
}

func (c *Coordinator) releaseDoneTask(taskID int) {
	delete(c.doneC, taskID)
}

func (c *Coordinator) deleteTimeoutTask(taskID int) {
	delete(c.isTimeout, taskID)
	delete(c.doneC, taskID)
}

func (c *Coordinator) MapTaskDone(args *ResponseArgs, reply *ResponseReply) (err error) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.isTimeout[args.TaskID]; ok {
		fmt.Printf("超时 MapTask-%d 返回，忽略结果", args.TaskID)
		c.deleteTimeoutTask(args.TaskID)
		// TODO: 删除超时任务所产生的临时文件
		for _, file := range args.Files {
			os.Remove(file)
		}
		return
	}
	// fmt.Printf("MapTask-%d Done\n", args.TaskID)
	// fmt.Printf("MapTask-%d Done, files are %v\n", args.TaskID, args.Files)
	c.mapTempFiles = append(c.mapTempFiles, args.Files...)
	c.doneC[args.TaskID] <- MAP
	return
}

func (c *Coordinator) ReduceTaskDone(args *ResponseArgs, reply *ResponseReply) (err error) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.isTimeout[args.TaskID]; ok {
		fmt.Printf("超时 ReduceTask-%d 返回，忽略结果", args.TaskID)
		c.deleteTimeoutTask(args.TaskID)
		// TODO: 删除超时任务所产生的临时文件
		return
	}
	c.reduceTempFiles = append(c.reduceTempFiles, args.Files...)
	c.doneC[args.TaskID] <- REDUCE
	return
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	ret := c.done
	return ret
}

func (c *Coordinator) generateReduceTask(id int, files []string, taskC chan<- ReduceTask) {
	task := ReduceTask{
		ID:    id,
		Files: files,
	}
	taskC <- task
}

func (c *Coordinator) groupIntermediateFiles() map[int][]string {
	var groupedFiles = make(map[int][]string)
	var re = regexp.MustCompile(`^mr-\d+-(\d+)\.json$`)
	var files = c.mapTempFiles
	var secondNumber string
	for _, file := range files {
		matches := re.FindStringSubmatch(file)
		if len(matches) > 1 {
			secondNumber = matches[1]
		} else {
			fmt.Println("No match found.")
		}
		n, _ := strconv.Atoi(secondNumber)
		groupedFiles[n] = append(groupedFiles[n], file)
	}
	return groupedFiles
}

func (c *Coordinator) reduce() {
	var wg = sync.WaitGroup{}
	var taskC = make(chan ReduceTask, 5)
	groupedFiles := c.groupIntermediateFiles()
	for i := 0; i < c.nReduce; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.generateReduceTask(
				i,
				groupedFiles[i],
				taskC,
			)
		}()
	}
	go func() {
		wg.Wait()
		close(taskC)
	}()
	for task := range taskC {
		c.addReduceTask(task)
	}
	// fmt.Println("所有 Reduce Task 准备完毕!")
	// fmt.Printf("The Rest Task Num in ReduceQueue: %d\n", c.reduceQueue.len())
}

func (c *Coordinator) _map(files []string) {
	for _, file := range files {
		// f, err := os.Open(file)
		// if err != nil {
		// 	log.Fatalf("cannot open %v", file)
		// }
		// content, err := io.ReadAll(f)
		// if err != nil {
		// 	log.Fatalf("cannot read %v", file)
		// }
		c.addMapTask(MapTask{Files: []string{file}})
	}
	// fmt.Printf("The Rest Task Num in MapQueue: %d\n", c.mapQueue.len())
}

func (c *Coordinator) end() {
	c.Lock()
	defer c.Unlock()
	c.done = true
}

func (c *Coordinator) removeTempFiles() {
	var wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, file := range c.mapTempFiles {
			wg.Add(1)
			go func(file string) {
				defer wg.Done()
				err := os.Remove(file)
				if err != nil {
					fmt.Println(err)
					return
				}
			}(file)
		}
	}()
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	for _, file := range c.reduceTempFiles {
	// 		wg.Add(1)
	// 		go func(file string) {
	// 			defer wg.Done()
	// 			os.Remove(file)
	// 		}(file)
	// 	}
	// }()
	wg.Wait()
	c.mapTempFiles = nil
	c.reduceTempFiles = nil
}

func (c *Coordinator) monitorReduceEnd() {
	var done = make(chan struct{})
	go func() {
		c.reduceWG.Wait()
		close(done)
	}()
	for {
		select {
		case <-c.StopC:
			fmt.Println("强制退出")
			return
		case <-done:
			select {
			case <-c.StopC:
				fmt.Println("强制退出")
				return
			default:
				// fmt.Println("所有 Reduce 任务完成")
				c.removeTempFiles()
				c.end()
				return
			}
		default:
			time.Sleep(time.Microsecond * 500)
		}
	}
}

func (c *Coordinator) monitorMapEnd() {
	var done = make(chan struct{})
	go func() {
		c.mapWG.Wait()
		close(done)
	}()
	for {
		select {
		case <-c.StopC:
			fmt.Println("强制退出")
			return
		case <-done:
			select {
			case <-c.StopC:
				fmt.Println("强制退出")
				return
			default:
				// fmt.Println("所有 Map 任务完成")
				c.reduce()
				go c.monitorReduceEnd()
				return
				// 进行排序以及生成 reduce 任务
			}
		default:
			time.Sleep(time.Microsecond * 500)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		mapQueue: &TaskQueue[MapTask]{
			timeoutC: make(chan MapTask),
			tasks:    list.New(),
		},
		reduceQueue: &TaskQueue[ReduceTask]{
			timeoutC: make(chan ReduceTask),
			tasks:    list.New(),
		},
		StopC: make(chan struct{}),
	}
	// Your code here.
	// 初始化 map 任务
	c._map(files)
	go c.mapQueue.addTimeoutTask(c.StopC)
	go c.reduceQueue.addTimeoutTask(c.StopC)
	go c.monitorMapEnd()
	c.server()
	return &c
}

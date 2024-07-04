package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallForReportStatus(successType MsgType, taskID int) error {
	args := MessageSend{
		MsgType: successType,
		TaskID:  taskID,
	}
	//fmt.Println("call for report status0")
	err := call("Coordinator.NoticeResult", &args, nil)
	//fmt.Println("call for report status")
	return err
}

func CallForTask() *MessageReply {
	//fmt.Println("Call for tasks")
	args := MessageSend{
		MsgType: AskForTasks,
	}

	reply := MessageReply{}
	err := call("Coordinator.AskForTask", &args, &reply)
	//fmt.Println("Call for tasks 2222")
	if err == nil {
		return &reply
	} else {
		return nil
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	// if err != nil {
	// 	log.Fatalf("无法打开日志文件: %v", err)
	// }
	// defer file.Close()

	// 将日志输出设置到文件
	// log.SetOutput(file)
	for {
		replyMsg := CallForTask()

		//fmt.Println("replyMsg.MsgType: ", replyMsg.MsgType)
		switch replyMsg.MsgType {
		case MapTaskAlloc:
			err := HandleMapTask(replyMsg, mapf)
			if err == nil {
				//fmt.Println("HadleMapTask err didn't came out")
				_ = CallForReportStatus(MapSuccess, replyMsg.TaskID)
				//fmt.Println("HadleMapTask err didn't came out 2")
			} else {
				_ = CallForReportStatus(MapFailed, replyMsg.TaskID)

			}
		case ReduceTaskAlloc:
			err := HandleReduceTask(replyMsg, reducef)
			if err == nil {
				//fmt.Println("HadleMapTask err didn't came out")
				_ = CallForReportStatus(ReduceSuccess, replyMsg.TaskID)
				//fmt.Println("HadleMapTask err didn't came out 2")
			} else {
				_ = CallForReportStatus(ReduceFailed, replyMsg.TaskID)
			}
		case Wait:
			time.Sleep(time.Second * 10)
		case Shutdown:
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}
}

func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	//进行mapf
	kva := mapf(reply.TaskName, string(content))
	sort.Sort(ByKey(kva))
	//log.Fatalln(kva)
	//fmt.Println(kva)
	// oname_prefix := "mr-out-" + strconv.Itoa(reply.TaskID) + "-"

	// key_group := map[string][]string{}
	// for _, kv := range kva {
	// 	key_group[kv.Key] = append(key_group[kv.Key], kv.Value)
	// }

	// 先清理可能存在的垃圾
	// TODO: 原子重命名的方法
	// _ = DelFileByMapId(reply.TaskID, "./")

	// for key, values := range key_group {
	// 	redId := ihash(key)
	// 	oname := oname_prefix + strconv.Itoa(redId%reply.NReduce)
	// 	var ofile *os.File
	// 	if _, err := os.Stat(oname); os.IsNotExist(err) {
	// 		ofile, _ = os.Create(oname)
	// 	} else {
	// 		ofile, _ = os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// 	}
	// 	enc := json.NewEncoder(ofile)
	// 	for _, value := range values {
	// 		err := enc.Encode(&KeyValue{Key: key, Value: value})
	// 		if err != nil {
	// 			ofile.Close()
	// 			return err
	// 		}
	// 	}
	// 	ofile.Close()
	// }
	// return nil

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for _, kv := range kva {
		redId := ihash(kv.Key) % reply.NReduce
		if encoders[redId] == nil {
			tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-map-tmp-%d", redId))
			if err != nil {
				return nil
			}
			defer tempFile.Close()
			tempFiles[redId] = tempFile
			encoders[redId] = json.NewEncoder(tempFile)
		}
		err := encoders[redId].Encode(&kv)
		if err != nil {
			return err
		}
	}

	for i, file := range tempFiles {
		if file != nil {
			fileName := file.Name()
			file.Close()
			newName := fmt.Sprintf("mr-out-%d-%d", reply.TaskID, i)
			if err := os.Rename(fileName, newName); err != nil {
				return err
			}
		}
	}
	return nil
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) error {
	//defer fmt.Println("33333333333333333")
	key_id := reply.TaskID
	//fmt.Println("HHHHHHHHHHHHHHHHH")
	k_vs := map[string][]string{}

	fileList, err := ReadSpecificFile(key_id, "./")
	if err != nil {
		return err
	}

	// 整理所有中间文件
	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			k_vs[kv.Key] = append(k_vs[kv.Key], kv.Value)
		}
		file.Close()
	}
	// 获取所有的键并排序
	var keys []string
	for k := range k_vs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	//log.Println("1111")
	//log.Println(keys)
	//fmt.Println("111111111111111111")
	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, k_vs[key])
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, output)
		if err != nil {
			return err
		}
	}

	DelFileByReduceId(reply.TaskID, "./")
	//fmt.Println("22222222222222222222222222")
	return nil

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		fmt.Println(err)
		os.Exit(-1)
	}
	defer c.Close()
	//fmt.Println("call for report status11")
	err = c.Call(rpcname, args, reply)
	//fmt.Println("call for report status22")
	return err
}

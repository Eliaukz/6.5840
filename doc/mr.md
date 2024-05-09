# mapreduce
该实验是分布式领域大名鼎鼎的lab之一，实验一要求我们实现mapreduce（Google三驾马车之一）。
* lab地址[https://pdos.csail.mit.edu/6.824/labs/lab-mr.html](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
* 论文地址[https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf)
## 总体设计
该实验比较简单，总体流程如下图所示
![](./imgs/img.png)
* Master 节点只负责分配任务
* worker节点负责map以及reduce任务：
	* map 将字符串分割为单个单词然后保存进中间文件中
		```go
		map(String key, String value): 
			// key: document name 
			// value: document contents 
			for each word w in value: 
				EmitIntermediate(w, "1"); 
		```
	* reduce 将中间文件单词排序后合并同类项然后保存斤final文件中	
		```go
		reduce(String key, Iterator values): 
			// key: a word 
			// values: a list of counts 
			int result = 0; 
			for each v in values: 
				result += ParseInt(v); 
			Emit(AsString(result)); 
		```
## task相关定义

```go

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Complete
)

type TaskInfo struct {
	RStatus   TaskStatus //是否被处理
	StartTime time.Time
	Task      *Task
}

// Task /*
type Task struct {
	TaskId   int
	Input    string //文件位置，mapper阶段用
	Costatus Status //coordinator状态
	NReduce  int
	Temp     []string //用于保存每个work产生的n个文件、进行reduce时处理的文件
	Output   string   //输出文件名，reduce阶段用
}

```

worker 在请求任务以及提交任务时，无需提供自己的相关信息，因此`AskForTask`时无需提供参数，在返回值中可以获得自己需要的文件名，nreducenum，taskid等信息，在`CommitTask`时相关的信息在参数中交付给master而无需获得返回值。



## coordinator 相关定义
```go

type Status int

const (
	Map Status = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	mu        sync.Mutex
	TaskQueue chan *Task
	TaskInfo  map[int]*TaskInfo
	NReduce   int
	Files     []string
	Temp      [][]string
	Exit      bool
}
```

1. TaskQueue 队列中存储未发放的任务，TaskInfo中存储已经发放，未提交的任务，
2. 在初始化时，将map任务全部加入到队列中，同时将任务id作为key，任务作为value存入taskinfo中，在每次提交任务时，在taskinfo中清除对应的任务。
3. 完成所有map任务后，再将所有的reduce任务加入队列，等待worker请求任务
4. 同时master还需负责监测worker是不是离线，需要每5秒钟检测一下taskinfo中的任务是否超时，如果超时，就将对应的任务重新加入到队列中，等待worker请求该任务。

在worker请求任务时，有以下四种状态
1. 处于map阶段且有任务，此时返回一个map任务
2. 处于ruduce阶段且有任务，此时返回一个reduce任务
3. 处于任意阶段但是还有任务未提交，此时返回sleep，让worker节点等待一段时间再重新请求
4. 已结束任务，此时返回exit，告知worker退出进程

~~我在最开始调试的时候发现master等待任务完成且目前没有新任务时，如果返回exit，worker会退出并且不会重新连接，此时无法完成mapper任务。~~

## worker相关定义

worker 相关函数主要参考mrsequential.go中的写法，每次请求任务以及提交任务时都调用call函数实现。

worker根据请求到的task分别执行相应的动作。


```txt
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS

```




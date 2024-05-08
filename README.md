# 6.5840
spring 2024

## p1 MapReduce
## p2 Key/Value Server
## p3 Raft
### leader election
### log  
### persistence   
### log compaction   
## p4 Fault-tolerant Key/Value Service
### Key/value service without snapshots
### Key/value service with snapshots

hint：
1. 在写raft实验时的entry一定要有Index字段，否则debug会很艰难
    ```go
    type Entry struct {
	    Index   int
	    Term    int
	    Command interface{}
    }
    ```
    我初期没有添加Index字段，但是在写3D时因为到处bug，不得已从3A重新remake了相关代码。

2. raft的snapshotBasic测试代码会莫名其妙的与applyLog函数出现死锁🤔，希望有时间了会研究一下相关代码，检查一下死锁的原因。    
    [TODO]



[mapreduce](./doc/mr.md)    
[mapreduce-paper](./doc/mr-paper.md)    
[raft](./doc/raft.md)   
[raft-paper](./doc/raft-paper.md)


[TODO] 有时间继续补充其他论文
# Raft

Raft的日志设计一定要加上Index，否则设计snapshot的时候会非常难debug，我在写3D的时候，因为很难debug，后来索性remake了。

```go
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}
```

但是remake后debug的速度真的很快，很多需要判断边界条件的地方也很简单的解决了。

## leader election

在节点从Leader转变为Follower之后，要及时停止发送心跳，这里可以使用信号量实现，但是我发现可以使用管道实现。在`close(rf.heartChan)` 时，立刻会执行`case <-rf.heartChan:`，因此可以使用一个管道实现信号量。这样就可以实现及时的退出心跳。

请求投票的处理逻辑如下

1. args.term < currentTerm  :  返回投票失败
2. args.term == currentTerm and have voted  ： 返回投票失败
3. args.term > currentTerm: 转换为对方的Follower 并且重置超时计时器
4. args.term == currentTerm and not vote  :  比较日志
5. 最终  比较 lastLogIndex and lastLogTerm :
	1. 对方日志新于自己：投票成功
	2. 投票失败

这里我犯了一个错误，节点的VoteFor应该在currentTerm转变时更改，而不是在转变节点状态时改变。  
 
在Candidate收到超过半数选票时，就可以转变为Leader，然后进行heartBeat，而不需等待所有节点返回reply。

如果Candidate收到的reply.term > rf.currentTerm ， 就要立即转变为对应的Follower。  

##  log

在这里，某节点的log会变为空，如果没有Index，在获得索引的时候，会处理许多边界条件。但是我在这里使用了一个比较简单的处理方法，就是当log为空时，向log中插入一条空日志，

```go

func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		rf.logs = []Entry{{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}}
	}
	return rf.logs[len(rf.logs)-1].Index
}
```

这样很容易的解决了我在之前写的时候遇到的处理边界条件的问题。

节点在收到rpc的处理如下

1. args.term < currentTerm :  return false
2. args.term > currentTerm :  转换为对应的Follower，更新自己的任期
3. 如果对方的日志与自己日志不匹配： return false
4. 将自己的日志截断，并将args.entry添加到自己的日志中
5. 更新自己的commitIndex，然后应用到状态机中
 
Leader节点收到reply后的处理如下

1. 如果当前该peer已不是leader，则无需处理
2. 如果reply.term > currentTerm 转变为对应的Follower，更新term
3. 添加失败，说明对方的日志过于久远，更新nextIndex，等待重新发送新的日志
4. 计算是否有过半数机器提交了某日志，如果有提交则应用此日志及之前日志。

在获得AppendEntries时，要发送的日志可能小于0，此时`rf.nextIndex[server]==0`，因此此时args.PrevLogTerm应该等于0，

在leader提交日志时，应该查看Figure8，不提交之前任期的日志。只根据自己任期的日志的复制的节点数量是否过半数来决定是否将该日志应用到状态机，同时将之前的日志应用的状态机。


## persistence

persist的实现比较简单，在每次voteFer，currentTerm，log被修改的时候都要及时执行persist。

为了完全通过3C的测试，需要在appendEntryReply中增加
```go
ConflictIndex int
ConflictTerm  int
```

来优化冲突日志。
## log compaction
 发送installSnapshot的时机问题：
	在leader发送AppendEntriesRpc时出现nextIndex[i] <= rf.lastIncludedIndex情况时，因为nextIndex[i]是节点S[i]期待收到的下条命令乐观估计，rf.lastIncludedIndex是leader已经apply且其之前的log都已被删掉。

如果节点收到超过自己的最大日志索引的快照，则完全丢弃自己的日志（但是我的操作时插入一条空的日志，时刻保证自己的日志不为空以减少边界条件的判断），然后将快照应用到自己的快照中。

# summary

这个实验给我的最大感受是做后面的实验还要重新修改前面的代码，~~我不是已经pass测试了吗~~ ，做实验4的时候疯狂修改raft的边界条件的判断。

虽然如此，还是不能通过实验4的速度测试。 估计是raft的设计问题，不想修改了。
```txt
Test: ops complete fast enough (4A) ...
--- FAIL: TestSpeed4A (126.30s)
    test_test.go:419: Operations completed too slowly 125.863652ms/op > 33.333333ms/op


Test: ops complete fast enough (4B) ...
--- FAIL: TestSpeed4B (126.33s)
    test_test.go:419: Operations completed too slowly 125.54246ms/op > 33.333333ms/op

```

实验4都不能通过测试，实验5索性摆烂。

最后，如果无法修改测试，不妨试着修改一下raft的超时时长。maybe useful。

感谢以下github仓库的帮助：
* [https://github.com/fravenx/MIT-6.824.git](https://github.com/fravenx/MIT-6.824.git)
* [https://github.com/epegase/6.824.git](https://github.com/epegase/6.824.git)
* [https://github.com/aure-lee/MIT6.5840-2024Spring.git](https://github.com/aure-lee/MIT6.5840-2024Spring.git)
* [https://github.com/maemual/raft-zh_cn.git](https://github.com/maemual/raft-zh_cn.git)



# MIT 6.284 课程 raft部分 学习笔记
## Raft分布式一致性协议介绍
集群中有很多台机器，每台机器都是一个值的副本(replica)，一致性要求所有机器就这个值达成共识。

这听起来很抽象，这里举一个简单的例子。我们有3台服务器，分别是server a、server b、server c，
这三台服务器都保存了某场篮球比赛的比分，篮球比赛正在进行，所以比分一直在更新。

某一个时刻最新的得分83比80已经更新到了server a上，所以用户访问server a查询比分的时候，得到的就是最新的得分；
如果同一时刻，另一个用户向server b查询比分，最近的比分还没有更新到b，所以该用户得到的是过期的比分。
这样两个用户得到比分就是不一致。分布式一致性协议就是为了解决这个问题。

raft实现了强一致性也称线性一致性，即读到的一定是最近写入的。

Raft的总体目标是将log完全一样地复制到集群中的所有机器上，
用来创建所谓的Replicated State Machine(多副本状态机，就是具有多个copy的应用程序)。

引入log这个概念，有助于使这些state machines执行完全一样的命令。
下面解释下运作过程：如果一个客户端，想执行一个command（命令，指令，指的是具体的某个操作请求），
那么它可以请求其中一台state machine，这台machine就把这个命令，如command X，记录到自己的本地日志log中，
另外还要把command X传递给其他所有machines；
其他machine也在各自的log中记录下这条命令；
一旦这条command X被safely replicated(安全地复制)到所有machine的所有log中，
那么这个command X就可以被传递给各个machine开始执行，一旦有machine执行完成命令X，
就会把结果返回给客户端。


在任意时刻，raft处于以下三种状态中的一种:
1. leader：同一时刻至多1个leader，处理所有客户端请求和日志复制
2. candidate：从follower到leader的中间状态；这是选举过程中的一个临时状态
3. follower：大部分节点处于follower状态，follower是完全被动，只能响应leader和candidate发来的消息
## Part A: Leader election and heartbeats
1. 集群启动时某个server能够成功的当选成为leader。
2. 在不出现宕机的情况下，leader周期性地发送作为心跳包(即不带command的AppendEntries RPC)，来保持leader地位。
3. 当old leader宕机或者old leader发出的心跳包丢失的情况下，能够选出新的leader。

   

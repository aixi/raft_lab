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

raft把时间被划分成一个个的term，每个term都是一个number，
这个number必须单向递增且从未被用过；
每个term时期，分两部分，一是为这个term选举leader的过程，
二是一旦选举成功，这个leader在这个term的剩余时间内作为leader管理整个系统。
term的作用是用来检测过期信息的，如果一个leader宕机，集群选举出新的leader，然后老leader恢复了，仍然认为自己是leader，
还会向其他节点发送AppendEntries RPC，其他节点会返回自己的term，老leader就会发现自己的term很老，于是就会退回follower状态。
term就是raft集群中的逻辑时钟。

在任意时刻，raft集群中的每个节点都处于以下三种状态中的一种:
1. leader：同一时刻至多1个leader，处理所有客户端请求和日志复制
2. candidate：从follower到leader的中间状态；这是选举过程中的一个临时状态
3. follower：大部分节点处于follower状态，follower是完全被动，只能响应leader和candidate发来的消息。


## Part A: Leader election
1. 集群启动时某个server能够成功的当选成为leader。
2. 在不出现宕机的情况下，leader周期性地发送作为心跳包(即不带command的AppendEntries RPC)，来保持leader地位。
3. 当old leader宕机或者old leader发出的心跳包丢失的情况下，能够选出新的leader。

一开始raft集群中的所有节点都处于follower状态，这些follower都拥有一个定时器，
如果follower没有收到AppendEntries RPC或者RequestVote RPC，定时器就是到期，follower就会转变成candidate状态，增大自己的term，然后发起竞选，
candidate首先投自己一票，然后向其他server广播RequestVote RPC，节点会向最早发送的请求投票，并将投票结果持久化，如果candidate收到过半机器的选票，那么就成功当选成为leader，否则退回follower状态。
为了防止投票分散，每个定时器不能是相同的，一般都是electionTimeout到2倍electionTimeout中的某个随机数


投票的过程中有一些微妙的地方，不是所有节点都能够获得投票，这里就要提到raft safety requirement
如果某个leader发现某条log entry已经被提交，则这条entry必须存在于所有后续的leader中。这意味着，一旦某个leader即位，则在它的整个term时期内，它一定含有所有的已提交的log entries。
这就要求，只有包含了所有已经committed entries的节点才能获得投票。这就要求我们修改投票准则，不再是先到就可以获得投票。

raft的做法是当candidate的log比voting server更完整才能才能获得投票，否则拒绝投票具体来说

- LastTerm_v > LastTerm_c
- (LastTerm_v == LastTerm_c) && (LastIndex_v > LastIndex_c)


如果出现以上两种情况就直接拒绝投票
还有一个值得注意的地方时，voteFor变量需要持久化，防止这种情况发生，
已经投票，但是由于网络原因，candidate没有收到投票的结果，此时candidate会重试RequestVote，只有当votedFor和candidate Id相同，才投票

(TODO)
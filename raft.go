package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"raft/rpc_mock"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive Logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Role uint32

const (
	Follower Role = iota
	Candidate
	Leader
	Shutdown

	HeartbeatInterval = 50
	VoteNull = -1
)



func (s Role) debugString() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Shutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

func intMin(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func intMax(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type AppendEntries struct {
	Term     int
	LeaderId int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	// see extended paper 5.3 最后一节
	// 当log consistency check不一致的时候，将会对nextIndex减1，每次出现log inconsistency只减少1，所以很慢，需要提高效率
	// 所以follower会在AppendEntriesReply中附加上ConflictTerm和ConflictTerm的第一个index
	// 论文还指出，这项优化并不一定是必须的，因为log inconsistency出现的频率不高
	ConflictTerm  int
	ConflictIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mutex     sync.Mutex            // Lock to protect shared access to this peer's state
	peers     []*rpc_mock.ClientEnd // RPC end points of all peers
	persister *Persister            // Object to hold this peer's persisted state
	me        int                   // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             Role
	heartbeatInterval time.Duration
	electionTimeout   time.Duration

	CurrentTerm int // all servers persistent
	VotedFor    int // all servers persistent
	Logs        []LogEntry // all servers persistent

	commitIndex int // all servers volatile
	lastApplied int // all servers volatile

	nextIndex  []int //only on leaders volatile
	matchIndex []int //only on leaders volatile

	applyCh        chan ApplyMsg
	appendEntryCh  chan bool
	grantVoteCh    chan bool
	becomeLeaderCh chan bool
	exitCh         chan bool
}

func (entry LogEntry) debugString() string {
	str := fmt.Sprintf("(i:%v,t:%v,c:%v)", entry.Index, entry.Term, entry.Command)
	return str
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).

	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	term = rf.CurrentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//DPrintf("persist:%v, %v, %v", rf.CurrentTerm, rf.VotedFor, rf.Logs)
	// FIXME: need mutex ?
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	err := d.Decode(&rf.CurrentTerm)
	err = d.Decode(&rf.VotedFor)
	err = d.Decode(&rf.Logs)
	if err != nil {
		log.Fatal("gob.NewDecoder.Decode error")
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	// leader completeness check
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist()

	// all servers
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
		// 这里不要直接return,这个节点不够up-to-date，是follower所以应该进行后续投票操作
	}

	voteGranted := false
	// 投票先到先得，如果已经投过票了，就不能再投了
	// rf.VotedFor == args.CandidateId是这样的情况，可能follower已经投票给candidate了，但是由于网络故障，candidate没收到
	// 所以 candidate 再次发送消息 requestVote
	// leader选择的条件，参看 paper 5.1 5.2 5.4 slides 17
	if args.Term >= rf.CurrentTerm && (rf.VotedFor == VoteNull || rf.VotedFor == args.CandidateId) &&
		                           (args.LastLogTerm > rf.getLastLogTerm() ||
		                           (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		voteGranted = true
		rf.VotedFor = args.CandidateId
		rf.state = Follower
		dropAndSet(rf.grantVoteCh)
		log.Infof("%v vote %v my term:%d, vote term:%d", rf.me, args.CandidateId, rf.CurrentTerm, args.Term)
	}

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = voteGranted
}

func dropAndSet(ch chan bool) {
	select {
	case <- ch:
	default:
	}
	ch <- true
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	defer rf.persist()

	success := false
	conflictTerm := 0
	conflictIndex := 0

	// all servers
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
		// 这里不要直接return, 进行后续的log replication操作
	}
	// rf.convertToFollower(term int)是有参数的，本节点的CurrentTerm设置为args.Term了
	if args.Term == rf.CurrentTerm {
		rf.state = Follower
		dropAndSet(rf.appendEntryCh)

		if args.PrevLogIndex > rf.getLastLogIndex() {
			// slides 22 页中 follower a的情况
			// missing entry
			conflictIndex = len(rf.Logs)
			conflictTerm = 0
		} else {
			// log consistency check
			prevLogTerm := rf.Logs[args.PrevLogIndex].Term
			if args.PrevLogTerm != prevLogTerm {
				conflictTerm = rf.Logs[args.PrevLogIndex].Term
				// find first index of conflictTerm
				// see Raft paper 5.3 最后3断
				for i := 1; i < len(rf.Logs); i++ {
					if rf.Logs[i].Term == conflictTerm {
						conflictIndex = i
						break
					}
				}
			}

			if args.PrevLogIndex == 0 || (args.PrevLogIndex <= rf.getLastLogIndex() && args.PrevLogTerm == prevLogTerm) {
				success = true
				index := args.PrevLogIndex
				// 这个arg.Entries是slice可以一次携带多个Command
				for i := 0; i < len(args.Entries); i++ {
					index++
					if index > rf.getLastLogIndex() {
						rf.Logs = append(rf.Logs, args.Entries[i:]...)
						break
					}

					if rf.Logs[index].Term != args.Entries[i].Term {
						log.Infof("Term not equal, Server(%v=>%v), prevIndex=%v, index=%v", args.LeaderId, rf.me, args.PrevLogIndex, index)
						for len(rf.Logs) > index {
							rf.Logs = rf.Logs[0 : len(rf.Logs)-1]
						}
						rf.Logs = append(rf.Logs, args.Entries[i])
					}
				}

				log.Infof("Server(%v=>%v) term:%v, Handle AppendEntries Success", args.LeaderId, rf.me, rf.CurrentTerm)
				// AppendEntries 5, 设置commitIndex为LeaderCommit和最后一个New Entry的较小值。
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = intMin(args.LeaderCommit, rf.getLastLogIndex())
				}
			}
		}
	}

	rf.applyLogs()

	reply.Term = rf.CurrentTerm
	reply.Success = success
	reply.ConflictIndex = conflictIndex
	reply.ConflictTerm = conflictTerm
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Logs, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	term := rf.CurrentTerm
	index := -1
	isLeader := rf.state == Leader

	if isLeader {
		index = rf.getLastLogIndex() + 1
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}

		//注意append entry必须与index设置在一个加锁位置，如果推迟append，会导致concurrent start失败。
		rf.Logs = append(rf.Logs, entry)
		rf.persist()
	}

	return index, term, isLeader
}

/**
If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and Logs[N].term == CurrentTerm:
set commitIndex = N
*/

/*
 entry commitment rule
1. the entry must be stored on a majority of servers
2. At least one new entry from the leader's term must also be stored on majority of servers(this need a sentinel value)
*/

func (rf *Raft) advanceCommitIndex() {
	matchIndexes := make([]int, len(rf.matchIndex))
	copy(matchIndexes, rf.matchIndex)
	matchIndexes[rf.me] = len(rf.Logs) - 1
	sort.Ints(matchIndexes)

	N := matchIndexes[len(rf.peers) / 2]
	log.Infof("matchIndexes:%v, N:%v", matchIndexes, N)

	if rf.state == Leader && N > rf.commitIndex && rf.Logs[N].Term == rf.CurrentTerm {
		log.Infof("Server(%v) advanceCommitIndex (%v => %v)", rf.me, rf.commitIndex, N)
		rf.commitIndex = N
		rf.applyLogs()
	}
}

func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(serverIndex int) {
			for {
				rf.mutex.Lock()
				if rf.state != Leader {
					rf.mutex.Unlock()
					return
				}

				nextIndex := rf.nextIndex[serverIndex]
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.Logs[nextIndex:]...)
				args := AppendEntriesArgs {
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(serverIndex),
					PrevLogTerm:  rf.getPrevLogTerm(serverIndex),
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mutex.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(serverIndex, args, reply)
				log.Infof("SendAppendEntries (%v=>%v), args:%v", rf.me, serverIndex, args)
				if !ok {
					return
				}
				rf.mutex.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.convertToFollower(reply.Term)
					rf.mutex.Unlock()
					return
				}
				if !rf.checkState(Leader, args.Term) {
					rf.mutex.Unlock()
					return
				}
				if reply.Success {
					// AppendEntries成功，更新对应raft实例的nextIndex和matchIndex值, Leader 5.3
					rf.matchIndex[serverIndex] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1
					log.Infof("SendAppendEntries Success(%v => %v), nextIndex:%v, matchIndex:%v", rf.me, serverIndex, rf.nextIndex, rf.matchIndex)
					rf.advanceCommitIndex()
					rf.mutex.Unlock()
					return
				} else {
					// AppendEntries失败，减小对应raft实例的nextIndex的值重试 paper 5.3
					// 这里要注意理解conflictIndex,conflictTerm在减少重试次数方面起的作用
					newIndex := reply.ConflictIndex
					for i := 1; i < len(rf.Logs); i++ {
						entry := rf.Logs[i]
						if entry.Term == reply.ConflictTerm {
							newIndex = i + 1
						}
					}
					rf.nextIndex[serverIndex] = intMax(1, newIndex)
					log.Infof("SendAppendEntries failed(%v => %v), decrease nextIndex(%v):%v", rf.me, serverIndex, serverIndex, rf.nextIndex)
					rf.mutex.Unlock()
				}
			}
		} (i)
	}
}

// 将msg放入applyCh即是将command 给state machine执行
func (rf *Raft) applyLogs() {
	//注意这里的for循环，如果写成if那就错了，会无法通过lab-2B的测试。
	for rf.commitIndex > rf.lastApplied {
		log.Infof("Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v", rf.me, rf.commitIndex, rf.lastApplied, rf.Logs[rf.lastApplied].Command)
		rf.lastApplied++
		entry := rf.Logs[rf.lastApplied]
		msg := ApplyMsg{
			Index:   entry.Index,
			Command: entry.Command,
		}
		rf.applyCh <- msg //applyCh在test_test.go中要用到
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	log.Infof("Kill Server(%v)", rf.me)
	dropAndSet(rf.exitCh)
}

func getRandomElectionTimeout() time.Duration {
	randomTimeout := 300 + rand.Intn(100)
	electionTimeout := time.Duration(randomTimeout) * time.Millisecond
	return electionTimeout
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	log.Infof("Convert server(%v) state(%v=>candidate) term(%v)", rf.me,
		rf.state.debugString(), rf.CurrentTerm+1)
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
}

func (rf *Raft) leaderElection() {
	rf.mutex.Lock()
	if rf.state != Candidate {
		rf.mutex.Unlock()
		return
	}

	args := RequestVoteArgs{
		rf.CurrentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}
	rf.mutex.Unlock()

	var voteReceived int32 = 1

	// broadcast voteRequestRPC
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int, args RequestVoteArgs) {
			reply := &RequestVoteReply{}
			log.Infof("sendRequestVote(%v=>%v) args:%v", rf.me, idx, args)
			ret := rf.sendRequestVote(idx, args, reply)
			if ret {
				rf.mutex.Lock()
				defer rf.mutex.Unlock()
				if reply.Term > rf.CurrentTerm {
					rf.convertToFollower(reply.Term)
					return
				}

				if !rf.checkState(Candidate, args.Term) {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&voteReceived, 1)
				}

				if atomic.LoadInt32(&voteReceived) > int32(len(rf.peers) / 2) {
					log.Infof("Server(%d) win vote", rf.me)
					// 这两句调用顺序很重要
					rf.convertToLeader()
					dropAndSet(rf.becomeLeaderCh)
				}
			}
		}(i, args)
	}
}

func (rf *Raft) checkState(state Role, term int) bool {
	return rf.state == state && rf.CurrentTerm == term
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	log.Infof("Convert server(%v) state(%v=>follower) term(%v => %v)", rf.me,
		rf.state.debugString(), rf.CurrentTerm, term)
	rf.state = Follower
	rf.CurrentTerm = term
	rf.VotedFor = VoteNull
}

func (rf *Raft) getPrevLogIndex(serverIdx int) int {
	return rf.nextIndex[serverIdx] - 1
}

func (rf *Raft) getPrevLogTerm(serverIdx int) int {
	prevLogIndex := rf.getPrevLogIndex(serverIdx)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.Logs[prevLogIndex].Term
	}
}

// logs index started from 1
func (rf *Raft) getLastLogIndex() int {
	return len(rf.Logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex == 0 {
		return -1
	} else {
		return rf.Logs[lastLogIndex].Term
	}
}

func (rf *Raft) convertToLeader() {
	defer rf.persist()
	if rf.state != Candidate {
		return
	}

	log.Infof("Convert server(%v) state(%v=>leader) term %v", rf.me,
		rf.state.debugString(), rf.CurrentTerm)
	rf.state = Leader

	// paper figure 2中描述了，这些都是volatile state on leader
	// 必须 reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*rpc_mock.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VotedFor = VoteNull

	// 如果slice的第一个元素为nil会导致gob Encode/Decode为空,这里改为一个空的LogEntry便于编码。
	// 所以logs其实是从1开始的
	// Raft的commit规则除了要求 log被复制到了majority之外，还要求Leader至少还有一个current term的log被commit了，这要求一个哨兵值
	rf.Logs = make([]LogEntry, 0)
	sentinel := LogEntry{}
	rf.Logs = append(rf.Logs, sentinel)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.applyCh = applyCh

	rf.exitCh = make(chan bool, 1)
	rf.grantVoteCh = make(chan bool, 1)
	rf.appendEntryCh = make(chan bool, 1)
	rf.becomeLeaderCh = make(chan bool, 1)

	rf.heartbeatInterval = time.Duration(HeartbeatInterval) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	log.Infof("Make Server(%v)", rf.me)

	go func() {
	Loop:
		for {
			select {
			case <- rf.exitCh:
				log.Infof("Exit Server(%v)", rf.me)
				break Loop
			default:
			}

			electionTimeout := getRandomElectionTimeout()
			rf.mutex.Lock()
			state := rf.state
			rf.mutex.Unlock()
			log.Infof("Server(%d) state:%v, electionTimeout:%v", rf.me, state, electionTimeout)

			switch state {
			case Follower:
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-time.After(electionTimeout):
					rf.mutex.Lock()
					rf.convertToCandidate()
					rf.mutex.Unlock()
				}
			case Candidate:
				go rf.leaderElection()
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-rf.becomeLeaderCh:
				case <-time.After(electionTimeout):
					rf.mutex.Lock()
					rf.convertToCandidate()
					rf.mutex.Unlock()
				}
			case Leader:
				rf.startAppendEntries()
				time.Sleep(rf.heartbeatInterval)
			}
		}
	} ()

	return rf
}
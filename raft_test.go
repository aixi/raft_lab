package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "testing"
import "fmt"
import "time"
import "math/rand"
import "sync/atomic"
import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2A): initial election ...\n")

	// is a LEADER elected?
	cfg.checkOneLeader()
	fmt.Printf("Test (2A): checkOneLeader done ...\n")

	// does the LEADER+term stay the same if there is no network failure?
	term1 := cfg.checkTerms()
	fmt.Printf("Test (2A): 1st checkTerms done ...\n")
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	fmt.Printf("Test (2A): 2nd checkTerms done ...\n")
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	fmt.Printf("  ... Passed\n")
}

func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2A): election after network failure ...\n")

	leader1 := cfg.checkOneLeader()

	// if the LEADER disconnects, a new one should be elected.
	fmt.Printf("if the LEADER disconnects, a new one should be elected\n")
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old LEADER rejoins, that shouldn't
	// disturb the old LEADER.
	fmt.Printf("if the LEADER rejoins, that shouldn't disturb the old LEADER.\n")
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no LEADER should
	// be elected.
	fmt.Printf("no quorum\n")
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a LEADER.
	fmt.Printf("a quorum arises\n")
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent LEADER from existing.
	fmt.Printf("re-join of last node shouldn't prevent LEADER from existing\n")
	cfg.connect(leader2)
	cfg.checkOneLeader()

	fmt.Printf("  ... Passed\n")
}

func TestBasicAgree2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): basic agreement ...\n")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): agreement despite follower disconnection ...\n")

	cfg.one(101, servers)

	// follower network disconnection
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	// agree despite one disconnected server?
	cfg.one(102, servers-1)
	cfg.one(103, servers-1)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1)
	cfg.one(105, servers-1)

	// re-connect
	cfg.connect((leader + 1) % servers)

	// agree with full set of servers?
	cfg.one(106, servers)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers)

	fmt.Printf("  ... Passed\n")
}

func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): no agreement if too many followers disconnect ...\n")

	cfg.one(10, servers)

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	fmt.Printf("LEADER:%v disconnect 3 servers(%v %v %v)\n", leader, (leader + 1) % servers, (leader + 2) % servers, (leader + 3) % servers)
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	fmt.Printf("after Start(20), LEADER:%d index:%d ok:%v\n", leader, index, ok)
	if ok != true {
		t.Fatalf("LEADER rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	fmt.Printf("nCommitted count n:%d\n", n)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	fmt.Printf("connect 3 servers\n")
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a LEADER from
	// among their own ranks, forgetting index 2.
	// or perhaps
	leader2 := cfg.checkOneLeader()
	fmt.Printf("checkOneLeader leader2:%d\n", leader2)
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	fmt.Printf("after Start(30), leader2:%d index2:%d ok2:%v\n", leader2, index2, ok2)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	fmt.Printf("before one(1000)")
	cfg.one(1000, servers)

	fmt.Printf("  ... Passed\n")
}

func TestConcurrentStarts2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): concurrent Start()s ...\n")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		fmt.Printf("LEADER:%v term:%v ok:%v\n", leader, term, ok)
		if !ok {
			// LEADER moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				fmt.Printf("Start i:%v term1:%v ok:%v\n", i, term1, ok)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	fmt.Printf("  ... Passed\n")
}

func TestRejoin2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): rejoin of partitioned LEADER ...\n")

	cfg.one(101, servers)

	// LEADER network failure
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// make old LEADER try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// new LEADER commits, also for index=2
	cfg.one(103, 2)

	// new LEADER network failure
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// old LEADER connected again
	cfg.connect(leader1)

	cfg.one(104, 2)

	// all together now
	cfg.connect(leader2)

	cfg.one(105, servers)

	fmt.Printf("  ... Passed\n")
}

func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): LEADER backs up quickly over incorrect follower logs ...\n")

	cfg.one(rand.Int(), servers)

	// put LEADER and one follower in a partition
	leader1 := cfg.checkOneLeader()
	fmt.Printf("leader1:%v disconnect 3 servers(%v %v %v)\n", leader1, (leader1 + 2) % servers, (leader1 + 3) % servers, (leader1 + 4) % servers)
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	fmt.Printf("Start rand.Int 50 times(won't commit) leader1:%v\n", leader1)
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	fmt.Printf("leader1:%v disconnect 2 servers(%v %v)\n", leader1, (leader1 + 0) % servers, (leader1 + 1) % servers)
	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	fmt.Printf("leader1:%v connect 3 servers(%v %v %v)\n", leader1, (leader1 + 2) % servers, (leader1 + 3) % servers, (leader1 + 4) % servers)
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	fmt.Printf("Start rand.Int 50 times(will commit) leader1:%v\n", leader1)
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3)
	}

	// now another partitioned LEADER and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	fmt.Printf("leader2:%v disconnect other:%v\n", leader2, other)
	cfg.disconnect(other)

	// lots more commands that won't commit
	fmt.Printf("Start rand.Int 50 times(won't commit), leader1:%v\n", leader1)
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original LEADER back to life,
	fmt.Printf("disconnect all\n ")
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	fmt.Printf("connect:%v %v %v\n", (leader1 + 0) % servers, (leader1 + 1) % servers, other)
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	fmt.Printf("Start rand.Int 50 times(will commit) leader1:%v\n", leader1)
	for i := 0; i < 50; i++ {
		fmt.Printf("cfg.one(rand.Int(), 3) i:%v\n", i)
		cfg.one(rand.Int(), 3)
	}

	// now everyone
	fmt.Printf("connect all\n ")
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	fmt.Printf("Start rand.Int once\n")
	cfg.one(rand.Int(), servers)

	fmt.Printf("  ... Passed\n")
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): RPC counts aren't too high ...\n")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial LEADER\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// LEADER moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the LEADER, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	fmt.Printf("  ... Passed\n")
}

func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): basic persistence ...\n")

	cfg.one(11, servers)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1)
	cfg.connect(leader1)

	cfg.one(13, servers)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1)
	cfg.start1(leader2)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1)
	cfg.start1(i3)
	cfg.connect(i3)

	cfg.one(16, servers)

	fmt.Printf("  ... Passed\n")
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): more persistence ...\n")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1 + 1) % servers)
		cfg.start1((leader1 + 2) % servers)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1 + 3) % servers)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers)

	fmt.Printf("  ... Passed\n")
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): partitioned LEADER and one follower crash, LEADER restarts ...\n")

	cfg.one(101, 3)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader + 0) % servers)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2)

	cfg.start1((leader + 1) % servers)
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers)

	fmt.Printf("  ... Passed\n")
}

//
// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a LEADER, if there is one, to insert a command in the Raft
// Logs.  If there is a LEADER, that LEADER will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The LEADER in a new term may try to finish replicating Logs entries that
// haven't been committed yet.
//
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): Figure 8 ...\n")

	cfg.one(rand.Int(), 1)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				cfg.start1(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), servers)

	fmt.Printf("  ... Passed\n")
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, true)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): unreliable agreement ...\n")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1)
			}(iters, j)
		}
		cfg.one(iters, 1)
	}

	cfg.setUnreliable(false)

	wg.Wait()

	cfg.one(100, servers)

	fmt.Printf("  ... Passed\n")
}

func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, true)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): Figure 8 (unreliable) ...\n")

	cfg.one(rand.Int()%10000, 1)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setLongReordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int()%10000, servers)

	fmt.Printf("  ... Passed\n")
}

func internalChurn(t *testing.T, unreliable bool) {

	if unreliable {
		fmt.Printf("Test (2C): unreliable churn ...\n")
	} else {
		fmt.Printf("Test (2C): churn ...\n")
	}

	servers := 5
	cfg := makeConfig(t, servers, unreliable)
	defer cfg.cleanup()

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a LEADER
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe LEADER will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election electionTimer, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setUnreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	internalChurn(t, true)
}
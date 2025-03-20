package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type state int

const (
	follower state = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state         state
	lastHeartbeat time.Time

	// persistent state
	currentTerm int
	votedFor    int
	logs        []Log

	// volatile state
	commitIndex int
	lastApplied int

	// leader specific volatile state
	nextIndex  []int
	matchIndex []int

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	Term  int
	Entry any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("[%d] received RequestVote from %d term %d\n", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		fmt.Printf("[%d] rejected RequestVote from %d term %d as current term is %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			fmt.Printf("[%d] rejected RequestVote from %d term %d as already voted for %d\n", rf.me, args.CandidateId, args.Term, rf.votedFor)
			return
		}
	}
	if args.LastLogIndex < len(rf.logs)-1 {
		fmt.Printf("[%d] rejected RequestVote from %d term %d as last log index is %d\n", rf.me, args.CandidateId, args.Term, len(rf.logs))
		return
	}

	rf.state = follower
	rf.lastHeartbeat = time.Now()
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	fmt.Printf("[%d] granted RequestVote from %d term %d\n", rf.me, args.CandidateId, args.Term)

	// Your code here (3A, 3B).
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func getRandomDuration(min, max int64) time.Duration {
	ms := min + (rand.Int63() % (max - min))
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	duration := getRandomDuration(400, 600)
	for rf.killed() == false {
		fmt.Printf("[%d] ticker sleeping for %v\n", rf.me, duration)
		// pause for a random amount of time between 400 and 600
		// milliseconds.
		time.Sleep(duration)

		rf.mu.Lock()
		elapsed := time.Since(rf.lastHeartbeat)
		if elapsed > duration {
			if rf.state != leader {
				fmt.Printf("[%d] starting election\n", rf.me)
				// start in separate thread so that we start a new election if this one
				// times out
				go rf.startElection()
			}
		} else {
			// it has already been "elapsed" time since the last heartbeat
			duration = max(getRandomDuration(400, 600)-elapsed, 0)
		}
		rf.mu.Unlock()

		// Your code here (3A)
		// Check if a leader election should be started.
	}
}

func (rf *Raft) startElection() {
	for {
		rf.mu.Lock()

		rf.state = candidate
		rf.currentTerm++
		rf.votedFor = rf.me

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}

		// alright to unlock here as rf.peers is not modified
		rf.mu.Unlock()

		fmt.Printf("[%d] sending request votes\n", rf.me)
		replies := make(chan RequestVoteReply, len(rf.peers))
		for peerId := range rf.peers {
			if peerId == rf.me {
				continue
			}
			go func(peerId int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(peerId, &args, &reply); ok {
					replies <- reply
				}
			}(peerId)
		}

		votes := 1 // votes for self
		allVotesTimeout := time.NewTimer(getRandomDuration(400, 600))

	outer:
		for _ = range len(rf.peers) - 1 {
			select {
			case reply := <-replies:
				if reply.VoteGranted {
					votes++
					// got enough votes
					if votes > len(rf.peers)/2 {
						break outer
					}
				}
				break
			case <-allVotesTimeout.C:
				// timeout, ticker will retry elections
				return
			}
		}
		fmt.Printf("[%d] election finished with %d votes\n", rf.me, votes)

		rf.mu.Lock()

		// other threads could have changed state while we were waiting for replies
		if rf.state == candidate && votes > len(rf.peers)/2 {
			rf.state = leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}
			go rf.startHeartbeat()
			rf.mu.Unlock()
			fmt.Printf("[%d] elected as leader\n", rf.me)
			return
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) startHeartbeat() {
	for {
		rf.mu.Lock()
		state := rf.state
		if state != leader {
			rf.mu.Unlock()
			fmt.Printf("[%d] stopped heartbeat, no longer leader\n", rf.me)
			return
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.logs) - 1,
			PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(server, &args, &reply); ok {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = follower
						rf.votedFor = -1
						rf.mu.Unlock()
					}
				}
			}(i)
		}
		// fmt.Printf("[%d] heartbeat sent\n", rf.me)
		time.Sleep(time.Millisecond * 200)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("[%d] received AppendEntries from %d term %d\n", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.PrevLogIndex != len(rf.logs)-1 {
		return
	}

	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		return
	}
	// entries are valid

	rf.lastHeartbeat = time.Now()

	if len(args.Entries) > 0 {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		// rf.applyLogs()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.votedFor = -1
	rf.state = follower
	rf.lastHeartbeat = time.Now()
	rf.logs = []Log{Log{Term: 0, Entry: nil}}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

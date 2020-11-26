package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	labrpc "github.com/davidmkwon/MIT-6.824/src/labrpc"
)

const (
	MAX_TIME = 900
	MIN_TIME = 500
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// Go struct representing a log entry
// TODO: check if these fields need to be uppercased
//
type Log struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int   // latest term the server has seen
	votedFor    int   // candidateID that recieved vote in current term
	logs        []Log // log entries
	isLeader    bool  // whether this server is the leader

	// volatile state for all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry known to be applied

	// volatile for leaders
	nextIndex  []int // for each server an index for the next log entry to send
	matchIndex []int // for each server an index of highest log entry known to be replicated

	// potential variables to handle election timeouts
	//random    *rand.Rand
	isWaiting bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A)
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()

	return term, isleader
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // ID of candidate
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term ofcandidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm of replying server (?)
	VoteGranted bool // whether candidate is granted vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// get mutex to read raft variables
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		rf.currentTerm = args.Term
	}
	// immediately return if curent term is greater than candidate's term
	/*if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if votedFor is nil or the args' candidateId, check if candidate's
	// log is at least as up-to-date as receveiver's log
	if rf.votedFor == nil || rf.votedFor == args.CandidateID {
		// get the term of last log entry for current server
		latestTerm := rf.logs[len(rf.logs)-1].term
		// check if candidate and receiver have different terms for last log entries
		if args.LastLogTerm == latestTerm {
			// server with longer log is more up-to-date
			if args.LastLogIndex+1 >= len(rf.logs) {
				args.Term = rf.currentTerm
				reply.VoteGranted = true
				return
			}
		} else if args.LastLogTerm > latestTerm {
			// server with later term is more up-to-date
			args.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
	}*/

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	return
}

// go struct for AppendEntries RPC's arguments
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderID     int   // for follower to redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex log
	Entries      []Log // the log entires to store
	LeaderCommit int   // leader's commit index
}

// go struct for AppendEntries RPC's reply
type AppendEntriesReply struct {
	Term    int  // current term of server for leader to update itself with
	Success bool // true if follower has entry matching prevLogIndex and prevLogTerm
}

// handler for AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: reset the election timer at the end of function

	// get mutex to read raft variables
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// set isWaiting to false as heartbeat has been received
	rf.isWaiting = false

	// respond false if leader's term is less than current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// respond false if log doesn't contain an entry at prevLogIndex whose term doesn't match
	if args.PrevLogIndex < len(rf.logs) && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if there is an entry with same index but different terms, delete existing
	// and all that follow it
	return
}

// this function makes a AppendEntries RPC call to the given server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// begins an election for current raft instance
func (rf *Raft) startElection() bool {
	// number of votes received, mutex around it
	//var mu sync.Mutex
	requiredVotes := (len(rf.peers) / 2) + 1
	numVotes := 0

	// immediately vote for self and increment current term
	numVotes++
	rf.currentTerm++

	// wait group to wait for go routines
	//var wg sync.WaitGroup
	//wg.Add(len(rf.peers) - 1)

	// vote channel?
	votes := make(chan bool)

	// args and reply RequestVote structs
	rf.mu.Lock()
	args := &RequestVoteArgs{
		CandidateID:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		Term:         rf.currentTerm,
	}
	reply := &RequestVoteReply{}
	rf.mu.Unlock()

	// loop over the servers
	for idx := range rf.peers {
		// skip itself
		if idx == rf.me {
			continue
		}

		// anon func for sending RequestVote RPCs to the instances
		// if vote is received, increment numVotes
		// TODO: make this quit out early if enough votes are recevied
		go func(idx int, args *RequestVoteArgs, reply *RequestVoteReply) {
			fmt.Println("making request vote call")
			ok := rf.sendRequestVote(idx, args, reply)
			fmt.Println("received request vote call")
			if ok && reply.VoteGranted {
				/*mu.Lock()
				numVotes++
				mu.Unlock()*/
				votes <- true
			} else {
				votes <- false
			}
			//wg.Done()
		}(idx, args, reply)
	}

	// wait for go routines
	//wg.Wait()

	count := 0
	for numVotes < requiredVotes || count < len(rf.peers)-1 {
		select {
		case voteGranted := <-votes:
			if voteGranted {
				numVotes++
				fmt.Println("Received vote, numVotes is:", numVotes)
			}
			count++
		default:
		}
	}

	// return whether the number of votes received exceeds requiredVotes
	return numVotes >= requiredVotes
}

// function that waits for heartbeat from the leader. if no heartbeat
// is received, then this raft instance will become a candidate and
// start an election. if a heartbeat is received, the election timer
// is reset.
func (rf *Raft) waitForCandidacy() {
	// set isWaiting to true
	rf.mu.Lock()
	rf.isWaiting = true
	rf.mu.Unlock()

	// sleep for a random time between MIN_TIME and MAX_TIME
	//randomTime := time.Duration(rand.Intn(MAX_TIME-MIN_TIME)+MIN_TIME) * time.Millisecond
	randomTime := time.Duration(rand.Int63()%333+550) * time.Millisecond
	time.Sleep(randomTime)

	// check if isWaiting is still true (no heartbeat received), and start election
	rf.mu.Lock()
	isWaiting := rf.isWaiting
	rf.mu.Unlock()
	if isWaiting {
		fmt.Println("starting election")
		success := rf.startElection()
		fmt.Println("election results")
		if success {
			fmt.Println("Waited for", randomTime)
			rf.mu.Lock()
			fmt.Println("votes received!")
			rf.isLeader = true
			rf.mu.Unlock()
		}
	}
}

// function that sends heartbeats out to the other raft servers.
func (rf *Raft) sendHeartbeats() {
	// sleep for .1 seconds (tester requirement)
	time.Sleep(50 * time.Millisecond)

	// wait group to wait for goroutines
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)

	// args and reply RequestVote structs
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}

	// send AppendEntry RPCs to all the raft instances
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		// anon func for sending RequestVote RPCs to the instances
		// if vote is received, lock mutex and increment numVotes
		go func(idx int) {
			rf.sendAppendEntries(idx, args, reply)
			wg.Done()
		}(idx)
	}

	// wait for hearbeat RPCs to all finish
	wg.Wait()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize other raft variables
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	//rf.random = rand.New(rand.NewSource(rand.Int63()))
	rf.isWaiting = true
	rf.isLeader = false

	// goroutine to send heartbeats if this raft instance is leader,
	// else wait for hearbeats
	go func() {
		for {
			rf.mu.Lock()
			if rf.isLeader {
				rf.mu.Unlock()
				//fmt.Printf("instance %d is leader\n", me)
				rf.sendHeartbeats()
			} else {
				rf.mu.Unlock()
				rf.waitForCandidacy()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

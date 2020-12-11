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
	"log"
	"math/rand"
	"sync"
	"time"

	labrpc "github.com/davidmkwon/MIT-6.824/src/labrpc"
)

// series of constants to represent potential states
const (
	FOLLOWER  = iota // follower state
	CANDIDATE = iota // candidate state
	LEADER    = iota // leader state
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
//
type Log struct {
	Command interface{}
	Term    int
	Index   int // TODO: fill this field in and use it
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
	state       int   // the state of this raft instance

	// volatile state for all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry known to be applied

	// volatile for leaders
	nextIndex  []int // for each server an index for the next log entry to send
	matchIndex []int // for each server an index of highest log entry known to be replicated

	// channels for communicating between functions
	aeChan       chan bool     // channel to signal that AppendEntries RPC is received
	electionChan chan bool     // channel to signal that election is over
	rvChan       chan bool     // channel to signal that RequestVote RPC is GRANTED
	commitChan   chan bool     // channel to signal that there are commands to be applied
	applyChan    chan ApplyMsg // channel where Raft sends ApplyMsg to service/tester
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A)
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
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
	Term        int  // currentTerm of replying server
	VoteGranted bool // whether candidate is granted vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// reply false immediately if candidate's term is less than current term
	if args.Term < rf.currentTerm {
		fmt.Println(args.CandidateID, "has term", args.Term, "while", rf.me, "has term", rf.currentTerm)
		return
	}

	// if sent term is > than current term, automatically convert to follower and change current term
	if args.Term > rf.currentTerm {
		fmt.Println(rf.me, "converted to follower")
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}

	// TODO: add election restriction (5.4.1)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// *** check if candidate's log is at least as up to date as raft instance's:
		// get the term and index of last log entry for current server
		latestTerm := rf.logs[len(rf.logs)-1].Term
		latestInd := rf.logs[len(rf.logs)-1].Index
		// check if candidate and receiver have different terms for last log entries
		if (latestTerm == args.LastLogTerm && args.LastLogIndex >= latestInd) ||
			(latestTerm < args.LastLogTerm) {
			// set fields and send on rvChan
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.state = FOLLOWER
			rf.rvChan <- true
			return
		}
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == CANDIDATE && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
	}
	return ok
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
	Term      int  // current term of server for leader to update itself with
	Success   bool // true if follower has entry matching prevLogIndex and prevLogTerm
	NextIndex int  // the index at which there are no conflicts in log entry
}

// handler for AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me, "recevied AE RPC from", args.LeaderID)

	// set values for reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// index of last log entry
	lastLogInd := rf.logs[len(rf.logs)-1].Index

	// return false immediately if leader's term is less than current term
	if args.Term < rf.currentTerm {
		reply.NextIndex = lastLogInd + 1
		return
	}

	// send to aeChan that a VALID heartbeat / AE RPC was received
	rf.aeChan <- true

	// if sent term is > than current term, automatically convert to follower and change current term
	if args.Term > rf.currentTerm {
		fmt.Println(rf.me, "converted to follower")
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	// TODO: make sure nothing goes wrong when Entries is empty and this is just a heartbeat
	// return false if PrevLogIndex points past the current log
	if args.PrevLogIndex > lastLogInd {
		reply.NextIndex = lastLogInd + 1
		return
	}

	// return false if log doesn't contain an entry at PrevLogIndex with term = PrevLogTerm
	// we calculate the index at PrevLogIndex relatively according to the index of the first entry
	baseInd := rf.logs[0].Index
	relativeInd := args.PrevLogIndex - baseInd
	if baseInd < args.PrevLogIndex {
		relativeTerm := rf.logs[relativeInd].Term
		if args.PrevLogTerm != relativeTerm {
			for i := args.PrevLogIndex - 1; i >= baseInd; i-- {
				if rf.logs[i-baseInd].Term != relativeTerm {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	// if existing entry conflicts with new one, delete it and all that follow
	// append any new entries not already in the log
	// note: we can just condense steps 3 and 4 in Figure 2 by basically skipping step 3
	// and just deleting all entries after PrevLogIndex and adding all the entries
	lastLogInd = rf.logs[len(rf.logs)-1].Index
	if baseInd <= args.PrevLogIndex {
		rf.logs = rf.logs[:relativeInd+1]
		rf.logs = append(rf.logs, args.Entries...)
		reply.Success = true
		reply.NextIndex = lastLogInd + 1
	}

	// if LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		var min int
		if args.LeaderCommit < lastLogInd {
			min = args.LeaderCommit
		} else {
			min = lastLogInd
		}
		rf.commitIndex = min

		// send on commitChan because commitIndex changed
		rf.commitChan <- true
	}

	return
}

// this function makes a AppendEntries RPC call to the given server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok && rf.state == LEADER && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		} else if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// if raft is leader, then add the log entry
	// note: sending out this new entry to the other instances will be taken
	// care of in the next iteration Act() loop
	if isLeader {
		index = rf.logs[len(rf.logs)-1].Index + 1
		newLog := Log{
			Term:    term,
			Command: command,
			Index:   rf.logs[len(rf.logs)-1].Index + 1,
		}
		rf.logs = append(rf.logs, newLog)
	}

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

//
// function that simulates election for current candidate by sending out
// RequestVote RPCs to the other servers
//
func (rf *Raft) startElection() {
	// vote variables
	numVotes := 0
	requiredVotes := (len(rf.peers) / 2) + 1

	// immediately vote for self and increment current term
	votes := make(chan bool)

	// args and reply RequestVote structs
	rf.mu.Lock()
	// immediately vote for self and increment current term
	rf.currentTerm++
	rf.votedFor = -1
	numVotes++
	args := &RequestVoteArgs{
		CandidateID:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		Term:         rf.currentTerm,
	}
	rf.mu.Unlock()

	// loop over the servers
	for idx := range rf.peers {
		// skip self
		if idx == rf.me {
			continue
		}

		// anon func for sending RequestVote RPCs to the instances
		// if vote is received, increment numVotes
		reply := &RequestVoteReply{}
		go func(idx int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.sendRequestVote(idx, args, reply)
			if ok && reply.VoteGranted {
				votes <- true
			} else {
				votes <- false
			}
		}(idx, args, reply)
	}

	// wait for the required number of votes or responses from every server
	numResponses := 1
	for numVotes < requiredVotes && numResponses != len(rf.peers) {
		voteGranted := <-votes
		if voteGranted {
			numVotes++
		}
		numResponses++
	}

	// return the results of the election onto the election channel
	if numVotes >= requiredVotes {
		rf.electionChan <- true
	} else {
		rf.electionChan <- false
	}
}

//
// function that sends AppendEntry RPCs to all the peer servers
//
func (rf *Raft) sendAppendEntriesAll() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check for a N where the majority of matchIndex[i] >= N and log[N].Term = currentTerm
	// if such N exists, then set commitIndex = N
	N := rf.commitIndex
	// TODO: add a relativeInd() function so you need to stop making this var?
	baseInd := rf.logs[0].Index
	for currN := rf.commitIndex + 1; currN <= rf.logs[len(rf.logs)-1].Index; currN++ {
		numServers := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= currN && rf.logs[currN-baseInd].Term == rf.currentTerm {
				numServers++
			}
		}
		if numServers > len(rf.peers)/2 {
			N = currN
		}
	}

	// if we have found an N such that meets the above condition, change commitIndex
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitChan <- true
	}

	// loop over raft servers and send AppendEntry RPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= baseInd {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[i]-1-baseInd].Term,
			Entries:      make([]Log, len(rf.logs)-(rf.nextIndex[i]-baseInd)),
			LeaderCommit: rf.commitIndex,
		}
		copy(args.Entries, rf.logs[rf.nextIndex[i]-baseInd:])
		reply := AppendEntriesReply{}

		// anon func for sending AppendEntry RPCs to the instances
		go func(idx int, args AppendEntriesArgs, reply AppendEntriesReply) {
			rf.sendAppendEntries(idx, &args, &reply)
		}(i, args, reply)
	}
}

//
// function that infinitely loops, executing the actions according to
// the state of the raft instance. think of as the controller for
// the state machine
//
func (rf *Raft) act() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case FOLLOWER:
			randomTime := time.Duration(rand.Int63()%333+550) * time.Millisecond
			select {
			case <-time.After(randomTime):
				// timeout -> so become candidate and start election
				fmt.Println(rf.me, "becoming candidate")
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
				break
			case <-rf.aeChan:
				// should only get a message from this channel if a VALID AE RPC is received
				break
			case <-rf.rvChan:
				// this raft instance granted a vote to a candidate
				break
			}
			break
		case CANDIDATE:
			randomTime := time.Duration(rand.Int63()%333+550) * time.Millisecond
			go rf.startElection()
			select {
			case <-time.After(randomTime):
				// increment currentTerm again and start new election (this one timed out)
				// note: this should be handled in the next iteration of the loop
				fmt.Println(rf.me, "election timed out")
				break
			case <-rf.aeChan:
				// should only get a message from this channel if a VALID heartbeat is received
				// note: the RPC receiver should also have updated state to follower
				fmt.Println(rf.me, "received heartbeat")
				break
			case <-rf.rvChan:
				break
			case success := <-rf.electionChan:
				// switch state to leader and send heartbeats IF election was success
				fmt.Println(rf.me, "election results")
				if success {
					// change state and reinitialize nextIndex and matchIndex
					fmt.Println(rf.me, "won election")
					rf.mu.Lock()
					rf.state = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					lastLogInd := rf.logs[len(rf.logs)-1].Index
					for i := range rf.peers {
						rf.nextIndex[i] = lastLogInd + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
				break
			}
			break
		case LEADER:
			// send out hearbeats/AppendEntries RPC
			go rf.sendAppendEntriesAll()
			time.Sleep(50 * time.Millisecond)
			break
		default:
			log.Fatal("invalid raft state:", state)
			break
		}
	}
}

//
// This function infinitely loops checking for a message from the commitChan,
// denoting that the commitIndex has changed and there are more messages to
// send to the client
//
func (rf *Raft) applyCommit() {
	for {
		<-rf.commitChan
		rf.mu.Lock()
		baseInd := rf.logs[0].Index
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				Index:   i,
				Command: rf.logs[i-baseInd].Command,
			}
			rf.applyChan <- msg
			rf.lastApplied = i
		}
		rf.mu.Unlock()
	}
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
	rf.state = FOLLOWER
	rf.aeChan = make(chan bool, 10)
	rf.rvChan = make(chan bool, 10)
	rf.electionChan = make(chan bool, 10)
	rf.commitChan = make(chan bool, 10)
	rf.applyChan = applyCh

	// start act function in separate goroutine
	go rf.act()
	go rf.applyCommit()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

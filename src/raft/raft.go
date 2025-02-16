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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	ElectionTimeout = time.Second * 3
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct{
	Command string // placeholder, subject to change
	Term int
}

type State int8

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state: updated on stable storage before responding to RPCs
	currentTerm int
	candidateVotedFor int // in the current term
	log []*LogEntry

	// Volatile state: exists on all nodes
	currentState State
	lastSuccessRPCReceivedTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == Leader
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


type RequestVoteArgs struct {
	Term int // candidate's term
	CandidateId int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
}


type RequestVoteReply struct {
	Term int // the term of the node from which vote was requested, for the candidate to update its own term
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// return if the candidate's term number is lesser than mine
	if args.Term < rf.currentTerm {
		return
	}

	// if candidate's term is higher than mine, update my term and transition to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = Follower
	}

	// if not already voted for another candidate, vote for this candidate if its log is at least as updated as mine
	if rf.candidateVotedFor == -1 {
		myLastLogTerm := rf.getLastLogTerm()
		// if candidate has a larger last log entry term, it is more up to date than me
		if args.LastLogTerm > myLastLogTerm {
			reply.VoteGranted = true
			rf.candidateVotedFor = args.CandidateId
			rf.lastSuccessRPCReceivedTime = time.Now()
			return
		}

		// else if the last log entry term is same, check for log size
		if args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= len(rf.log) - 1 {
			reply.VoteGranted = true
			rf.candidateVotedFor = args.CandidateId
			rf.lastSuccessRPCReceivedTime = time.Now()
			return
		}

		// do not vote for this candidate in any other case
	} else if rf.candidateVotedFor == args.CandidateId {
		// this can happen in case the reply to the candidate was lost, and it retried the RPC
		reply.VoteGranted = true
	}

	// else i've already voted for someone, don't do anything
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

type AppendEntriesArgs struct {
	// required for leader election to be successful
	Term int // leader's term
	LeaderId int
	PrevLogIndex int 
	PrevLogTerm int
	Entries []*LogEntry // entries that the receiving node should append to its log
}


type AppendEntriesReply struct {
	Term int // the term of the node from which vote was requested, for the leader to update its own term
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// more to be added

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// don't do anything if term of the leader is outdated
	if args.Term < rf.currentTerm {
		return
	}

	// don't do anything if the entry at prevLogIndex does not have the same term as prevLogTerm
	if rf.getLogTermAtIndex(args.PrevLogIndex) != args.PrevLogTerm {
		return
	}

	// if leader's term is higher than mine, update my term and transition to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = Follower
	}

	rf.lastSuccessRPCReceivedTime = time.Now()

	// append the log entries
	rf.log = append(rf.log, args.Entries...)
}

func (rf *Raft) sendHeartbeats() {
	for rf.currentState == Leader{
		rf.mu.Lock()
		// send empty AppendEntriesRPC to all servers except myself
		for idx := range rf.peers {
			if idx != rf.me {
				go func (server int)  {
					args := &AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: rf.getLastLogIndex(),
						PrevLogTerm: rf.getLastLogTerm(),
						Entries: make([]*LogEntry, 0),
					}
					reply := &AppendEntriesReply{}
			
					if rf.sendAppendEntries(server, args, reply) {
						if reply.Term > rf.currentTerm {
							// if a node has a higher term than me, I'm not the leader anymore
							fmt.Printf("[node %d, term %d, state %v]: node %d has a higher term (%d) than me, transitioning to follower\n", rf.me, rf.currentTerm, rf.currentState, server, reply.Term)
							rf.currentTerm = reply.Term
							rf.currentState = Follower
						}
					}
				}(idx)
			}
		}
		rf.mu.Unlock()
		// send 10 hearbeats in 1 second
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// pause for a random amount of time between 50 and 350
		// milliseconds. This is jitter
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		switch rf.currentState {
		case Follower:
			rf.mu.Lock()
			if time.Since(rf.lastSuccessRPCReceivedTime) < ElectionTimeout {
				rf.mu.Unlock()
				continue
			}
			// else, transition to candidate
			fmt.Printf("[node %d, term %d, state %v]: transitioning to candidate from follower\n", rf.me, rf.currentTerm, rf.currentState)
			rf.currentState = Candidate
			rf.mu.Unlock()
			fallthrough
		case Candidate:
			rf.mu.Lock()
			if time.Since(rf.lastSuccessRPCReceivedTime) < ElectionTimeout {
				rf.mu.Unlock()
				continue
			}
			// increment current term and start election
			rf.currentTerm++
			// reset election timeout
			rf.lastSuccessRPCReceivedTime = time.Now()
			// vote for myself
			rf.candidateVotedFor = rf.me

			fmt.Printf("[node %d, term %d, state %v]: starting election\n", rf.me, rf.currentTerm, rf.currentState)
			// request votes
			votesReceived := rf.requestVotes()

			fmt.Printf("[node %d, term %d, state %v]: received %d votes\n", rf.me, rf.currentTerm, rf.currentState, votesReceived)

			if votesReceived + 1 < len(rf.peers) / 2 {
				// did not receive majority votes
				rf.mu.Unlock()
				continue
			}

			fmt.Printf("[node %d, term %d, state %v]: received majority votes, transitioning to leader\n", rf.me, rf.currentTerm, rf.currentState)
			// received majority votes, transition to leader
			rf.currentState = Leader
			rf.mu.Unlock()
			fallthrough
		case Leader:
			// send heartbeats
			fmt.Printf("[node %d, term %d, state %v]: sending heartbeats as the leader\n", rf.me, rf.currentTerm, rf.currentState)
			rf.sendHeartbeats()
		}
	}
}

// request vote from all peers
func (rf *Raft) requestVotes() int {
	var votesReceived int32 = 0

	wg := sync.WaitGroup{}

	for idx := range rf.peers {
		if idx != rf.me {
			wg.Add(1)
			
			go func (server int)  {
				defer wg.Done()

				args := &RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: len(rf.log)-1,
					LastLogTerm: rf.getLastLogTerm(),
				}
				reply := &RequestVoteReply{}
	
				fmt.Printf("[node %d, term %d, state %v]: requesting vote from node %d\n", rf.me, rf.currentTerm, rf.currentState, server)
				if rf.sendRequestVote(server, args, reply) {
					if reply.VoteGranted {
						fmt.Printf("[node %d, term %d, state %v]: received vote from node %d\n", rf.me, rf.currentTerm, rf.currentState, server)
						atomic.AddInt32(&votesReceived, 1)
					}
					if reply.Term > rf.currentTerm {
						// if a node has higher term than me, update my own term and transition to follower
						fmt.Printf("[node %d, term %d, state %v]: node %d has higher term (%d) than me, transitioning to follower\n", rf.me, rf.currentTerm, rf.currentState, server, reply.Term)
						rf.currentTerm = reply.Term
						rf.currentState = Follower
					}
				}
			}(idx)
		}
	}

	// wait for all RPCs to complete
	wg.Wait()

	return int(votesReceived)
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// get the last log term for a node
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}

	return rf.log[len(rf.log) - 1].Term
}

// get log term for a given index for a node
func (rf *Raft) getLogTermAtIndex(idx int) int {
	if idx >= len(rf.log) || idx == -1 {
		// idx exceeds length of log, return -1
		return -1
	}

	return rf.log[idx].Term
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		currentTerm: 0,
		candidateVotedFor: -1,
		log: make([]*LogEntry, 0),
		currentState: Follower,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

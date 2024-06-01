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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
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

type state int

const (
	follower state = iota
	candidate
	leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.RWMutex        // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	applyCh       chan ApplyMsg
	state         state
	lastHeartbeat time.Time

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	log          []LogEntry
	snapshotIdx  int
	snapshotTerm int
	currentTerm  int
	votedFor     int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3A).
	rf.mu.RLock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.RUnlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Encode the relevant state
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIdx)
	e.Encode(rf.snapshotTerm)

	// Save the encoded state to persister
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotIdx int
	var snapshotTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIdx) != nil ||
		d.Decode(&snapshotTerm) != nil {
		Debug(rf, "Error decoding state")
		return
	}

	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.snapshotIdx = snapshotIdx
	rf.snapshotTerm = snapshotTerm
	rf.mu.Unlock()

	Debug(rf, "load persist, T%v Vote%v LogLen%v SSIdx%v SSTerm%v", rf.currentTerm, rf.votedFor, len(rf.log), rf.snapshotIdx, rf.snapshotTerm)
}

func (rf *Raft) getLastLogIndex() int {
	return rf.snapshotIdx + len(rf.log)
}

func (rf *Raft) getLogByIndex(index int) *LogEntry {
	if index <= rf.snapshotIdx {
		Debug(rf, "getLogByIndex nil Idx%v ssIdx%v", index, rf.snapshotIdx)
		return nil
	} else if index > rf.getLastLogIndex() {
		Debug(rf, "getLogByIndex nil Idx%v logLen%v", index, len(rf.log))
		return nil
	}
	return &rf.log[index-rf.snapshotIdx-1]
}

func (rf *Raft) getTermByIndex(index int) int {
	if index == rf.snapshotIdx {
		return rf.snapshotTerm
	}
	log := rf.getLogByIndex(index)
	if log == nil {
		Debug(rf, "getTermByIndex nil Idx%v", index)
		return -1
	}
	return rf.log[index-rf.snapshotIdx-1].Term
}

func (rf *Raft) getFirstLogIndex(index int) int {
	curLog := rf.getLogByIndex(index)
	if curLog == nil {
		Debug(rf, "getFirstLogIndex nil Idx%v", index)
		return rf.snapshotIdx
	}
	for {
		prevLog := rf.getLogByIndex(index - 1)
		if prevLog == nil || prevLog.Term != curLog.Term {
			break
		}
		index--
	}
	return index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	log := rf.getLogByIndex(index)
	if log == nil {
		Debug(rf, "snapshot fail Idx%v logIdx %vTo%v", index, rf.snapshotIdx+1, rf.getLastLogIndex())
		rf.mu.Unlock()
		return
	}
	rf.snapshotTerm = rf.getTermByIndex(index)
	rf.log = rf.log[index-rf.snapshotIdx:]
	rf.snapshotIdx = index
	rf.persist()
	rf.mu.Unlock()

	Debug(rf, "snapshot succ, ssIdx%v ssTerm%v", index, rf.snapshotTerm)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
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
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.turnToFollower(args.Term)
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getTermByIndex(lastLogIndex)
	isValidVoted := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	isNewerTerm := args.LastLogTerm > lastLogTerm
	isSameTermLongerLog := args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex
	Debug(rf, "Cmp Res %v %v %v", isValidVoted, isNewerTerm, isSameTermLongerLog)

	if isValidVoted && (isNewerTerm || isSameTermLongerLog) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	Debug(rf, "vote for R%v T%v, RET T%v %v", args.CandidateId, args.Term, reply.Term, reply.VoteGranted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	// Debug(rf, "apppend from T%v R%v logLen%v preIdx%v preTerm%v", args.Term, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || rf.state == candidate {
		rf.turnToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex < rf.snapshotIdx {
		diff := rf.snapshotIdx - args.PrevLogIndex
		args.PrevLogIndex = rf.snapshotIdx
		args.PrevLogTerm = rf.snapshotTerm
		args.Entries = args.Entries[diff:]
	}

	if args.PrevLogIndex > rf.getLastLogIndex() ||
		rf.getTermByIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.XTerm = -1
			reply.XIndex = -1
		} else {
			reply.XTerm = rf.getTermByIndex(args.PrevLogIndex)
			reply.XIndex = args.PrevLogIndex
			for reply.XIndex-1 > rf.snapshotIdx && rf.getTermByIndex(reply.XIndex-1) == reply.XTerm {
				reply.XIndex--
			}
		}
		reply.XLen = rf.getLastLogIndex() + 1
		return
	}

	Debug(rf, "append entries from R%v T%v, len %vTo%v", args.LeaderId, args.Term, rf.getLastLogIndex()+1, args.PrevLogIndex+len(args.Entries)+1)
	rf.log = append(rf.log[:args.PrevLogIndex-rf.snapshotIdx], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		go rf.applyCommittedEntries()
	}
}

func (rf *Raft) applyCommittedEntries() {
	var applyMsgs []ApplyMsg

	rf.mu.RLock()
	Debug(rf, "apply committed %vTo%v", rf.lastApplied, rf.commitIndex)
	for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastLogIndex() {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogByIndex(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		applyMsgs = append(applyMsgs, applyMsg)
	}
	rf.mu.RUnlock()

	for _, msg := range applyMsgs {
		rf.applyCh <- msg
	}
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
	// Debug(rf, "request vote R%v T%v, RET T%v %v", server, args.Term, reply.Term, reply.VoteGranted)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == leader)

	// Your code here (3B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		index = rf.getLastLogIndex()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()

		Debug(rf, "command %v, SlotIdx%v T%v Leader%v", command, index, term, isLeader)
	}

	return index, term, isLeader
}

func (rf *Raft) updateCommitIndex() {
	// sort matchIndex
	Debug(rf, "matchIdx%v", rf.matchIndex)

	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)

	// find the majority matchIndex
	majorityIndex := matchIndex[len(rf.peers)/2]

	if majorityIndex > rf.commitIndex &&
		rf.getLastLogIndex() >= majorityIndex && rf.getTermByIndex(majorityIndex) == rf.currentTerm {
		Debug(rf, "update commitIdx %vTo%v", rf.commitIndex, majorityIndex)
		rf.commitIndex = majorityIndex
		go rf.applyCommittedEntries()
	}
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

func (rf *Raft) turnToFollower(term int) {
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.persist()
	Debug(rf, "become follower")
}

func (rf *Raft) turnToCandidate() {
	// Debug(rf, "become candidate")

	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.persist()
}

func (rf *Raft) turnToLeader() {
	rf.state = leader
	rf.lastHeartbeat = time.Now()
	rf.initLeaderState()
	go rf.heartbeat()
	Debug(rf, "========become leader========")
}

func (rf *Raft) initLeaderState() {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.turnToCandidate()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getTermByIndex(rf.getLastLogIndex()),
	}
	Debug(rf, "start election")
	rf.mu.Unlock()

	votes := 1
	voteCh := make(chan *RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rsp := &RequestVoteReply{}
			rf.sendRequestVote(index, req, rsp)
			voteCh <- rsp
		}(i)
	}

	for {
		select {
		case rsp := <-voteCh:
			rf.mu.Lock()
			if rf.state != candidate || rf.currentTerm != req.Term {
				rf.mu.Unlock()
				return
			}
			if rsp.Term > rf.currentTerm {
				rf.turnToFollower(rsp.Term)
				rf.mu.Unlock()
				return
			} else if rsp.VoteGranted {
				votes++
			}

			if votes > len(rf.peers)/2 {
				rf.turnToLeader()
				rf.mu.Unlock()
				Debug(rf, "election finish, %v/%v", votes, len(rf.peers))
				return
			}
			rf.mu.Unlock()

		case <-time.After(10 * time.Millisecond):
			rf.mu.RLock()
			if rf.currentTerm != req.Term {
				rf.mu.RUnlock()
				return
			}
			rf.mu.RUnlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		args := make([]*AppendEntriesArgs, len(rf.peers))

		rf.mu.RLock()
		if rf.state != leader {
			rf.mu.RUnlock()
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			args[i] = &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
		}
		rf.mu.RUnlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendWithRetry(i, args[i])
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendWithRetry(server int, args *AppendEntriesArgs) {
	for {
		rf.mu.RLock()
		if rf.state != leader || rf.currentTerm != args.Term {
			rf.mu.RUnlock()
			return
		}
		if rf.nextIndex[server] <= rf.snapshotIdx {
			// TODO: installSnapshot
			rf.nextIndex[server] = rf.snapshotIdx + 1
			args.PrevLogIndex = rf.snapshotIdx
			args.PrevLogTerm = rf.snapshotTerm
		} else {
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.getTermByIndex(args.PrevLogIndex)
		}
		args.Entries = append([]LogEntry{}, rf.log[args.PrevLogIndex-rf.snapshotIdx:]...)
		args.LeaderCommit = rf.commitIndex
		rf.mu.RUnlock()

		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(server, args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.turnToFollower(reply.Term)
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				Debug(rf, "append succ, server%v nextIdx%v matchIdx%v", server, rf.nextIndex[server], rf.matchIndex[server])
				rf.updateCommitIndex()
				rf.mu.Unlock()
				return
			} else {
				Debug(rf, "append fail, server%v XTerm%v XIdx%v XLen%v", server, reply.Term, reply.XIndex, reply.XLen)
				if reply.XTerm == -1 {
					// Follower's log is shorter than PrevLogIndex
					rf.nextIndex[server] = reply.XLen
				} else {
					// Follower has conflicting entry
					rf.nextIndex[server] = rf.getFirstLogIndex(reply.XIndex)
				}
			}
			rf.mu.Unlock()
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.RLock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.RUnlock()

		// Debug(rf, "ticker, state: %v", state)

		switch state {
		case follower:
			if time.Since(lastHeartbeat) > 300*time.Millisecond {
				go rf.election()
			}
		case candidate:
			go rf.election()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 250 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
		mu:        sync.RWMutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		state:     follower,
		applyCh:   applyCh,

		log:         make([]LogEntry, 1),
		snapshotIdx: -1,
		currentTerm: 0,
		votedFor:    -1,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}

	rf.initLeaderState()

	Debug(rf, "make raft")

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

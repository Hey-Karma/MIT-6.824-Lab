package raft

//LabD
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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 100
	ElectTimeOutBase = 450
	//检查是否超时的间隔
	ElectTimeOutCheckInterval = time.Duration(250) * time.Millisecond
	CommitCheckTimeInterval   = time.Duration(100) * time.Millisecond
)

type Entry struct {
	Term int
	Cmd  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int           // 将要提交的日志的最高索引
	lastApplied int           // 已经被应用到状态机的日志的最高索引
	nextIndex   []int         // 复制到某一个follower时, log开始的索引
	matchIndex  []int         // 已经被复制到follower的日志的最高索引
	applyCh     chan ApplyMsg // 用于在应用到状态机时传递消息
	// 记录心跳
	timeStamp time.Time
	// role: 一个枚举量, 记录当前实例的角色, 取值包括: Follower, Candidate, Leader
	role int
	// 保护投票数据
	muVote sync.Mutex
	// voteCount: 得票计数
	voteCount int

	condApply *sync.Cond
	rd        *rand.Rand
	timer     *time.Timer

	snapShot          []byte // 快照
	lastIncludedIndex int    // 快照中的最高索引
	lastIncludedTerm  int    // 快照中的最高Term

}

func (rf *Raft) ResetTimer() {
	rdTimeOut := GetRandomElectTimeOut(rf.rd)
	rf.timer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var votedFor, currentTerm int
	var log []Entry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode((&lastIncludedIndex)) != nil ||
		d.Decode((&lastIncludedTerm)) != nil {
		DPrintf("readPersist failed\n")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("server %v readPersist 成功 \n", rf.me)
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	// 目前只在Make中调用,因此不需要锁
	if len(data) == 0 {
		DPrintf("server %v 读取快照失败:无快照\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v 读取快照成功\n", rf.me)
}

func (rf *Raft) RealLogIdx(vIdx int) int {
	// 调用该函数需要是加锁的状态
	return vIdx - rf.lastIncludedIndex
}
func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 从节点
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		DPrintf("server %v 拒绝了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	DPrintf("server %v 同意了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, 原来的lastIncludedIndex=%v, 快照后的lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)

	rf.snapShot = snapshot

	rf.lastIncludedTerm = rf.log[rf.RealLogIdx(index)].Term
	// 截断log
	rf.log = rf.log[rf.RealLogIdx(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // Follower中与Leader冲突的Log对应的Term
	XIndex  int  // Follower中，对应Term为XTerm的第一条Log条目的索引
	XLen    int  // Follower的log的长度
}

func (rf *Raft) handleAppendEntries(serverTo int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(serverTo, args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 在函数调用间隙Term值发生了改变
	if args.Term != rf.currentTerm {
		return
	}
	// 2B
	if reply.Success {
		// server 回复成功
		rf.matchIndex[serverTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		// 需要判断是否可以commit
		N := rf.VirtualLogIdx(len(rf.log) - 1)
		for N > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N /*&& rf.log[rf.RealLogIdx(N)].Term == rf.currentTerm 需要吗?存疑!*/ {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
			N -= 1
		}
		rf.commitIndex = N
		rf.condApply.Signal()
		return
	}
	// reply.Success 已经是 false 了
	if reply.Term > rf.currentTerm {
		DPrintf("server %v 旧的leader收到了心跳函数中更新的term: %v,转化为Follower\n", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		//rf.timeStamp = time.Now()

		rf.role = Follower
		rf.ResetTimer()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		// Term 仍然相同,且自己还是Leader,表明对应的Follwer在prevLogIndex位置与prevLogTerm 不匹配
		// 快速回退
		if reply.XTerm == -1 {
			// PrevLogIndex这个位置在Follwer中不存在
			DPrintf("leader %v 收到 server %v 的回退请求, 原因是log过短, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, serverTo, rf.nextIndex[serverTo], serverTo, reply.XLen)
			if rf.lastIncludedIndex >= reply.XLen {
				// 由于snapshot被截断
				go rf.handleInstallSnapshot(serverTo)
			} else {
				rf.nextIndex[serverTo] = reply.XLen
			}
			return
		}

		i := rf.nextIndex[serverTo] - 1
		// 回退到小于等于reply的Xterm的地方
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			i -= 1
		}
		if i == rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			go rf.handleInstallSnapshot(serverTo)
		} else if rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			// 之前PrevLogIndex发生冲突的位置，Follower也有那个Term
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的最后一个XTerm索引为%v, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, i, serverTo, rf.nextIndex[serverTo], serverTo, i+1)

		}
		return
	}

}

func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 心跳接收方
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//rf.mu.Unlock()
		reply.Success = false
		return
	}
	// 代码执行到这里代表 args.Term >= tf.currentTerm

	//rf.timeStamp = time.Now()
	rf.ResetTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	if len(args.Entries) == 0 {
		// 心跳函数
		DPrintf("server %v 接收到 leader &%v 的心跳\n", rf.me, args.LeaderId)
	} else {
		DPrintf("server %v 受到 leader %v 的AppendEntries: %+v \n", rf.me, args.LeaderId, args)
	}

	isConflict := false
	if args.PrevLogIndex >= rf.VirtualLogIdx(len(rf.log)) {
		// PrevLogIndex位置不存在日志项
		reply.XTerm = -1
		reply.XLen = rf.VirtualLogIdx(len(rf.log))
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置不存在日志项, Log长度为%v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// PrevLogIndex位置的日志项存在,但Term不匹配
		reply.XTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置Term不匹配, args.Term=%v, 实际的term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
	}
	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 检验的是当前的及之后的,可能的情况是args.Term已经大于rf.currentTerm,即之前的任期内有未commit的部分
	if len(args.Entries) != 0 && rf.VirtualLogIdx(len(rf.log)) > args.PrevLogIndex+1 {
		rf.log = rf.log[:rf.RealLogIdx(args.PrevLogIndex+1)]
	}
	// 将收到的entries放进自己的log中
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行append\n", rf.me)
	}
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.VirtualLogIdx(len(rf.log)-1) {
			rf.commitIndex = rf.VirtualLogIdx(len(rf.log) - 1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.condApply.Signal()
	}
	//rf.mu.Unlock()
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
// 是各个非选举者的服务器来调用,即rf为非Candidate节点
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
// 非Candidate节点
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票:args.Term,rf.currentTerm:%v,%v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// 代码到这里时,args.Term >= rf.currentTerm

	if args.Term > rf.currentTerm {
		// 已经是新的一轮Term,之前的投票记录作废
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.VirtualLogIdx(len(rf.log)-1)) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			// 这里是投票服务器的*Raft
			rf.role = Follower
			rf.ResetTimer()
			//rf.timeStamp = time.Now()
			rf.persist()

			//rf.mu.Unlock()
			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v 投票\n\t args = %v\n", rf.me, args.CandidateId, args)
			return
		}
	} else {
		DPrintf("server(%v) %v 拒绝向 server %v 投票: 已投票给%v \n\t args = %v\n", rf.role, rf.me, args.CandidateId, rf.votedFor, args)
	}

	reply.Term = rf.currentTerm
	//rf.mu.Unlock()
	reply.VoteGranted = false
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
	if rf.role != Leader {
		return -1, -1, false
	}
	newEntry := &Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, *newEntry)

	rf.persist()
	return rf.VirtualLogIdx(len(rf.log) - 1), rf.currentTerm, true

	// Your code here (2B).
}

func (rf *Raft) CommitChecker() {
	// 检查是否有新的commit
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *msg
			DPrintf("server %v 准备将命令 %v(索引为 %v) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
		//time.Sleep(CommitCheckTimeInterval)
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

func (rf *Raft) ticker() {
	//rd := rand.New(rand.NewSource(int64(rf.me)))
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// rdTimeOut := GetRandomElectTimeOut(rd)
		// DPrintf("rf.me(%v) 的超时时间为:%v", rf.me, rdTimeOut)
		// rf.mu.Lock()
		// if rf.role != Leader && time.Since(rf.timeStamp) > time.Duration(rdTimeOut)*time.Millisecond {
		// 	go rf.Elect()
		// }
		// rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		// time.Sleep(ElectTimeOutCheckInterval)
		<-rf.timer.C
		rf.mu.Lock()
		if rf.role != Leader {
			go rf.Elect()
		}
		rf.ResetTimer()
		rf.mu.Unlock()
	}
}

func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.currentTerm += 1       // 自增term
	rf.role = Candidate       // 成为候选人
	rf.votedFor = rf.me       // 给自己投票
	rf.voteCount = 1          // 自己有一票
	rf.timeStamp = time.Now() // 自己给自己投票也算一种消息

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.VirtualLogIdx(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args)
	}
}

// collectVote函数处理每个server的回复并统计票数
func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}
	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role == Follower {
			// 另外一个投票的协程收到了更新的term而更改了自身状态为Follower
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}
		rf.role = Leader
		DPrintf("server %v 当选为Leader,Term为: %v \n", rf.me, rf.currentTerm)
		// 需要重新初始化nextIndex和matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}
	rf.muVote.Unlock()
}

func (rf *Raft) SendHeartBeats() {
	// 2B相对2A的变化, 真实的AppendEntries也通过心跳发送
	DPrintf("leader %v 开始发送心跳\n", rf.me)

	for !rf.killed() {
		rf.mu.Lock()
		// if the server is dead or is not the leader, just return
		if rf.role != Leader {
			rf.mu.Unlock()
			// 不是leader则终止心跳的发送
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				//PrevLogTerm:  rf.log[rf.nextIndex[i]].Term,
				LeaderCommit: rf.commitIndex,
			}

			sendInstallSnapshot := false

			if args.PrevLogIndex < rf.lastIncludedIndex {
				// 表示Follower有落后的部分且被截断，改为发送同步心跳
				DPrintf("leader %v 取消向 server %v 广播新的心跳, 改为发送sendInstallSnapshot, lastIncludedIndex=%v, nextIndex[%v]=%v, args = %+v \n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args)
				sendInstallSnapshot = true
			} else if rf.VirtualLogIdx(len(rf.log)-1) > args.PrevLogIndex {
				// args中的PrevLogIndex代表的是peers的PrevLogIndex
				// 有新的log需要发送，是AppendEntries
				args.Entries = rf.log[rf.RealLogIdx(args.PrevLogIndex+1):]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries, lastIncludedIndex=%v, nextIndex[%v]=%v, args = %+v\n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args)

			} else {
				// 心跳
				DPrintf("leader %v 开始向 server %v 广播新的心跳, lastIncludedIndex=%v, nextIndex[%v]=%v, args = %+v \n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args)
				args.Entries = make([]Entry, 0)
			}

			if sendInstallSnapshot {
				go rf.handleInstallSnapshot(i)
			} else {
				args.PrevLogTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
				go rf.handleAppendEntries(i, args)
			}
		}

		rf.mu.Unlock()

		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 易错点:函数调用的间隙被修改了
	if sendArgs.Term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

type InstallSnapshotArgs struct {
	Term              int         // Leader的Term
	LeaderId          int         // 便于将客户端重定向给Leader
	lastIncludedIndex int         // 将此前的entries全部替换掉
	lastIncludedTerm  int         // lastIncludedIndex指向的快照的Term
	Data              []byte      // 快照
	LastIncludedCmd   interface{} // 用于在0处占位
}

type InstallSnapshotReply struct {
	Term int // currentTerm，让Leader更新自己
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
// 用来初始化各个服务器
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("server %v 调用Make启动", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// rf.timeStamp = time.Now()
	rf.role = Follower
	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))
	rf.timer = time.NewTimer(0)
	rf.ResetTimer()

	// initialize from state persisted before a crash
	// 如果读取成功, 将覆盖log, votedFor和currentTerm
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log)) // raft中的index是从1开始的
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CommitChecker()

	return rf
}

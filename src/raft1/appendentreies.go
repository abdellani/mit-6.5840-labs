package raft

import "time"

type AppendEntryArg struct {
	Term     int
	LeaderId int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArg, reply *AppendEntryReply) {
	rf.Log("hb <= %d", args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return
	}

	// rf.CurrentTerm <= args.Term
	rf._becomeFollower(args.Term)
	rf._resetElectionsTimeout()
	reply.Term = rf.CurrentTerm

}

func (rf *Raft) HeartbeatLoop(term int) {
	const hbInterval = 100 * time.Millisecond
	const failBackoff = 10 * time.Millisecond
	rf.Log("starting HB")
	cancel := make(chan struct{})
	followerReplicator := func(server int) {
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()
		defer func(server int) { rf.Log("killing hb for %d", server) }(server)
		for !rf.killed() {
			select {
			case <-cancel:
				return
			case <-ticker.C:
				rf.mu.Lock()
				if !rf._isLeader() {
					rf.mu.Unlock()
					return
				}
				if rf.CurrentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.Log("hb => %d", server)

				args := AppendEntryArg{
					Term:     term,
					LeaderId: rf.me,
				}
				reply := AppendEntryReply{}

				ok := rf.sendAppendEntry(server, &args, &reply)
				rf.Log("hb => %d (ok? %v)", server, ok)

				if rf.killed() {
					return
				}
				if !ok {
					time.Sleep(failBackoff)
					continue
				}

				rf.mu.Lock()
				if !rf._isLeader() {
					rf.mu.Unlock()
					return
				}
				if rf.CurrentTerm < reply.Term {
					rf._becomeFollower(reply.Term)
					rf._resetElectionsTimeout()
					rf.mu.Unlock()
					rf.Log("step down at t= %d", reply.Term)
					return
				}
				if rf.CurrentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go followerReplicator(i)
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArg, reply *AppendEntryReply) bool {
	done := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
		done <- ok
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(120 * time.Millisecond):
		return false
	}
	// return rf.peers[server].Call("Raft.AppendEntry", args, reply)
}

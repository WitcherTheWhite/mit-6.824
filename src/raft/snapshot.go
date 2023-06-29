package raft

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index >= rf.log.lastIndex() {
		return
	}
	tmpSlice := rf.log.getSliceFrom(index + 1)
	entries := make([]Entry, len(tmpSlice)+1)
	copy(entries, tmpSlice)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log.getTermOfIndex(index)
	rf.log.Entries = entries
	rf.log.StartIndex = index + 1
}

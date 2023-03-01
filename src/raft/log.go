package raft

//
// 每个日志条目包含给状态机的指令，该条目被leader接受时的term
//
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// 初始化log
//
func makeEmptyLog() Log {
	log := Log{make([]LogEntry, 1)}
	return log
}

//
// 日志
//
type Log struct {
	Entries []LogEntry
}

func (log *Log) lastIndex() int {
	return len(log.Entries) - 1
}

func (log *Log) append(entry LogEntry) {
	log.Entries = append(log.Entries, entry)
}

func (log *Log) getTermOfIndex(index int) int {
	return log.Entries[index].Term
}

func (log *Log) getLastLogTerm() int {
	return log.Entries[log.lastIndex()].Term
}

func (log *Log) getSliceFrom(index int) []LogEntry {
	return log.Entries[index:]
}

func (log *Log) getSliceTo(index int) []LogEntry {
	return log.Entries[:index+1]
}

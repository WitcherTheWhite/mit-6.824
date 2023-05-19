package raft

// Entry LogEntry
// 每个日志条目包含给状态机的指令，该条目被leader接受时的term
type Entry struct {
	Command interface{}
	Term    int
}

// 初始化log
func makeEmptyLog() *Log {
	log := Log{
		Entries:    make([]Entry, 1),
		StartIndex: 0,
	}
	return &log
}

// Log 日志
type Log struct {
	Entries    []Entry
	StartIndex int
}

func (l *Log) length() int {
	return len(l.Entries)
}

func (l *Log) lastIndex() int {
	return l.StartIndex + l.length() - 1
}

func (l *Log) append(entry Entry) {
	l.Entries = append(l.Entries, entry)
}

func (l *Log) getIndexAtEntries(index int) int {
	return index - l.StartIndex
}

func (l *Log) getTermOfIndex(index int) int {
	return l.Entries[l.getIndexAtEntries(index)].Term
}

func (l *Log) getLastLogTerm() int {
	return l.Entries[l.length()-1].Term
}

func (l *Log) getSliceFrom(index int) []Entry {
	start := l.getIndexAtEntries(index)
	return l.Entries[start:]
}

func (l *Log) getSliceTo(index int) []Entry {
	end := l.getIndexAtEntries(index)
	return l.Entries[:end+1]
}

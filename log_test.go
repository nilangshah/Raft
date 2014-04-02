package Raft

import (
	"os"
	"testing"
)

func newLogItem(index uint64, term uint64, command []byte, resp chan bool) *LogItem {
	entry := &LogItem{
		Index:     index,
		Term:      term,
		Command:   command,
		committed: resp,
	}
	return entry
}

func TestLog_1(t *testing.T) {
	path := GetPath() + "/src/github.com/nilangshah/Raft/tempDb"
	err := os.Mkdir(path, 0766)
	if err != nil {
		panic("unable to creat temp directory")
	} else {
		defer os.RemoveAll(path)
	}

	log := newRaftLog(path)
	log.ApplyFunc = func(e *LogItem) {
		return
	}
	e := newLogItem(1, 1, []byte("log test entry 1"), nil)
	if err := log.appendEntry(e); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	e = newLogItem(2, 1, []byte("log test entry 2"), nil)
	if err := log.appendEntry(e); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	e = newLogItem(3, 2, []byte("log test entry 3"), nil)
	if err := log.appendEntry(e); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}

	if err := log.commitTo(2); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}
	if index, term := log.commitInfo(); index != 2 || term != 1 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	if err := log.commitTo(3); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}
	if index, term := log.commitInfo(); index != 3 || term != 2 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

}

func TestLogContainsEntries(t *testing.T) {
	path := GetPath() + "/src/github.com/nilangshah/Raft/tempDb"
	err := os.Mkdir(path, 0777)
	if err != nil {
		panic("unable to creat temp directory")
	} else {
		defer os.RemoveAll(path)
	}

	log := newRaftLog(path)
	log.ApplyFunc = func(e *LogItem) {
		return
	}
	//response := make(chan bool)
	e := newLogItem(1, 1, []byte("log test entry 1"), nil)
	if err := log.appendEntry(e); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	e = newLogItem(2, 1, []byte("log test entry 2"), nil)
	if err := log.appendEntry(e); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	e = newLogItem(3, 2, []byte("log test entry 3"), nil)
	if err := log.appendEntry(e); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}

	if log.containsEntry(0, 0) {
		t.Fatalf("Zero-index entry should not exist in log.")
	}
	if log.containsEntry(1, 0) {
		t.Fatalf("Entry with mismatched term should not exist")
	}
	if log.containsEntry(4, 0) {
		t.Fatalf("Out-of-range entry should not exist")
	}
	if !log.containsEntry(2, 1) {
		t.Fatalf("Entry 2/1 should exist")
	}
	if !log.containsEntry(3, 2) {
		t.Fatalf("Entry 2/1 should exist")
	}
}

//--------------------------------------
// Append
//--------------------------------------

// Ensure that we can truncate uncommitted entries in the log.
func TestLogTruncate(t *testing.T) {
	path := GetPath() + "/src/github.com/nilangshah/Raft/tempDb"
	err := os.Mkdir(path, 0777)
	if err != nil {
		panic("unable to creat temp directory")
	} else {
		defer os.RemoveAll(path)
	}

	log := newRaftLog(path)
	log.ApplyFunc = func(e *LogItem) {
		return
	}

	e1 := newLogItem(1, 1, []byte("log test entry 1"), nil)
	if err := log.appendEntry(e1); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	e2 := newLogItem(2, 1, []byte("log test entry 2"), nil)
	if err := log.appendEntry(e2); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	e3 := newLogItem(3, 2, []byte("log test entry 3"), nil)
	if err := log.appendEntry(e3); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}

	if err := log.commitTo(2); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}

	// Truncate committed entry.
	if err := log.discard(1, 1); err == nil {
		t.Fatalf("Truncating committed entries shouldn't work: %v", err)
	}
	// Truncate past end of log.
	if err := log.discard(4, 2); err == nil {
		t.Fatalf("Truncating past end-of-log shouldn't work: %v", err)
	}
	// Truncate entry with mismatched term.
	if err := log.discard(2, 2); err == nil {
		t.Fatalf("Truncating mismatched entries shouldn't work: %v", err)
	}
	// Truncate end of log.
	if err := log.discard(3, 2); err != nil {
		t.Fatalf("Truncating end of log should work")
	}
	// Truncate at last commit.
	if err := log.discard(2, 1); err != nil {
		t.Fatalf("Truncating at last commit should work")
	}

	// Append after truncate
	if err := log.appendEntry(e3); err != nil {
		t.Fatalf("Unable to append after truncate: %v", err)
	}

	log.close()

	// Recovery the truncated log
	log = newRaftLog(path)

	// Validate existing log entries.
	if len(log.entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(log.entries))
	}
	if log.entries[0].Index != 1 || log.entries[0].Term != 1 {
		t.Fatalf("Unexpected entry[0]: %v", log.entries[0])
	}
	if log.entries[1].Index != 2 || log.entries[1].Term != 1 {
		t.Fatalf("Unexpected entry[1]: %v", log.entries[1])
	}
	if log.entries[2].Index != 3 || log.entries[2].Term != 2 {
		t.Fatalf("Unexpected entry[2]: %v", log.entries[2])
	}
}

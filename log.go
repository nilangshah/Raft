package Raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

var (
	errNoCommand       = errors.New("no command")
	errWrongIndex      = errors.New("bad index")
	errWrongTerm       = errors.New("bad term")
	errTermIsSmall     = errors.New("term is too small")
	errIndexIsSmall    = errors.New("index is too small")
	errIndexIsBig      = errors.New("commit index is too big")
	errChecksumInvalid = errors.New("checksum invalid")
)

type raftLog struct {
	sync.RWMutex
	ApplyFunc   func(*LogItem)
	db          *leveldb.DB
	entries     []*LogItem
	commitIndex uint64
	initialTerm uint64
}

func newRaftLog(dbPath string) *raftLog {

	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		panic(fmt.Sprintf("dir not exist,%v", err))
	}
	l := &raftLog{
		entries:     []*LogItem{},
		db:          db,
		commitIndex: 0,
		initialTerm: 0,
	}
	l.readFirst()
	return l
}

func (l *raftLog) currentIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.CurrentIndexWithOutLock()
}

// The current index in the log without locking
func (l *raftLog) CurrentIndexWithOutLock() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Closes the log file.
func (l *raftLog) close() {
	l.Lock()
	defer l.Unlock()

	l.db.Close()
	l.entries = make([]*LogItem, 0)
}

func (l *raftLog) containsEntry(index uint64, term uint64) bool {
	entry := l.getEntry(index)
	return (entry != nil && entry.Term == term)
}

func (l *raftLog) getEntry(index uint64) *LogItem {
	l.RLock()
	defer l.RUnlock()

	if index <= 0 || index > (uint64(len(l.entries))) {
		return nil
	}
	return l.entries[index-1]
}

func (l *raftLog) readFirst() error {

	iter := l.db.NewIterator(nil, nil)
	for iter.Next() {
		entry := new(LogItem)
		value := iter.Value()
		b := bytes.NewBufferString(string(value))
		dec := gob.NewDecoder(b)

		err := dec.Decode(entry)
		if err != nil {
			panic(fmt.Sprintf("decode:", err))
		}

		if entry.Index > 0 {
			// Append entry.
			fmt.Println(entry.Command)
			l.entries = append(l.entries, entry)
			if entry.Index <= l.commitIndex {
				//command, err := newCommand(entry.CommandName(), entry.Command())
				//if err != nil {
				//	continue
				//}
				l.ApplyFunc(entry)
			}
		}

	}
	iter.Release()
	err := iter.Error()
	return err
}

func (l *raftLog) entriesAfter(index uint64) ([]*LogItem, uint64) {
	l.RLock()
	defer l.RUnlock()

	if index < 0 {
		fmt.Println("log.entriesAfter.before: ", index)
		return nil, 0
	}
	if index > (uint64(len(l.entries))) {
		panic(fmt.Sprintf("raft: Index is beyond end of log: %v %v", len(l.entries), index))
	}

	pos := 0
	lastTerm := uint64(0)
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index > index {
			break
		}
		lastTerm = l.entries[pos].Term
	}

	a := l.entries[pos:]
	if len(a) == 0 {
		return []*LogItem{}, lastTerm
	}

	return closeResponseChannels(a), lastTerm
}

func closeResponseChannels(a []*LogItem) []*LogItem {
	stripped := make([]*LogItem, len(a))
	for i, entry := range a {
		stripped[i] = &LogItem{
			Index:     entry.Index,
			Term:      entry.Term,
			Command:   entry.Command,
			committed: nil,
		}
	}
	return stripped
}

func (l *raftLog) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTermWithOutLock()
}

func (l *raftLog) lastTermWithOutLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term

}

func (l *raftLog) discard(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	if index > l.lastIndexWithOutLock() {
		return errIndexIsBig
	}
	if index < l.getCommitIndexWithOutLock() {
		return errIndexIsSmall
	}

	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {

			if l.entries[pos].committed != nil {
				l.entries[pos].committed <- false
				close(l.entries[pos].committed)
				l.entries[pos].committed = nil
			}
		}
		l.entries = []*LogItem{}
		return nil
	} else {
		// Do not truncate if the entry at index does not have the matching term.
		entry := l.entries[index-1]
		if len(l.entries) > 0 && entry.Term != term {
			return errors.New(fmt.Sprintf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term))
		}

		// Otherwise truncate up to the desired entry.
		if index < uint64(len(l.entries)) {
			buf := make([]byte, 8)

			// notify clients if this node is the previous leader
			for i := index; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				binary.LittleEndian.PutUint64(buf, entry.Index)

				err := l.db.Delete(buf, nil)
				if err != nil {
					panic("entry not exist")
				}

				if entry.committed != nil {
					entry.committed <- false
					close(entry.committed)
					entry.committed = nil
				}
			}

			l.entries = l.entries[0:index]
		}
	}

	return nil
}

func (l *raftLog) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.getCommitIndexWithOutLock()
}

func (l *raftLog) getCommitIndexWithOutLock() uint64 {
	return l.commitIndex

}

func (l *raftLog) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithOutLock()
}

func (l *raftLog) lastIndexWithOutLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index

}

// Appends a series of entries to the log.
func (l *raftLog) appendEntries(entries []*LogItem) error {
	l.Lock()
	defer l.Unlock()

	// Append each entry but exit if we hit an error.
	for i := range entries {

		if err := entries[i].writeToDB(l.db); err != nil {
			return err
		} else {
			l.entries = append(l.entries, entries[i])
		}

	}

	return nil
}

func (l *raftLog) appendEntry(entry *LogItem) error {
	l.Lock()
	defer l.Unlock()

	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithOutLock()
		if entry.Term < lastTerm {
			fmt.Println(entry.Term, lastTerm)
			return errTermIsSmall
		}
		lastIndex := l.lastIndexWithOutLock()
		if entry.Term == lastTerm && entry.Index <= lastIndex {
			return errIndexIsSmall
		}
	}
	if err := entry.writeToDB(l.db); err != nil {
		return err
	}
	l.entries = append(l.entries, entry)

	return nil

}

func (l *raftLog) updateCommitIndex(index uint64) {
	l.Lock()
	defer l.Unlock()
	if index > l.commitIndex {
		l.commitIndex = index
	}

}

func (l *raftLog) commitTo(commitIndex uint64) error {
	l.Lock()
	defer l.Unlock()

	if commitIndex > uint64(len(l.entries)) {
		fmt.Printf("raft.Log: Commit index", commitIndex, "set back to \n", len(l.entries))
		commitIndex = uint64(len(l.entries))
	}
	if commitIndex < l.commitIndex {
		return nil
	}
	pos := l.commitIndex + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}
	for i := l.commitIndex + 1; i <= commitIndex; i++ {
		entryIndex := i - 1
		entry := l.entries[entryIndex]

		// Update commit index.
		l.commitIndex = entry.Index
		if entry.committed != nil {
			entry.committed <- true
			close(entry.committed)
			entry.committed = nil
		}
		// Decode the command.

		// Apply the changes to the state machine and store the error code.

	}

	return nil
}

func (l *raftLog) commitInfo() (index uint64, term uint64) {
	l.RLock()
	defer l.RUnlock()
	if l.commitIndex == 0 {
		return 0, 0
	}

	if l.commitIndex == 0 {
		return 0, 0
	}

	entry := l.entries[l.commitIndex-1]
	return entry.Index, entry.Term
}

func (e *LogItem) writeToDB(db *leveldb.DB) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(e)
	if err != nil {
		panic("gob error: " + err.Error())
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, e.Index)
	err = db.Put(buf, []byte(network.String()), nil)
	return err

}

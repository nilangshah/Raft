package Raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"bufio"
	//"log"
	"os"
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
	ApplyFunc    func(*LogItem) (interface{}, error)
	writer       io.Writer
	file         *os.File
	entries      []*LogItem
	commitIndex  uint64
	initialIndex uint64
	initialTerm  uint64
}

func newRaftLog(file1 *os.File) *raftLog {
	l := &raftLog{
		writer:       file1,
		entries:      []*LogItem{},
		file:         file1,
		commitIndex:  0,
		initialIndex: 0,
		initialTerm:  0,
	}
	l.readFirst(file1)
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
	return l.initialIndex
	}
	return l.entries[len(l.entries)-1].Index
}





// Closes the log file.
func (l *raftLog) close() {
	l.Lock()
	defer l.Unlock()

	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
	l.entries = make([]*LogItem, 0)
}

func (l *raftLog) readFirst(r io.Reader) error {
	var readBytes int64
	readBytes = 0
	for {
		
		entry :=new(LogItem)
		entry.Position, _ = l.file.Seek(0, os.SEEK_CUR)
		switch n, err := entry.readFromFile(r); err {
		case io.EOF:
				
			return nil
		case nil:
			
			if entry.Index > l.initialIndex {
				// Append entry.
				fmt.Println(entry.Command)
				l.entries = append(l.entries, entry)
				if entry.Index <= l.commitIndex {
					//command, err := newCommand(entry.CommandName(), entry.Command())
					//if err != nil {
					//	continue
					//}
					//l.ApplyFunc(entry, command)  function to apply
				}
			}

			
			readBytes += int64(n)

		default:
			if err = l.file.Truncate(readBytes); err != nil {
				return errors.New(fmt.Sprintf("raft.Log: Unable to recover: %v", err))
			}
			return err

		}
	}

	return nil
}

func (l *raftLog) entriesAfter(index uint64) ([]*LogItem, uint64) {
	l.RLock()
	defer l.RUnlock()

	if index < l.initialIndex {
		fmt.Println("log.entriesAfter.before: ", index, " ", l.initialIndex)
		return nil, 0
	}
	if index > (uint64(len(l.entries)) + l.initialIndex) {
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

func (e *LogItem) readFromFile(r io.Reader) (int, error) {
	header := make([]byte, 24)
	var n int
	if n, err := r.Read(header); err != nil {
		return n, err
	}

	command := make([]byte, binary.LittleEndian.Uint32(header[20:24]))

	if n, err := r.Read(command); err != nil {
		return n, err
	}

	crc := binary.LittleEndian.Uint32(header[:4])

	check := crc32.NewIEEE()
	check.Write(header[4:])
	check.Write(command)

	if crc != check.Sum32() {
		return 0, errChecksumInvalid
	}
	e.Term = binary.LittleEndian.Uint64(header[4:12])
	e.Index = binary.LittleEndian.Uint64(header[12:20])
	e.Command = command

	return n, nil
}
func (l *raftLog) discardUpto(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	if index > l.lastIndexWithOutLock() {
		return errIndexIsBig
	}
	if index < l.getCommitIndexWithOutLock() {
		return errIndexIsSmall
	}

	if index == 0 {
		//l.file.Truncate(0)
		//l.file.Seek(0, os.SEEK_SET)
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
		entry := l.entries[index-l.initialIndex-1]
		if len(l.entries) > 0 && entry.Term != term {
			//debugln("log.truncate.termMismatch")
			return errors.New(fmt.Sprintf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term))
		}

		// Otherwise truncate up to the desired entry.
		if index < l.initialIndex+uint64(len(l.entries)) {
			//debugln("log.truncate.finish")
			position := l.entries[index-l.initialIndex].Position
			l.file.Truncate(position)
			l.file.Seek(position, os.SEEK_SET)

			// notify clients if this node is the previous leader
			for i := index - l.initialIndex; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				if entry.committed != nil {
					entry.committed <- false
					close(entry.committed)
					entry.committed = nil
				}
			}

			l.entries = l.entries[0 : index-l.initialIndex]
		}
	}

	/*var pos uint64
	pos = 0
	for ; pos < uint64(len(l.entries)); pos++ {
		if l.entries[pos].Index < index {
			continue
		}
		if l.entries[pos].Index > index {
			return errWrongIndex
		}
		if l.entries[pos].Index != index {
			log.Println("not <, not >, but somehow !=")
		}
		if l.entries[pos].Term != term {
			return errWrongTerm
		}
		break
	}

	if pos < l.commitIndex {
		log.Println("index >= commitIndex, but pos < commitIndex")
	}

	truncateFrom := pos + 1
	if truncateFrom >= uint64(len(l.entries)) {
		return nil // nothing to truncate
	}

	for pos = truncateFrom; pos < uint64(len(l.entries)); pos++ {

		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- false
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}
	}

	l.entries = l.entries[:truncateFrom]
	*/
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

	startPosition, _ := l.file.Seek(0, os.SEEK_CUR)

	w := bufio.NewWriter(l.file)

	var size int64
	var err error
	// Append each entry but exit if we hit an error.
	for i := range entries {

		entries[i].Position = startPosition

		if size, err = entries[i].writeToFile(w); err != nil {
			return err
		}else{
			l.entries = append(l.entries, entries[i])		
		}

		startPosition += size
	}
	w.Flush()
	err = l.sync()

	if err != nil {
		panic(err)
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
	position, _ := l.file.Seek(0, os.SEEK_CUR)

	entry.Position = position

	// Write to storage.
	if _, err := entry.writeToFile(l.file); err != nil {
		return err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return nil

}

func (l *raftLog) sync() error {
	return l.file.Sync()
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

	if commitIndex > l.initialIndex+uint64(len(l.entries)) {
		fmt.Printf("raft.Log: Commit index", commitIndex, "set back to \n", len(l.entries))
		commitIndex = l.initialIndex + uint64(len(l.entries))
	}
	if commitIndex < l.commitIndex {
		return nil
	}
	pos := l.commitIndex + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}
	for i := l.commitIndex + 1; i <= commitIndex; i++ {
		entryIndex := i - 1 - l.initialIndex
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

func (e *LogItem) writeToFile(w io.Writer) (int64, error) {
	if len(e.Command) <= 0 {
		return 0,errNoCommand
	}
	if e.Index <= 0 {
		return 0,errWrongIndex
	}
	if e.Term <= 0 {
		return 0,errWrongTerm
	}

	commandSize := len(e.Command)
	buf := make([]byte, 24+commandSize)

	binary.LittleEndian.PutUint64(buf[4:12], e.Term)
	binary.LittleEndian.PutUint64(buf[12:20], e.Index)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(commandSize))

	copy(buf[24:], e.Command)

	binary.LittleEndian.PutUint32(
		buf[0:4],
		crc32.ChecksumIEEE(buf[4:]),
	)

	size, err := w.Write(buf)
	return int64(size), err
}

package Raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
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
	writer      io.Writer
	entries     []LogItem
	commitIndex int
}

func newRaftLog(writer io.ReadWriter) *raftLog {
	l := &raftLog{
		writer:      writer,
		entries:     []LogItem{},
		commitIndex: -1, // no commits to begin with
	}
	l.readFirst(writer)
	return l
}

func (l *raftLog) readFirst(r io.Reader) error {
	for {
		var entry LogItem
		switch err := entry.readFromFile(r); err {
		case io.EOF:
			return nil
		case nil:
			if err := l.appendEntry(entry); err != nil {
				return err
			}
			l.commitIndex++

		default:
			return err
		}
	}
	return nil
}

func (l *raftLog) entriesAfter(index uint64) ([]LogItem, uint64) {
	l.RLock()
	defer l.RUnlock()

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
		return []LogItem{}, lastTerm
	}

	return closeResponseChannels(a), lastTerm
}

func closeResponseChannels(a []LogItem) []LogItem {
	stripped := make([]LogItem, len(a))
	for i, entry := range a {
		stripped[i] = LogItem{
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
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}
func (e *LogItem) readFromFile(r io.Reader) error {
	header := make([]byte, 24)

	if _, err := r.Read(header); err != nil {
		return err
	}

	command := make([]byte, binary.LittleEndian.Uint32(header[20:24]))

	if _, err := r.Read(command); err != nil {
		return err
	}

	crc := binary.LittleEndian.Uint32(header[:4])

	check := crc32.NewIEEE()
	check.Write(header[4:])
	check.Write(command)

	if crc != check.Sum32() {
		return errChecksumInvalid
	}

	e.Term = binary.LittleEndian.Uint64(header[4:12])
	e.Index = binary.LittleEndian.Uint64(header[12:20])
	e.Command = command

	return nil
}
func (l *raftLog) discardUpto(index, term uint64) error {

	if index > l.lastIndex() {
		return errIndexIsBig
	}
	if index < l.getCommitIndex() {
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
		l.entries = []LogItem{}
		return nil
	}
	pos := 0
	for ; pos < len(l.entries); pos++ {
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
	if truncateFrom >= len(l.entries) {
		return nil // nothing to truncate
	}

	for pos = truncateFrom; pos < len(l.entries); pos++ {

		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- false
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}
	}

	l.entries = l.entries[:truncateFrom]

	return nil
}

func (l *raftLog) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	if l.commitIndex < 0 {
		return 0
	}

	if l.commitIndex >= len(l.entries) {
		log.Printf(fmt.Sprintf("commitIndex %d > len(l.entries) %d; bad bookkeeping in raftLog", l.commitIndex, len(l.entries)))
	}
	return l.entries[l.commitIndex].Index

}

func (l *raftLog) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) appendEntry(entry LogItem) error {
	if len(l.entries) > 0 {
		lastTerm := l.lastTerm()
		if entry.Term < lastTerm {
			fmt.Println(entry.Term, lastTerm)
			return errTermIsSmall
		}
		lastIndex := l.lastIndex()
		if entry.Term == lastTerm && entry.Index <= lastIndex {
			return errIndexIsSmall
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

func (l *raftLog) commitTo(commitIndex uint64) error {
	if commitIndex == 0 {
		log.Println("commitTo(0)")
	}

	if commitIndex < l.getCommitIndex() {
		return errIndexIsSmall
	}
	if commitIndex > l.lastIndex() {
		return errIndexIsBig
	}
	if commitIndex == l.getCommitIndex() {
		return nil
	}

	pos := l.commitIndex + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}

	for {
		if pos >= len(l.entries) {
			panic(fmt.Sprintf("commitTo pos=%d advanced past all log entries (%d)", pos, len(l.entries)))
		}
		if l.entries[pos].Index > commitIndex {
			panic("commitTo advanced past the desired commitIndex")
		}

		if err := l.entries[pos].writeToFile(l.writer); err != nil {
			return err
		}
		fmt.Println("commit too")
		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- true
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}

		l.commitIndex = pos

		if l.entries[pos].Index == commitIndex {
			break
		}
		if l.entries[pos].Index > commitIndex {
			log.Println(fmt.Sprintf(
				"current entry Index %d is beyond our desired commitIndex %d",
				l.entries[pos].Index,
				commitIndex,
			))
		}

		pos++
	}

	return nil
}

func (e *LogItem) writeToFile(w io.Writer) error {
	if len(e.Command) <= 0 {
		return errNoCommand
	}
	if e.Index <= 0 {
		return errWrongIndex
	}
	if e.Term <= 0 {
		return errWrongTerm
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

	_, err := w.Write(buf)
	return err
}

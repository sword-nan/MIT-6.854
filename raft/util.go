package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func encodeSnapshot(lastIncludedIndex int, commands []interface{}, lastIncludedTerm int) []byte {
	var w bytes.Buffer
	e := labgob.NewEncoder(&w)
	e.Encode(lastIncludedIndex)
	e.Encode(commands)
	e.Encode(lastIncludedTerm)
	return w.Bytes()
}

func decodeSnapshot(snapshot []byte) (lastIncludedIndex int, commands []interface{}, lastIncludedTerm int) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&commands) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("decodeSnapshot: decode failed")
	}
	// DPrintf("decode snapshot lastIncludedIndex: %d, commands: %v", lastIncludedIndex, commands)
	return lastIncludedIndex, commands, lastIncludedTerm
}

func decodeAppliedSnapshot(snapshot []byte) (lastIncludedIndex int, commands []interface{}) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&commands) != nil {
		log.Fatal("decodeSnapshot: decode failed")
	}
	return lastIncludedIndex, commands
}

func encodeRaftstate(term, voteFor int, log []LogEntry) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(term)
	e.Encode(voteFor)
	e.Encode(log)
	return w.Bytes()
}

func decodeRaftstate(data []byte) (int, int, []LogEntry) {
	var term, votedFor int
	var logs []LogEntry
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("readRaftState: decode failed")
	}
	return term, votedFor, logs
}

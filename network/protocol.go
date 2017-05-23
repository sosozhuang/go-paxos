package network

import (
	"github.com/sosozhuang/paxos/comm"
)

var (
	token    = "paxos"
	tokenLen = len(token)
)

func setToken(t string) {
	if t == "" {
		return
	}
	token = t
	tokenLen = len(token)
}

func pack(msg []byte) []byte {
	b, err := comm.IntToBytes(len(msg))
	if err != nil {
		return msg
	}
	return append(append([]byte(token), b...), msg...)
}

func unpack(buffer []byte, ch chan<- []byte) []byte {
	length := len(buffer)
	minLen := tokenLen + comm.Int32Len

	var i, start, offset int
	for i = 0; i < length; i += 1 {
		if length < i+minLen {
			break
		}
		offset = i + tokenLen
		if string(buffer[i:offset]) == token {
			start = offset
			offset = start+comm.Int32Len
			msgLen, err := comm.BytesToInt(buffer[start : offset])
			if err != nil {
				break
			}
			start = offset
			offset = start + msgLen
			if length < offset {
				break
			}
			data := buffer[start : offset]
			ch <- data

			i = offset - 1
		}
	}

	if i == length {
		return make([]byte, 0)
	}
	return buffer[i:]
}
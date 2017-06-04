// Copyright © 2017 sosozhuang <sosozhuang@163.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package util

import (
	"hash/crc32"
	"bytes"
	"encoding/binary"
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)


func ObjectToBytes(i interface{}) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytesBuffer, binary.BigEndian, i); err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func BytesToObject(b []byte, i interface{}) error {
	bytesBuffer := bytes.NewBuffer(b)
	return binary.Read(bytesBuffer, binary.BigEndian, i)
}

func IntToBytes(n int) ([]byte, error) {
	d := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytesBuffer, binary.BigEndian, d); err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func BytesToInt(b []byte) (int, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var n int32
	if err := binary.Read(bytesBuffer, binary.BigEndian, &n); err != nil {
		return 0, err
	}
	return int(n), nil
}

func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

func UpdateChecksum(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crcTable, data)
}

func Compare(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if x != b[i] {
			return false
		}
	}
	return true
}
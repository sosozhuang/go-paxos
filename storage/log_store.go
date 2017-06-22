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
package storage

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"io"
	"os"
	"path"
	"syscall"
	"unsafe"
	"github.com/sosozhuang/paxos/util"
)

type logStore struct {
	lastFile          *os.File
	metaFile          *os.File
	lastFileID        int
	lastRemovedFileID int
	curFileSize       int64
	curFileOffset     int64
	dir               string
	st                Storage
	writeCh           chan req
	logger.Logger
}

type req struct {
	instanceID uint64
	value      []byte
	retCh      chan ret
}

type ret struct {
	value []byte
	err   error
}

const (
	vfileDir     = "vfile"
	loggerOutput = "LOG"
	metaFile     = "meta"
	int32Size    = int(unsafe.Sizeof(int32(0)))
	uint32Size   = int(unsafe.Sizeof(uint32(0)))
	uint64Size   = int(unsafe.Sizeof(uint64(0)))
)

func newLogStore(dir string, st Storage) (ls *logStore, err error) {
	ls = &logStore{
		st:                st,
		dir:               path.Join(dir, vfileDir),
		lastRemovedFileID: -1,
		writeCh:           make(chan req),
	}

	defer func() {
		if err != nil {
			ls.close()
			ls = nil
		}
	}()

	// todo specify log level
	if ls.Logger, err = logger.NewLogger(loggerOutput, ls.dir, "INFO"); err != nil {
		return
	}

	info, err := os.Stat(ls.dir)
	if err != nil && !os.IsNotExist(err) {
		err = fmt.Errorf("logstore: get %s stat: %v", ls.dir, err)
		return
	} else if os.IsNotExist(err) {
		if err = os.MkdirAll(ls.dir, syscall.S_IRWXU|syscall.S_IRWXG|syscall.S_IROTH|syscall.S_IXOTH); err != nil {
			err = fmt.Errorf("logstore: make dir %s: %v", ls.dir, err)
			return
		}
	}
	if !info.IsDir() {
		err = fmt.Errorf("logstore: %s not dir", ls.dir)
		return
	}

	metaFilePath := path.Join(ls.dir, metaFile)
	if ls.metaFile, err = os.OpenFile(metaFilePath, syscall.O_CREAT|syscall.O_RDWR, syscall.S_IREAD|syscall.S_IWRITE); err != nil {
		err = fmt.Errorf("logstore: open meta file %s: %v", metaFilePath, err)
		return
	}

	if _, err = ls.metaFile.Seek(0, os.SEEK_SET); err != nil {
		err = fmt.Errorf("logstore: seek meta file %s: %v", metaFilePath, err)
		return
	}
	id := make([]byte, int32Size)
	n, err := ls.metaFile.Read(id)
	if err != nil && err != io.EOF {
		err = fmt.Errorf("logstore: read meta file %s: %v", metaFilePath, err)
		return
	}
	if n != int32Size && n != 0 {
		err = fmt.Errorf("logstore: read file id from meta file %s: length %d, expect %d", metaFilePath, n, int32Size)
		return
	}
	if n == 0 {
		ls.lastFileID = 0
	} else {
		if ls.lastFileID, err = util.BytesToInt(id); err != nil {
			err = fmt.Errorf("logstore: convert bytes to file id: %v", err)
			return
		}
	}

	cs := make([]byte, uint32Size)
	n, err = ls.metaFile.Read(cs)
	if err != nil && err != io.EOF {
		err = fmt.Errorf("logstore: read checksum from meta file %s: %v", metaFilePath, err)
		return
	}
	if err == io.EOF {
		err = nil
		return
	}
	if n != uint32Size {
		err = fmt.Errorf("logstore: read checksum from meta file %s: length %d, expect length %d", metaFilePath, n, uint32Size)
		return
	}
	var checksum uint32
	if err = util.BytesToObject(cs, &checksum); err != nil {
		err = fmt.Errorf("logstore: convert bytes to checksum: %v", err)
		return
	}
	if checksum != util.Checksum(id) {
		err = fmt.Errorf("logstore: meta file %s checksum inconsistent", metaFilePath)
		return
	}

	return
}

func (ls *logStore) open(stopped <-chan struct{}) (err error) {
	go ls.handleAppend()
	defer func() {
		if err != nil {
			log.Errorf("Open logstore %s return error: %v.", ls.dir, err)
		}
	}()
	if err = ls.rebuildIndex(); err != nil {
		return
	}

	if ls.lastFile, err = ls.openFile(ls.lastFileID); err != nil {
		return
	}

	if err = ls.expandFile(); err != nil {
		return
	}

	if ls.curFileOffset, err = ls.lastFile.Seek(ls.curFileOffset, os.SEEK_SET); err != nil {
		return
	}
	ls.Infof("Logstore init info, file id: %d, offset: %d, file size: %d.", ls.lastFileID, ls.curFileOffset, ls.curFileSize)
	return
}

func (ls *logStore) close() {
	close(ls.writeCh)
	if ls.lastFile != nil {
		ls.lastFile.Close()
		ls.lastFile = nil
	}
	if ls.metaFile != nil {
		ls.metaFile.Close()
		ls.metaFile = nil
	}
}

func (ls *logStore) rebuildIndex() error {
	maxInstanceID, maxFileID, err := ls.st.GetMaxInstanceIDFileID()
	if err != nil {
		return err
	}
	log.Debugf("Log store rebuilding index, get max instance id: %d.", maxInstanceID)
	var (
		fileID int
		offset int64
	)
	if len(maxFileID) > 0 {
		fileID, offset, _, err = parse(maxFileID)
		if err != nil {
			return err
		}

		log.Debugf("Log store rebuilding index, get file id: %d, offset %d.", fileID, offset)

		if fileID > ls.lastFileID {
			return fmt.Errorf("logstore: file id %d from db > %d from meta file", fileID, ls.lastFileID)
		}
	}

	for i := fileID; i <= ls.lastFileID; i++ {
		if err = ls.rebuildIndexForOneFile(&maxInstanceID, i, offset); err != nil {
			return err
		}
		offset = 0
	}

	return nil
}

func (ls *logStore) rebuildIndexForOneFile(maxInstanceID *uint64, fileID int, offset int64) error {
	name := path.Join(ls.dir, fmt.Sprintf("%d.f", fileID))
	if _, err := os.Stat(name); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("logstore: get %s stat: %v", name, err)
	}
	f, err := ls.openFile(fileID)
	if err != nil {
		return err
	}
	defer f.Close()

	endOffset, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("logstore: seek %s to end: %v", name, err)
	}
	curOffset, err := f.Seek(offset, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("logstore: seek %s to %d: %v", name, offset, err)
	}

	var (
		truncate   bool
		instanceID uint64
	)
	for {
		b := make([]byte, int32Size)
		n, err := f.Read(b)
		if err != nil && err != io.EOF {
			return fmt.Errorf("logstore: read %s: %v", name, err)
		}
		if n == 0 {
			ls.curFileOffset = curOffset
			break
		} else if n != int32Size {
			truncate = true
			break
		}
		i, err := util.BytesToInt(b)
		if err != nil {
			return fmt.Errorf("logstore: convert bytes to data length: %v", err)
		}
		if i == 0 {
			ls.curFileOffset = curOffset
			break
		}
		if int64(i) > endOffset || i < uint64Size {
			return fmt.Errorf("logstore: rebuild data length %d invalid, end offset %d, min data length %d",
				i, endOffset, uint64Size)
		}

		b = make([]byte, i)
		if n, err = f.Read(b); err != nil && err != io.EOF {
			return fmt.Errorf("logstore: read %s: %v", name, err)
		}
		if n != i {
			truncate = true
			break
		}

		if err = util.BytesToObject(b[:uint64Size], &instanceID); err != nil {
			err = fmt.Errorf("logstore: convert bytes to instance id: %v", err)
			return err
		}

		log.Debugf("Log store rebuilding data of instance id %d.", instanceID)
		if instanceID < *maxInstanceID {
			return fmt.Errorf("logstore: rebuild data instance id %d invalid, previous id %d", instanceID, *maxInstanceID)
		}
		*maxInstanceID = instanceID

		var state comm.AcceptorState
		if err = proto.Unmarshal(b[uint64Size:], &state); err != nil {
			ls.Warningf("Rebuild data unmarshal acceptor state error: %v.", err)
			ls.curFileOffset = curOffset
			truncate = true
			break
		}

		v, err := format(fileID, curOffset, util.Checksum(b))
		if err != nil {
			return err
		}

		if err = ls.st.RebuildOneIndex(instanceID, v); err != nil {
			return err
		}

		curOffset += int64(int32Size + i)
	}

	if truncate {
		ls.Warning("File data broken, truncate file id: %d to size %d, old file size: %d.", fileID, curOffset, endOffset)
		if err = f.Truncate(curOffset); err != nil {
			return fmt.Errorf("logstore: truncate file %s: %v", name, err)
		}
	}
	return nil
}

func format(fileID int, offset int64, checksum uint32) (b []byte, err error) {
	b, err = util.IntToBytes(fileID)
	if err != nil {
		err = fmt.Errorf("logstore: convert file id %d to bytes: %v", fileID, err)
		return
	}
	var x []byte
	x, err = util.ObjectToBytes(offset)
	if err != nil {
		err = fmt.Errorf("logstore: convert offset %d to bytes: %v", offset, err)
		return
	}
	b = append(b, x...)
	x, err = util.ObjectToBytes(checksum)
	if err != nil {
		err = fmt.Errorf("logstore: convert checksum %d to bytes: %v", checksum, err)
		return
	}
	b = append(b, x...)
	return
}

func parse(b []byte) (fileID int, offset int64, checksum uint32, err error) {
	i, j := 0, int32Size
	if fileID, err = util.BytesToInt(b[i:j]); err != nil {
		err = fmt.Errorf("logstore: convert bytes to file id: %v", err)
		return
	}
	i = j
	j = i + uint64Size
	if err = util.BytesToObject(b[i:j], &offset); err != nil {
		err = fmt.Errorf("logstore: convert bytes to offset: %v", err)
		return
	}
	if err = util.BytesToObject(b[j:], &checksum); err != nil {
		err = fmt.Errorf("logstore: convert byte to checksum: %v", err)
	}
	return
}

func (ls *logStore) openFile(fileID int) (*os.File, error) {
	name := path.Join(ls.dir, fmt.Sprintf("%d.f", fileID))
	f, err := os.OpenFile(name, syscall.O_CREAT|syscall.O_RDWR, syscall.S_IWRITE|syscall.S_IREAD)
	if err != nil {
		return nil, fmt.Errorf("logstore: open file %s: %v", name, err)
	}
	return f, nil
}

func (ls *logStore) expandFile() error {
	var err error
	ls.curFileSize, err = ls.lastFile.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("logstore: seek file %s to end: %v", ls.lastFile.Name(), err)
	}

	if ls.curFileSize == 0 {
		ls.curFileSize, err = ls.lastFile.Seek(1024*1024*100-1, os.SEEK_SET)
		if err != nil {
			return fmt.Errorf("logstore: expand file %s: %v", ls.lastFile.Name(), err)
		}
		if ls.curFileSize != 1024*1024*100-1 {
			return errors.New("logstore: expand file size != 104857600-1")
		}

		n, err := ls.lastFile.Write([]byte{0})
		if err != nil {
			return fmt.Errorf("logstore: write 0 to file %s: %v", ls.lastFile.Name(), err)
		}
		if n != 1 {
			return fmt.Errorf("logstore: can't write 0 to file %s", ls.lastFile.Name())
		}
		if ls.curFileOffset, err = ls.lastFile.Seek(0, os.SEEK_SET); err != nil {
			return fmt.Errorf("logstore: seek file %s to start: %v", ls.lastFile.Name(), err)
		}
	}

	return nil
}

func (ls *logStore) incFileID() error {
	fileID, err := util.IntToBytes(ls.lastFileID + 1)
	if err != nil {
		return fmt.Errorf("logstore: convert file id to bytes: %v", err)
	}
	checksum, err := util.ObjectToBytes(util.Checksum(fileID))
	if err != nil {
		return fmt.Errorf("logstore: convert checksum to bytes: %v", err)
	}

	if _, err = ls.metaFile.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("logstore: seek meta file to start: %v", err)
	}

	if _, err = ls.metaFile.Write(fileID); err != nil {
		return fmt.Errorf("logstore: write file id to meta file: %v", err)
	}
	if _, err = ls.metaFile.Write(checksum); err != nil {
		return fmt.Errorf("logstore: write checksum to meta file: %v", err)
	}
	if err = ls.metaFile.Sync(); err != nil {
		return fmt.Errorf("logstore: sync meta file: %v", err)
	}
	ls.lastFileID += 1
	log.Infof("Logstore switch to new file id: %d.", ls.lastFileID)
	return nil
}

func (ls *logStore) removeFiles(fileID int) error {
	if ls.lastRemovedFileID >= fileID {
		ls.Warningf("Logstore try to remove file id %d, last removed file id %d.",
			fileID, ls.lastRemovedFileID)
		return nil
	}

	if ls.lastRemovedFileID == -1 && fileID > 2000 {
		ls.lastRemovedFileID = fileID - 2000
	}

	for i := ls.lastRemovedFileID; i <= fileID; i++ {
		name := path.Join(ls.dir, fmt.Sprintf("%d.f", i))
		_, err := os.Stat(name)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("logstore: get %s stat: %v", name, err)
		}
		if os.IsNotExist(err) {
			ls.lastRemovedFileID = i
			continue
		}

		if err = os.Remove(name); err != nil {
			return fmt.Errorf("logstore: remove file %s: %v", name, err)
		}

		ls.lastRemovedFileID = i
		ls.Infof("File %s removed.", name)
	}

	return nil
}

func (ls *logStore) read(b []byte) (uint64, []byte, error) {
	var (
		instanceID uint64
		value      []byte
	)
	fileID, offset, checksum, err := parse(b)
	if err != nil {
		return instanceID, value, err
	}
	f, err := ls.openFile(fileID)
	if err != nil {
		return instanceID, value, err
	}
	defer f.Close()

	_, err = f.Seek(offset, os.SEEK_SET)
	if err != nil {
		return instanceID, value, fmt.Errorf("logstore: seek %s to offset %d: %v",
			f.Name(), offset, err)
	}

	b = make([]byte, int32Size)
	n, err := f.Read(b)
	if err != nil {
		return instanceID, value, fmt.Errorf("logstore: read %s: %v",
			f.Name(), err)
	}
	if n != int32Size {
		return instanceID, value, fmt.Errorf("logstore: can't read %s", f.Name())
	}

	l, err := util.BytesToInt(b)
	if err != nil {
		return instanceID, value, fmt.Errorf("logstore: convert bytes to data length: %v", err)
	}

	b = make([]byte, l)
	n, err = f.Read(b)
	if err != nil {
		return instanceID, value, fmt.Errorf("logstore: read file %s: %v",
			f.Name(), err)
	}
	if n != l {
		return instanceID, value, fmt.Errorf("logstore: read file %s error", f.Name())
	}
	if checksum != util.Checksum(b) {
		return instanceID, value, errors.New("logstore: verify checksum error")
	}

	if err = util.BytesToObject(b[:uint64Size], &instanceID); err != nil {
		return instanceID, value, errors.New("logstore: convert byte to instance id error")
	}
	value = b[uint64Size:]

	return instanceID, value, nil
}

func (ls *logStore) getFileOffset(i int) (offset int64, err error) {
	if ls.lastFile == nil {
		err = fmt.Errorf("logstore: file %s closed", ls.lastFile.Name())
		return
	}

	if offset, err = ls.lastFile.Seek(ls.curFileOffset, os.SEEK_SET); err != nil {
		err = fmt.Errorf("logstore: seek file %s to offset %d: %v",
			ls.lastFile.Name(), ls.curFileOffset, err)
		return
	}
	if offset+int64(i) > ls.curFileSize {
		if err = ls.lastFile.Close(); err != nil {
			err = fmt.Errorf("logstore: close file %s: %v", ls.lastFile.Name(), err)
			return
		}
		if err = ls.incFileID(); err != nil {
			return
		}
		if ls.lastFile, err = ls.openFile(ls.lastFileID); err != nil {
			return
		}
		if offset, err = ls.lastFile.Seek(0, os.SEEK_END); err != nil {
			err = fmt.Errorf("logstore: seek file %s to end: %v", ls.lastFile.Name(), err)
			return
		}
		if offset != 0 {
			err = fmt.Errorf("logstore: expect %s a new file but in used", ls.lastFile.Name())
			return
		}
		if err = ls.expandFile(); err != nil {
			return
		}

	}

	return
}

func (ls *logStore) append(instanceID uint64, value []byte) ([]byte, error) {
	x := req{
		instanceID: instanceID,
		value:      value,
		retCh:      make(chan ret),
	}
	ls.writeCh <- x
	ret := <-x.retCh
	return ret.value, ret.err
}

func (ls *logStore) handleAppend() {
	for x := range ls.writeCh {
		v, err := ls.doAppend(x.instanceID, x.value)
		select {
		case x.retCh <- ret{v, err}:
		default:
		}
	}
}

func (ls *logStore) doAppend(instanceID uint64, value []byte) ([]byte, error) {
	head := len(value) + uint64Size
	wlen := int32Size + head
	offset, err := ls.getFileOffset(wlen)
	if err != nil {
		return nil, err
	}

	b, err := util.IntToBytes(head)
	if err != nil {
		return nil, fmt.Errorf("logstore: convert %d to bytes: %v", head, err)
	}
	i, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return nil, fmt.Errorf("logstore: convert %d to bytes: %v", instanceID, err)
	}
	b = append(b, i...)
	b = append(b, value...)

	n, err := ls.lastFile.Write(b)
	if err != nil {
		return nil, fmt.Errorf("logstore: write %s: %v", ls.lastFile.Name(), err)
	}
	if n != wlen {
		return nil, errors.New("logstore: write file error")
	}

	// todo: check sync option
	if true {
		if err = ls.lastFile.Sync(); err != nil {
			return nil, err
		}
	}
	ls.curFileOffset += int64(wlen)

	return format(ls.lastFileID, offset, util.Checksum(b[int32Size:]))
}

func (ls *logStore) delete(b []byte) error {
	fileID, _, _, err := parse(b)
	if err != nil {
		return err
	}
	if fileID > ls.lastFileID {
		return nil
	}

	if fileID > 0 {
		return ls.removeFiles(fileID - 1)
	}

	return nil
}

func (ls *logStore) truncate(b []byte) error {
	fileID, offset, _, err := parse(b)
	if err != nil {
		return err
	}
	if fileID != ls.lastFileID {
		return fmt.Errorf("logstore: can't truncate file id %d, last file id %d", fileID, ls.lastFileID)
	}
	if err = ls.lastFile.Truncate(offset); err != nil {
		return fmt.Errorf("logstore: truncate file %s to size %d: %v",
			ls.lastFile.Name(), offset, err)
	}
	return nil
}

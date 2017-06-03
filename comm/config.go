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
package comm

import (
	"time"
	"fmt"
	"bytes"
)

type Config struct {
	//node
	Name             string
	GroupCount       int
	Members          string
	EnableMembership bool
	ForceNewMembers  bool
	FollowerMode     bool
	FollowNode       string
	ProposeTimeout   time.Duration

	//storage
	DataDir          string
	StorageType      string
	Sync             bool
	SyncPeriod       time.Duration
	SyncCount        int
	DisableWAL       bool
	MaxLogCount      int64

	//peer network
	Token            string
	ListenMode       string
	AdvertiseIP      string
	ListenPeerIP     string
	ListenPeerPort   int
	ListenTimeout    time.Duration
	DialTimeout      time.Duration
	WriteTimeout     time.Duration
	ReadTimeout      time.Duration
	KeepAlive        time.Duration
	ServerChanCap    int
	ClientChanCap    int

	//client network
	ListenClientAddr string

	//election
	EnableElection   bool
	ElectionTimeout  time.Duration

	//log
	LogDir           string
	LogOutput        string
	LogLevel         string
	LogAppend        bool
}

func (cfg Config) String() string {
	x := bytes.NewBufferString("")
	fmt.Fprintln(x, "[node config]")
	fmt.Fprintf(x, "name: %s\n", cfg.Name)
	fmt.Fprintf(x, "group count: %d\n", cfg.GroupCount)
	fmt.Fprintf(x, "members: %s\n", cfg.Members)
	fmt.Fprintf(x, "enable membership: %v\n", cfg.EnableMembership)
	fmt.Fprintf(x, "force new members: %v\n", cfg.ForceNewMembers)
	fmt.Fprintf(x, "follower mode: %v\n", cfg.FollowerMode)
	fmt.Fprintf(x, "follow node: %s\n", cfg.FollowNode)
	fmt.Fprintf(x, "propose timeout: %v\n", cfg.ProposeTimeout)

	fmt.Fprintln(x, "[storage config]")
	fmt.Fprintf(x, "data dir: %s\n", cfg.DataDir)
	fmt.Fprintf(x, "storage type: %s\n", cfg.StorageType)
	fmt.Fprintf(x, "sync: %v\n", cfg.Sync)
	fmt.Fprintf(x, "sync period: %v\n", cfg.SyncPeriod)
	fmt.Fprintf(x, "sync count: %d\n", cfg.SyncCount)
	fmt.Fprintf(x, "disable wal: %v\n", cfg.DisableWAL)
	fmt.Fprintf(x, "max log count: %d\n", cfg.MaxLogCount)

	fmt.Fprintln(x, "[peer network config]")
	fmt.Fprintf(x, "token: %s\n", cfg.Token)
	fmt.Fprintf(x, "listen mode: %s\n", cfg.ListenMode)
	fmt.Fprintf(x, "advertise ip: %s\n", cfg.AdvertiseIP)
	fmt.Fprintf(x, "listen peer ip: %s\n", cfg.ListenPeerIP)
	fmt.Fprintf(x, "listen peer port: %d\n", cfg.ListenPeerPort)
	fmt.Fprintf(x, "listen timeout: %v\n", cfg.ListenTimeout)
	fmt.Fprintf(x, "dial timeout: %v\n", cfg.DialTimeout)
	fmt.Fprintf(x, "write timeout: %v\n", cfg.WriteTimeout)
	fmt.Fprintf(x, "read timeout: %v\n", cfg.ReadTimeout)
	fmt.Fprintf(x, "keep alive period: %v\n", cfg.KeepAlive)
	fmt.Fprintf(x, "server channel capacity: %d\n", cfg.ServerChanCap)
	fmt.Fprintf(x, "client channel capacity: %d\n", cfg.ClientChanCap)

	fmt.Fprintln(x, "[client network config]")
	fmt.Fprintf(x, "listen client addr: %s\n", cfg.ListenClientAddr)

	fmt.Fprintln(x, "[election config]")
	fmt.Fprintf(x, "enable election: %v\n", cfg.EnableElection)
	fmt.Fprintf(x, "election timeout: %v\n", cfg.ElectionTimeout)

	fmt.Fprintln(x, "[log config]")
	fmt.Fprintf(x, "log dir: %s\n", cfg.LogDir)
	fmt.Fprintf(x, "log output: %s\n", cfg.LogOutput)
	fmt.Fprintf(x, "log level: %s\n", cfg.LogLevel)
	fmt.Fprintf(x, "log append: %v", cfg.LogAppend)
	return x.String()
}

const (
	DefaultConfigFile = ""
	//node
	DefaultName = "default"
	DefaultGroupCount = 10
	DefaultMembers = ""
	DefaultMembership = true
	DefaultForceNewMembers = false
	DefaultFollowerMode = false
	DefaultFollowNode = ""
	DefaultProposeTimeout = 10 * 1000

	//storage
	DefaultDataDir = ""
	DefaultStorage = "rocksdb"
	DefaultSync = true
	DefaultSyncPeriod = 10000
	DefaultSyncCount = 100
	DefaultDisableWAL = false
	DefaultMaxLogCount = 10000

	//peer network
	DefaultToken = "paxos"
	DefaultListenMode = "tcp"
	DefaultAdvertiseIP = ""
	DefaultListenPeerIP = ""
	DefaultListenPeerPort = 17524
	DefaultListenTimeout = 3 * 1000
	DefaultDialTimeout = 30 * 1000
	DefaultWriteTimeout = 3 * 1000
	DefaultReadTimeout = 3 * 1000
	DefaultKeepAlivePeriod = 60 * 60
	DefaultServerCap = 100
	DefaultClientCap = 100

	//client network
	DefaultListenClientAddr = "http://127.0.0.1:17603"

	//election
	DefaultElection = true
	DefaultElectionTimeout = 10

	//log
	DefaultLogDir = "."
	DefaultLogOutput = ""
	DefaultLogLevel = "INFO"
	DefaultLogAppend = true
)

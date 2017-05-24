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

import "time"

type Config struct {
	//node
	Name             string
	GroupCount       int
	Peers            string
	EnableMemberShip bool
	FollowerMode     bool
	FollowNode       string
	ProposeTimeout   time.Duration

	//storage
	DataDir     string
	StorageType string
	Sync        bool
	SyncPeriod  time.Duration
	SyncCount   int
	DisableWAL  bool

	//network
	ListenMode    string
	Token         string
	AdvertiseIP   string
	ListenIP      string
	ListenPort    int
	ListenTimeout time.Duration
	DialTimeout   time.Duration
	WriteTimeout  time.Duration
	ReadTimeout   time.Duration
	KeepAlive     time.Duration
	ServerChanCap int
	ClientChanCap int

	//election
	EnableElection  bool
	ElectionTimeout time.Duration

	//log
	LogOutput string
	LogDir    string
	LogLevel  string
}

const (
	DefaultConfigFile     = ""
	//node
	DefaultName           = "default"
	DefaultGroupCount     = 10
	DefaultPeers          = ""
	DefaultMemberShip     = true
	DefaultFollowerMode   = true
	DefaultFollowNode     = ""
	DefaultProposeTimeout = 10000
	
	//storage
	DefaultDataDir    = ""
	DefaultStorage    = "rocksdb"
	DefaultSync       = true
	DefaultSyncPerid  = 10000
	DefaultSyncCount  = 10000
	DefaultDisableWAL = false

	//network
	DefaultToken           = "paxos"
	DefaultListenMode      = "tcp"
	DefaultAdvertiseIP     = ""
	DefaultListenIP        = ""
	DefaultListenPort      = 17524
	DefaultListenTimeout   = 10000
	DefaultDialTimeout     = 10000
	DefaultSendTimeout     = 10000
	DefaultReceiveTimeout  = 10000
	DefaultKeepAlivePeriod = 10000
	DefaultServerCap       = 100
	DefaultClientCap       = 100

	//election
	DefaultElection        = true
	DefaultElectionTimeout = 10000

	//log
	DefaultLogDir    = "."
	DefaultLogOutput = ""
	DefaultLogLevel  = "INFO"
)

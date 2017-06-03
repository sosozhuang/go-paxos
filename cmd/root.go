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

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/sosozhuang/paxos/paxosmain"
	"github.com/sosozhuang/paxos/comm"
	"time"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "paxos",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := &comm.Config{
			Name: viper.GetString("name"),
			GroupCount: viper.GetInt("group-count"),
			Members: viper.GetString("members"),
			EnableMembership: viper.GetBool("membership"),
			ForceNewMembers: viper.GetBool("force-new-members"),
			FollowerMode: viper.GetBool("follower-mode"),
			FollowNode: viper.GetString("follow-node"),
			ProposeTimeout: viper.GetDuration("propose-timeout") * time.Millisecond,

			DataDir: viper.GetString("data-dir"),
			StorageType: viper.GetString("storage"),
			Sync: viper.GetBool("sync"),
			SyncPeriod: viper.GetDuration("sync-period") * time.Millisecond,
			SyncCount: viper.GetInt("sync-count"),
			MaxLogCount: viper.GetInt64("max-log-count"),

			Token: viper.GetString("token"),
			ListenMode: viper.GetString("listen-mode"),
			AdvertiseIP: viper.GetString("advertise-ip"),
			ListenPeerIP: viper.GetString("listen-peer-ip"),
			ListenPeerPort: viper.GetInt("listen-peer-port"),
			ListenTimeout: viper.GetDuration("listen-timeout") * time.Millisecond,
			DialTimeout: viper.GetDuration("dial-timeout") * time.Millisecond,
			WriteTimeout: viper.GetDuration("write-timeout") * time.Millisecond,
			ReadTimeout: viper.GetDuration("read-timeout") * time.Millisecond,
			KeepAlive: viper.GetDuration("keepalive-period") * time.Second,
			ServerChanCap: viper.GetInt("server-capacity"),
			ClientChanCap: viper.GetInt("client-capacity"),

			ListenClientAddr: viper.GetString("listen-client-addr"),

			EnableElection: viper.GetBool("election"),
			ElectionTimeout: viper.GetDuration("election-timeout") * time.Second,

			LogDir: viper.GetString("log-dir"),
			LogOutput: viper.GetString("log-output"),
			LogLevel: viper.GetString("log-level"),
			LogAppend: viper.GetBool("log-append"),
		}

		err := paxosmain.StartPaxos(cfg)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(-1)
		}
	},
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	flags := RootCmd.Flags()
	flags.SortFlags = false
	flags.StringVar(&cfgFile, "config", comm.DefaultConfigFile, "config file for paxos")
	flags.String("name", comm.DefaultName, "readable name for this member")
	flags.Int("group-count", comm.DefaultGroupCount, "number of group")
	flags.String("members", comm.DefaultMembers, "initial cluster members for bootstrapping (comma-separated)")
	flags.Bool("membership", comm.DefaultMembership, "enable cluster membership")
	flags.Bool("force-new-members", comm.DefaultForceNewMembers, "force to create new cluster members")
	flags.Bool("follower-mode", comm.DefaultFollowerMode, "enable follower mode")
	flags.String("follow-node", comm.DefaultFollowNode, "node address to follow if 'follower-mode' is true")
	flags.Int("propose-timeout", comm.DefaultProposeTimeout, "time (in milliseconds) for a proposal to timeout")

	flags.String("data-dir", comm.DefaultDataDir, "path to the data directory")
	flags.String("storage", comm.DefaultStorage, "storage type of the data")
	flags.Bool("sync", comm.DefaultSync, "enable sync")
	flags.Int("sync-period", comm.DefaultSyncPeriod, "time (in milliseconds) of committed transactions to trigger sync")
	flags.Int("sync-count", comm.DefaultSyncCount, "number of committed transactions to trigger sync")
	flags.Bool("disable-wal", comm.DefaultDisableWAL, "disable write ahead log")
	flags.Int64("max-log-count", comm.DefaultMaxLogCount, "number of logs to retain")

	flags.String("token", comm.DefaultToken, "token of peers' traffic")
	flags.String("listen-mode", comm.DefaultListenMode, "network listen mode ('tcp' or 'udp')")
	flags.String("advertise-ip", comm.DefaultAdvertiseIP, "advertise ip to the cluster")
	flags.String("listen-peer-ip", comm.DefaultListenPeerIP, "ip to listen on for peers' traffic")
	flags.Int("listen-peer-port", comm.DefaultListenPeerPort, "port to listen on for peers' traffic")
	flags.Int("listen-timeout", comm.DefaultListenTimeout, "time (in milliseconds) for server listen to timeout")
	flags.Int("dial-timeout", comm.DefaultDialTimeout, "time (in milliseconds) for a client dial to timeout")
	flags.Int("write-timeout", comm.DefaultWriteTimeout, "time (in milliseconds) for a client write to timeout")
	flags.Int("read-timeout", comm.DefaultReadTimeout, "time (in milliseconds) for a server read to timeout")
	flags.Int("keepalive-period", comm.DefaultKeepAlivePeriod, "keep-alive period (in seconds) for an active connection")
	flags.Int("server-capacity", comm.DefaultServerCap, "buffer capacity for server receive message channel")
	flags.Int("client-capacity", comm.DefaultClientCap, "buffer capacity for client send message channel")

	flags.String("listen-client-addr", comm.DefaultListenClientAddr, "address to listen on for clients' traffic")

	flags.Bool("election", comm.DefaultElection, "enable leader election")
	flags.Int("election-timeout", comm.DefaultElectionTimeout, "time (in milliseconds) for an election to timeout")

	flags.String("log-dir", comm.DefaultLogDir, "path to the log directory")
	flags.String("log-output", comm.DefaultLogOutput, "log output for logger, can specify 'stdout', 'stderr' or '/dev/null'")
	flags.String("log-level", comm.DefaultLogLevel, "log level for logger")
	flags.Bool("log-append", comm.DefaultLogAppend, "append for logger")
	viper.BindPFlags(flags)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName(".paxos") // name of config file (without extension)
		viper.AddConfigPath(".")      // adding current directory as first search path
		//viper.AddConfigPath("$HOME")  // adding home directory as second search path
	}

	viper.SetEnvPrefix("paxos")
	viper.AutomaticEnv()          // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintln(os.Stderr, "Reading config file error:", err)
		os.Exit(-1)
	}
}

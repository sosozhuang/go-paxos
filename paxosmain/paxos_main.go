package paxosmain

import (
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/node"
	"github.com/sosozhuang/paxos/util"
	"time"
	"math/rand"
	"github.com/sosozhuang/paxos/comm"
	"runtime"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func StartPaxos(cfg *comm.Config) error {
	if err := logger.SetupLogger(cfg.LogOutput, cfg.LogDir, cfg.LogLevel, cfg.LogAppend); err != nil {
		return err
	}
	n, err := node.NewNode(cfg)
	if err != nil {
		return err
	}

	if err = n.Start(); err != nil {
		return err
	}

	util.HandleInterrupts()
	n.Serve()
	util.ExitPaxos(0)
	return nil
}
package util

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	mu     sync.Mutex
	hookMu sync.Mutex
	hooks  = []func(){}
)

func ExitPaxos(code int) {
	mu.Lock()
	os.Exit(code)
}

func AddHook(h func()) {
	hookMu.Lock()
	defer hookMu.Unlock()
	hooks = append(hooks, h)
}

func HandleInterrupts() {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier, syscall.SIGINT, syscall.SIGTERM)
	go func() {

		sig := <-notifier
		mu.Lock()

		hookMu.Lock()
		hs := make([]func(), len(hooks))
		for i, j := 0, len(hooks)-1; j >= 0; i, j = i+1, j-1 {
			hs[i] = hooks[j]
		}
		hookMu.Unlock()
		for _, h := range hs {
			h()
		}
		signal.Stop(notifier)
		pid := syscall.Getpid()
		syscall.Kill(pid, sig.(syscall.Signal))
	}()
}

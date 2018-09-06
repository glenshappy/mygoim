package main

import (
	"flag"
	"goim/libs/perf"
	"runtime"

	log "github.com/thinkboy/log4go"
)

var (
	DefaultStat *Stat
)

func main() {
	flag.Parse()
	if err := InitConfig(); err != nil {
		panic(err)
	}
	runtime.GOMAXPROCS(Conf.MaxProc)
	log.LoadConfiguration(Conf.Log)
	defer log.Close()
	log.Info("logic[%s] start", Ver)
	perf.Init(Conf.PprofAddrs)
	DefaultStat = NewStat()
	// router rpc
	if err := InitRouter(Conf.RouterRPCAddrs); err != nil {
		panic(err)
	}
	// start monitor
	if Conf.MonitorOpen {
		InitMonitor(Conf.MonitorAddrs)
	}
	MergeCount()
	go SyncCount()
	// logic rpc
	if err := InitRPC(NewDefaultAuther()); err != nil {
		panic(err)
	}
	if err := InitHTTP(); err != nil {
		panic(err)
	}
	if err := InitNsq("127.0.0.1:4150"); err != nil {
		panic(err)
	}
	// block until a signal is received.
	InitSignal()
}

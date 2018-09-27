package main

import (
	"flag"
	log "github.com/thinkboy/log4go"
	"goim/libs/perf"
	"runtime"
)

var (
	DefaultStat *Stat
)

func main() {
	flag.Parse()
	if err := InitConfig(); err != nil {
		panic(err)
	}
	log.LoadConfiguration(Conf.Log)
	runtime.GOMAXPROCS(runtime.NumCPU())
	perf.Init(Conf.PprofAddrs)
	DefaultStat = NewStat()
	// comet
	err := InitComet(Conf.Comets,
		CometOptions{
			RoutineSize: Conf.RoutineSize,
			RoutineChan: Conf.RoutineChan,
		})
	if err != nil {
		log.Warn("comet rpc current can't connect, retry")
	}
	// start monitor
	if Conf.MonitorOpen {
		InitMonitor(Conf.MonitorAddrs)
	}
	// round
	round := NewRound(RoundOptions{
		Timer:     Conf.Timer,
		TimerSize: Conf.TimerSize,
	})
	// room
	InitRoomBucket(round,
		RoomOptions{
			BatchNum:   Conf.RoomBatch,
			SignalTime: Conf.RoomSignal,
			IdleTime:   Conf.RoomIdle,
		})
	//room info
	MergeRoomServers()
	go SyncRoomServers()
	InitPush()
	if err := InitKafka(); err != nil {
		panic(err)
	}
	// block until a signal is received.
	InitSignal()
}
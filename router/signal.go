package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/thinkboy/log4go"
)

// InitSignal register signals handler.
func InitSignal() {
	c := make(chan os.Signal, 1)
	// syscall.SIGSTOP
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		log.Info("router[%s] get a signal %s", VERSION, s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			return
		case syscall.SIGHUP:
			reload()
		default:
			return
		}
	}
}

func reload() {
	newConf, err := ReloadConfig()
	if err != nil {
		log.Error("ReloadConfig() error(%v)", err)
		return
	}
	Conf = newConf
}

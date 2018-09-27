package main

import (
	"encoding/json"
	"fmt"

	"os"

	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	log "github.com/thinkboy/log4go"
)

var (
	producer sarama.AsyncProducer
)

type KafkaMsg struct {
	OP       string   `json:"op"`
	RoomId   int32    `json:"roomid,omitempty"`
	ServerId int32    `json:"server,omitempty"`
	SubKeys  []string `json:"subkeys,omitempty"`
	Msg      []byte   `json:"msg"`
	Ensure   bool     `json:"ensure,omitempty"`
}

func main() {
	var err error
	msg := []byte("wodexiaoxi")
	log.Info("start send [%s]", string(msg))
	var (
		vBytes      []byte
		v           = &KafkaMsg{OP: "broadcast", Msg: msg}
		kafkaAdress = []string{"127.0.0.1:9092"}
	)
	if vBytes, err = json.Marshal(v); err != nil {
		return
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err = sarama.NewAsyncProducer(kafkaAdress, config)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(vBytes))
	fmt.Println(producer)
	go handleSuccess()
	producer.Input() <- &sarama.ProducerMessage{Topic: "KafkaPushsTopic", Value: sarama.ByteEncoder(vBytes)}
	InitSignal()
}

func InitSignal() {
	c := make(chan os.Signal, 1)
	//, syscall.SIGSTOP
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		//log.Info("comet[%s] get a signal %s", Ver, s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			log.Info("end signal received!")
			return

		default:
			return
		}
	}
}

func handleSuccess() {
	var (
		pm *sarama.ProducerMessage
	)
	for {
		pm = <-producer.Successes()
		fmt.Println("success before")
		if pm != nil {
			log.Info("producer message success, partition:%d offset:%d key:%v valus:%s", pm.Partition, pm.Offset, pm.Key, pm.Value)
		}
		fmt.Println("success!")
		// increase msg succeeded stat
		//DefaultStat.IncrMsgSucceeded()
	}
}

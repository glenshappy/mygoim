package main

import (
	"encoding/json"
	"goim/libs/define"
	"goim/libs/encoding/binary"
	"goim/libs/proto"

	"fmt"

	"github.com/Shopify/sarama"
	nsq "github.com/nsqio/go-nsq"
	log "github.com/thinkboy/log4go"
)

var (
	producer *nsq.Producer
	config   nsq.Config
)

func InitNsq(str string) (err error) {
	config := nsq.NewConfig()
	log.Info("init nsq: %v", str)
	producer, err = nsq.NewProducer(str, config)
	//go handleSuccess()
	//go handleError()
	return
}

/*

func handleSuccess() {
	var (
		pm *sarama.ProducerMessage
	)
	for {
		pm = <-producer.Successes()
		if pm != nil {
			log.Info("producer message success, partition:%d offset:%d key:%v valus:%s", pm.Partition, pm.Offset, pm.Key, pm.Value)
		}
		// increase msg succeeded stat
		DefaultStat.IncrMsgSucceeded()
	}
}

func handleError() {
	var (
		err *sarama.ProducerError
	)
	for {
		err = <-producer.Errors()
		if err != nil {
			log.Error("producer message error, partition:%d offset:%d key:%v valus:%s error(%v)", err.Msg.Partition, err.Msg.Offset, err.Msg.Key, err.Msg.Value, err.Err)
		}
		// increase msg failed stat
		DefaultStat.IncrMsgFailed()
	}
}
*/

//发布消息
func Publish(topic string, message string) error {
	var err error
	if producer != nil {
		if message == "" { //不能发布空串，否则会导致error
			return nil
		}
		err = producer.Publish(topic, []byte(message)) // 发布消息
		return err
	}
	return fmt.Errorf("producer is nil", err)
}

func mpushKafka(serverId int32, keys []string, msg []byte) (err error) {
	var (
		vBytes []byte
		v      = &proto.KafkaMsg{OP: define.KAFKA_MESSAGE_MULTI, ServerId: serverId, SubKeys: keys, Msg: msg}
	)
	if vBytes, err = json.Marshal(v); err != nil {
		return
	}

	Publish("test", string(vBytes))
	return
}

func broadcastKafka(msg []byte) (err error) {
	var (
		vBytes []byte
		v      = &proto.KafkaMsg{OP: define.KAFKA_MESSAGE_BROADCAST, Msg: msg}
	)
	if vBytes, err = json.Marshal(v); err != nil {
		return
	}

	Publish("test", string(vBytes))
	return
}

func broadcastRoomKafka(rid int32, msg []byte, ensure bool) (err error) {
	var (
		vBytes   []byte
		ridBytes [4]byte
		v        = &proto.KafkaMsg{OP: define.KAFKA_MESSAGE_BROADCAST_ROOM, RoomId: rid, Msg: msg, Ensure: ensure}
	)
	if vBytes, err = json.Marshal(v); err != nil {
		return
	}
	binary.BigEndian.PutInt32(ridBytes[:], rid)
	producer.Input() <- &sarama.ProducerMessage{Topic: Conf.KafkaTopic, Key: sarama.ByteEncoder(ridBytes[:]), Value: sarama.ByteEncoder(vBytes)}
	Publish("test", string())
	return
}

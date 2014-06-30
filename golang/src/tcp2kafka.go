package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"
	"syscall"
	"time"
	"os"
)

import (
	"github.com/Shopify/sarama"
	"github.com/stathat/jconfig"
)

// 构造kafka producer默认参数
func makeConfig() *sarama.ProducerConfig {
	config := sarama.NewProducerConfig()

	config.RequiredAcks = sarama.WaitForLocal // NoResponse, WaitForLocal, WaitForAll
	config.MaxBufferTime = 1000 * time.Millisecond
	config.MaxBufferedBytes = 1024 * 1024 * 16

	return config
}

// 构造kafka producer client
func makeProducer(addr []string) (producer *sarama.Producer, err error) {

	clientConfig := &sarama.ClientConfig{MetadataRetries: 10, WaitForElection: 250 * time.Millisecond}
	client, err := sarama.NewClient("client_id", addr, clientConfig)
	if err != nil {
		return
	}

	producer, err = sarama.NewProducer(client, makeConfig())
	if err != nil {
		return
	}

	defer producer.Close()

	return
}

// 异步发送msg
func publish(producer *sarama.Producer, topic string, msg string) error {
	return producer.QueueMessage(topic, nil, sarama.StringEncoder(msg))
}

// 根据消息体选择目的producer client
func selectProducer(msg map[string]interface{}) (p *sarama.Producer, err error) {

	cs := CONFIG.GetArray("clusters")
	lb := CONFIG.GetString("load_balancer")

	for i := 0; i < len(cs); i++ {
		c := cs[i].(map[string]interface{})

		target, ok := c[lb].([]interface{})
		if !ok {
			continue
		}

		for j := 0; j < len(target); j++ {
			t := target[j].(string)
			idc := msg["idc"]
			topic := msg["topic"]
			if t == idc.(string) || t == topic.(string) {
				p = PRODUCERS[c["name"].(string)]
				return
			}
		}
	}

	if p == nil {
		for _, v := range PRODUCERS {
			p = v
			break
		}
	}

	return
}

// 验证msg格式
func verify(s []byte) (producer *sarama.Producer, topic string, msg string, err error) {

	var j map[string]interface{}
	err = json.Unmarshal(s, &j)
	if err != nil {
		return
	}

	product, ok := j["product"].(string)
	if !ok {
		err = errors.New("product not find in message")
		return
	}
	service, ok := j["service"].(string)
	if !ok {
		err = errors.New("service not find in message")
		return
	}

	topic = product + "_" + service + "_topic"

	j["topic"] = topic

	syscall.Gettimeofday(&TV)
	j["collector_time"] = (int64(TV.Sec)*1e3 + int64(TV.Usec)/1e3)

	producer, err = selectProducer(j)
	if nil != err {
		return
	}

	msgInByte, err := json.Marshal(j)

	msg = string(msgInByte)

	return
}

// 处理连接请求
func handle(conn net.Conn, ch chan []byte) {
	defer conn.Close()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
	}

	reader := bufio.NewReader(conn)

	for {
	  // 读取bytes，push进channel,等待发送routine消费

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetReadDeadline(time.Now().Add(5 * time.Second))
			tcpConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}

		line, err := reader.ReadBytes('\n')

		if err != nil {
			return
		}

		if '\n' == line[len(line)-1] {
			line = line[:len(line)-1]
		}

		ch <- line
	}
}

// 初始化producer clients
func initProducers(config *jconfig.Config) error {

	PRODUCERS = make(map[string]*sarama.Producer)

	cs := config.GetArray("clusters")

	for i := 0; i < len(cs); i++ {
		c := cs[i].(map[string]interface{})

		tmp_broker_list, ok := c["broker_list"].([]interface{})
		if !ok {
			return errors.New("broker_list not find in config")
		}

		broker_list := make([]string, 5)
		for j := 0; j < len(tmp_broker_list); j++ {
			broker_list = append(broker_list, tmp_broker_list[j].(string))
		}

		if len(broker_list) < 1 {
			return errors.New("broker_list is null")
		}

		p, e := makeProducer(broker_list)
		if nil != e {
			log.Fatal(e)
			return e
		} else {
			name, ok := c["name"].(string)
			if ok {
				PRODUCERS[name] = p
			} else {
				return errors.New("cluster name is not provided")
			}
		}
	}
	return nil
}

// 清空Error channel
func dumpErrors(p *sarama.Producer) {
	for {
		err := <-p.Errors()
		log.Println(err)
	}
}

// 从channel中消费msg，发送
func publishMsg(ch chan []byte) {
	for {
		line := <-ch
		producer, topic, msg, err := verify(line)
		if err == nil {
			go publish(producer, topic, msg)
		} else {
			log.Println(string(line))
		}
	}
}

// gloal variable
var PRODUCERS map[string]*sarama.Producer
var TV syscall.Timeval
var CONFIG *jconfig.Config

// main function
func main() {

  args := os.Args
  if args == nil || len(args) < 2 {
    fmt.Println("Usage: tcp2kafka ./default.json")
    return
  }

	const Compiler = "gc"
	const GOARCH string = "amd64"
	const GOOS string = "linux"

	runtime.GOMAXPROCS(runtime.NumCPU())

	CONFIG = jconfig.LoadConfig(args[1])

	port := CONFIG.GetString("port")
	if port == "" {
		port = "8090"
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	err = initProducers(CONFIG)
	if err != nil {
		log.Println(err)
		return
	}

	for _, p := range PRODUCERS {
		go dumpErrors(p)
	}

	// 定长阻塞队列
	msgChan := make(chan []byte, 100000)
	defer close(msgChan)

	// 初始化发送routine
	for i := 0; i < runtime.NumCPU(); i++ {
		go publishMsg(msgChan)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handle(conn, msgChan)
	}
}

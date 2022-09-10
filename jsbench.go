package main

import (
	"flag"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

var (
	natsAddress string
	subject     string
)

func subscriber(done chan struct{}) {
	nc, err := nats.Connect(natsAddress, nats.Name("sub"))
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalln(err)
	}
	sub, err := js.Subscribe(subject, func(msg *nats.Msg) {
		var tm time.Time
		err := tm.UnmarshalBinary(msg.Data)
		if err != nil {
			log.Fatalln("Could no deseialize message", err)
		}
		log.Println("latency", time.Since(tm))
		close(done)
	})
	defer sub.Unsubscribe()
	<-done
}

func main() {
	var streamName string
	flag.StringVar(&natsAddress, "s", nats.DefaultURL, "nats server address")
	flag.StringVar(&subject, "subject", "testsubject", "subject to publish to")
	flag.StringVar(&streamName, "stream", "teststream", "subject to publish to")
	replicas := flag.Int("replicas", 1, "replication factor")
	flag.Parse()

	log.SetFlags(log.Lshortfile)
	nc, err := nats.Connect(natsAddress, nats.Name("pub"))
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalln(err)
	}
	js.DeleteStream(streamName)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Replicas: *replicas,
		Storage:  nats.FileStorage,
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer js.DeleteStream(streamName)

	done := make(chan struct{})
	go subscriber(done)
	err = sendMsg(js)
	if err != nil {
		log.Fatalln(err)
	}

	<-done
}

func sendMsg(js nats.JetStreamContext) error {
	now := time.Now()
	msg, _ := now.MarshalBinary()
	_, err := js.Publish(subject, msg)
	return err
}

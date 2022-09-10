package main

import (
	"context"
	"flag"
	"github.com/nats-io/nats.go"
	"log"
	"strconv"
	"sync"
	"time"
)

var (
	natsAddress  string
	replicas     int
	streamPrefix string
)

func streamName(i int) string {
	return streamPrefix + strconv.Itoa(i)
}

func subscriber(ctx context.Context, done chan struct{}, subject string) {
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
			log.Fatalln("Could no unmarshal message", err)
		}
		log.Println("latency", time.Since(tm))
		close(done)
	})
	defer sub.Unsubscribe()
	<-done
	<-ctx.Done()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var subject string
	flag.StringVar(&natsAddress, "s", nats.DefaultURL, "nats server address")
	flag.StringVar(&subject, "subject", "testsubject", "subject to publish to")
	flag.StringVar(&streamPrefix, "stream", "teststream", "subject to publish to")
	flag.IntVar(&replicas, "replicas", 3, "replication factor")
	numStreams := flag.Int("n", 1, "number of streams")
	delay := flag.Duration("delay", time.Second, "delay between stream creations")
	flag.Parse()

	ticker := time.NewTicker(*delay)
	i := 0
	wg := new(sync.WaitGroup)
	wg.Add(*numStreams)
	for range ticker.C {
		if i == *numStreams {
			break
		}

		go do(ctx, wg, streamName(i))
		i++
	}

	deleteAllStreams(*numStreams)
}

func deleteAllStreams(n int) {
	nc, err := nats.Connect(natsAddress, nats.Name("deleter"))
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalln(err)
	}
	for i := 0; i < n; i++ {
		js.DeleteStream(streamName(n))
	}
}

func do(ctx context.Context, wg *sync.WaitGroup, streamName string) {
	defer wg.Done()
	nc, err := nats.Connect(natsAddress, nats.Name("pub_"+streamName))
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
		Subjects: []string{streamName},
		Replicas: replicas,
		Storage:  nats.FileStorage,
	})
	if err != nil {
		log.Fatalln(err)
	}

	done := make(chan struct{})
	go subscriber(ctx, done, streamName)
	err = sendMsg(js, streamName)
	if err != nil {
		log.Fatalln(err)
	}

	<-done
}

func sendMsg(js nats.JetStreamContext, subject string) error {
	now := time.Now()
	msg, _ := now.MarshalBinary()
	_, err := js.Publish(subject, msg)
	return err
}

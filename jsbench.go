package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"jseg/histogram"
	"log"
	"strconv"
	"sync"
	"time"
)

var (
	hist         = histogram.New()
	natsAddress  string
	replicas     int
	streamPrefix string
	quiet        bool
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
		latency := time.Since(tm)
		if !quiet {
			log.Println(subject, "latency", latency)
		}
		hist.Add(latency)
		close(done)
	})
	defer sub.Unsubscribe()
	<-done
	<-ctx.Done()
}

func main() {
	log.SetFlags(log.Lshortfile)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var subject string
	flag.StringVar(&natsAddress, "s", nats.DefaultURL, "nats server address")
	flag.StringVar(&subject, "subject", "testsubject", "subject to publish to")
	flag.StringVar(&streamPrefix, "stream", "teststream", "subject to publish to")
	flag.IntVar(&replicas, "replicas", 3, "replication factor")
	flag.BoolVar(&quiet, "q", false, "supress logging of individual latencies")
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
	wg.Wait()
	fmt.Println(hist)
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
		log.Println("failed to create stream", err)
		return
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

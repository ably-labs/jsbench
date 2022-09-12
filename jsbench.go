package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"jseg/histogram"
	"log"
	"strconv"
	"strings"
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
	nc, err := nats.Connect(natsAddress, nats.Name("sub"), nats.Timeout(2*time.Second))
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
	select {
	case <-done:
	case <-ctx.Done():
		log.Println("Waiting for message", ctx.Err())
		return
	}
	<-ctx.Done()
}

func deleteStreams() {
	nc, err := nats.Connect(natsAddress, nats.Name("deleter"))
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalln(err)
	}
	ch := js.StreamNames()
	for name := range ch {
		if strings.HasPrefix(name, streamPrefix) {
			if !quiet {
				log.Println("deleting stale stream", name)
			}
			err := js.DeleteStream(name)
			if err != nil {
				log.Println("deleting stream", name, err)
			}
		}
	}
}

func main() {
	log.SetFlags(log.Lshortfile)

	var subject string
	flag.StringVar(&natsAddress, "s", nats.DefaultURL, "nats server address")
	flag.StringVar(&subject, "subject", "testsubject", "subject to publish to")
	flag.StringVar(&streamPrefix, "stream", "teststream", "subject to publish to")
	flag.IntVar(&replicas, "replicas", 3, "replication factor")
	flag.BoolVar(&quiet, "q", false, "supress logging of individual latencies")
	numStreams := flag.Int("n", 1, "number of streams")
	delay := flag.Duration("delay", time.Second, "delay between stream creations")
	timeout := flag.Duration("timeout", time.Minute, "timeout for whole run")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	deleteStreams()

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
	if ctx.Err() != nil {
		log.Println(ctx.Err())
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	defer wg.Done()
	nc, err := nats.Connect(natsAddress, nats.Name("pub_"+streamName), nats.Timeout(10*time.Second))
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
	}, nats.Context(ctx))
	if err != nil {
		log.Println("failed to create stream", err)
		return
	}

	done := make(chan struct{})
	go subscriber(ctx, done, streamName)
	err = sendMsg(ctx, js, streamName)
	if err != nil {
		log.Fatalln(err)
	}

	<-done
}

func sendMsg(ctx context.Context, js nats.JetStreamContext, subject string) error {
	now := time.Now()
	msg, _ := now.MarshalBinary()
	_, err := js.Publish(subject, msg, nats.Context(ctx))
	return err
}

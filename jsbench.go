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
	"sync/atomic"
	"time"
)

var (
	hist         = histogram.New()
	natsAddress  string
	replicas     int
	streamPrefix string
	quiet        bool
	waitForSub   bool
	nClients     int
	clients      []nats.JetStreamContext
)

func streamName(i int) string {
	return streamPrefix + strconv.Itoa(i)
}

func subscriber(ctx context.Context, subscribed, done chan struct{}, subject string) {
	js := getClient()
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
	}, nats.DeliverNew())
	if err != nil {
		log.Println("could not subscrive to", subject, err)
		return
	}
	close(subscribed)
	defer sub.Unsubscribe()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		log.Println("10s timeout waiting for read from", subject)
		return
	}
	<-ctx.Done()
}

func deleteStreams() {
	js := getClient()
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

func openClients() {
	clients = make([]nats.JetStreamContext, nClients)
	for i := 0; i < nClients; i++ {
		nc, err := nats.Connect(natsAddress, nats.Name("deleter"))
		if err != nil {
			log.Fatalln(err)
		}
		js, err := nc.JetStream()
		if err != nil {
			log.Fatalln(err)
		}
		clients[i] = js
	}
}

var clientIDx int64

func getClient() nats.JetStreamContext {
	idx := atomic.AddInt64(&clientIDx, 1)
	return clients[int(idx)%len(clients)]
}

func main() {
	log.SetFlags(log.Lshortfile)

	var subject string
	flag.StringVar(&natsAddress, "s", nats.DefaultURL, "nats server address")
	flag.StringVar(&subject, "subject", "testsubject", "subject to publish to")
	flag.StringVar(&streamPrefix, "stream", "teststream", "subject to publish to")
	flag.IntVar(&replicas, "replicas", 3, "replication factor")
	flag.IntVar(&nClients, "nclients", 1, "number of clients to open")
	flag.BoolVar(&quiet, "q", false, "supress logging of individual latencies")
	flag.BoolVar(&waitForSub, "w", false, "wait for subscibe to be initiated before sending messages")
	numStreams := flag.Int("n", 1, "number of streams")
	delay := flag.Duration("delay", time.Second, "delay between stream creations")
	timeout := flag.Duration("timeout", time.Minute, "timeout for whole run")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer func() {
		cancel()
	}()

	openClients()

	deleteStreams()

	ticker := time.NewTicker(*delay)
	i := 0
	wg := new(sync.WaitGroup)
	for range ticker.C {
		if i == *numStreams || ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go do(ctx, wg, streamName(i))
		i++
	}
	wg.Wait()
	fmt.Println(hist)
	deleteAllStreams(*numStreams)
}

func deleteAllStreams(n int) {
	js := getClient()
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
	defer func() {
		cancel()
	}()

	defer wg.Done()
	js := getClient()
	js.PurgeStream(streamName)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName},
		Replicas: replicas,
		Storage:  nats.FileStorage,
	}, nats.Context(ctx))
	if err != nil {
		log.Println("failed to create stream", err)
		return
	}

	subscribed := make(chan struct{})
	done := make(chan struct{})
	subStart := time.Now()
	go subscriber(ctx, subscribed, done, streamName)
	if waitForSub {
		<-subscribed
		if !quiet {
			log.Println("waited for subscription to start", time.Since(subStart))
		}

	}
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

package main

import (
	"bytes"
	"fmt"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

const finalMessage = "should be ok now"

func logSlowConsumer(nc *nats.Conn, sub *nats.Subscription, err error) {
	log.Println("error", sub.Subject, err)
}

func listner(url string, done chan struct{}) {
	nc, err := nats.Connect(url, nats.ErrorHandler(logSlowConsumer))
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()
	log.Println("subscribing to 'greet'")
	sub, err := nc.SubscribeSync("greet")
	if err != nil {
		log.Fatalln(err)
	}
	sub.SetPendingLimits(1, 100)

	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("got %s %s", msg.Subject, msg.Data)

	log.Println("sleeping 10s")
	time.Sleep(1 * time.Second)

	for i := 0; i < 1000; i++ {
		msg, err = sub.NextMsg(time.Second)
		if err != nil {
			log.Println(err)
			continue
		}
		nDropped, _ := sub.Dropped()
		log.Printf("got %s %s, %d dropped", msg.Subject, msg.Data, nDropped)
		if string(msg.Data) == finalMessage {
			break
		}
	}
	close(done)

}

func main() {
	done := make(chan struct{})
	log.SetFlags(log.Lshortfile)
	server, err := natsserver.NewServer(&natsserver.Options{
		Port:         natsserver.RANDOM_PORT,
		PingInterval: 100 * time.Millisecond,
	})
	if err != nil {
		log.Fatalln(err)
	}
	server.Start()
	if !server.ReadyForConnections(10 * time.Millisecond) {
		log.Fatalln("server not ready for connections")
	}
	log.Println(server.ClientURL())
	go listner(server.ClientURL(), done)

	time.Sleep(100 * time.Millisecond)
	nc, err := nats.Connect(server.ClientURL())
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()
	log.Println("sending hello")
	err = nc.Publish("greet", []byte("hello"))
	if err != nil {
		log.Fatalln(err)
	}
	buf := new(bytes.Buffer)
	for i := 0; i < 100; i++ {
		buf.Reset()
		fmt.Fprintf(buf, "will drop %d", i)
		err = nc.Publish("greet", buf.Bytes())
		if err != nil {
			log.Fatalln(err)
		}

	}
	time.Sleep(time.Second)
	err = nc.Publish("greet", []byte(finalMessage))
	if err != nil {
		log.Fatalln(err)
	}

	<-done
}

package main

import (
	"context"
	"github.com/Burmuley/priority-pubsub/internal/processor"
	"github.com/Burmuley/priority-pubsub/internal/queue"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	logInfo = log.New(os.Stdout, "[PRIORITY_PUBSUB][INFO] ", log.LstdFlags|log.Lmsgprefix)
	logErr  = log.New(os.Stderr, "[PRIORITY_PUBSUB][ERROR] ", log.LstdFlags|log.Lmsgprefix)
)

func main() {

	logInfo.Println("Priority Pub/Sub started")
	// TODO: read config from file
	//config := make(map[string]string)

	// TODO: rewrite this manual setup
	// setup queues
	queues := make([]queue.Queue, 0, 2)
	queuesNames := []string{"high-priority", "low-priority"}

	for _, qn := range queuesNames {
		qCfg := queue.SQSConfig{
			Name:              qn,
			VisibilityTimeout: 120,
			Endpoint:          "http://localhost:4566",
			Region:            "us-west-2",
		}
		q, err := queue.NewSQSQueue(qCfg)
		if err != nil {
			logErr.Fatal(err)
		}
		queues = append(queues, q)
	}

	ctx, cancel := context.WithCancel(context.Background())

	procConfig := processor.RawConfig{
		SubscriberUrl: "http://localhost:5000/",
		Method:        "POST",
		Timeout:       240,
		FatalCodes:    []int{450},
	}
	proc, err := processor.NewRaw(procConfig)
	if err != nil {
		logErr.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	go Poller(ctx, wg, queues, proc)
	wg.Add(1)

	for {
		select {
		case sig := <-sigs:
			logErr.Printf("signal received: %s\n", sig)
			logErr.Println("interrupting all jobs")
			cancel()
			wg.Wait()
			logInfo.Println("cancelled all pollers")
			return
		}
	}
}

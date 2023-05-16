/*
 * Copyright 2023. Konstantin Vasilev (burmuley@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"flag"
	"github.com/Burmuley/priority-pubsub/internal/helpers"
	"github.com/Burmuley/priority-pubsub/internal/process"
	"github.com/Burmuley/priority-pubsub/internal/queue"
	koanfjson "github.com/knadh/koanf/parsers/json"
	koanffile "github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	logInfo = log.New(os.Stdout, "[PRIORITY_PUBSUB] [INFO] ", log.LstdFlags|log.Lmsgprefix)
	logErr  = log.New(os.Stderr, "[PRIORITY_PUBSUB] [ERROR] ", log.LstdFlags|log.Lmsgprefix)
)

func main() {
	var queueConfig []any
	var processorConfig any

	logInfo.Println("Priority Pub/Sub started")

	// set config file name from cmd flag "config"
	cfgFlag := flag.String("config", "config.json", "path to the configuration file")
	flag.Parse()
	configFileName := *cfgFlag

	kfg := koanf.New(".")
	cfgFile := koanffile.Provider(configFileName)
	kParser := koanfjson.Parser()
	if err := kfg.Load(cfgFile, kParser); err != nil {
		logErr.Fatalf("error loading configuration file: %s\n", err.Error())
	}

	// reading poller parameters
	pollConfig := PollerConfig{}
	if err := kfg.Unmarshal("poller", &pollConfig); err != nil {
		logErr.Fatalf("error parsing 'poller' configuration: %s", err.Error())
	}

	// getting queues configuration
	qType := kfg.String("queues.type")

	switch qType {
	case "aws_sqs":
		var qConfig []queue.AwsSQSConfig
		if err := kfg.Unmarshal("queues.config", &qConfig); err != nil {
			logErr.Fatalf("error parsing queues configuration: %s\n", err.Error())
		}
		helpers.CopySliceElems(qConfig, &queueConfig)
	case "gcp_pubsub":
		var qConfig []queue.GcpPubSubConfig
		if err := kfg.Unmarshal("queues.config", &qConfig); err != nil {
			logErr.Fatalf("error parsing queues configuration: %s\n", err.Error())
		}
		helpers.CopySliceElems(qConfig, &queueConfig)
	default:
		logErr.Fatal("unknown queue type %s", qType)
	}

	// getting process configuration
	prType := kfg.String("processor.type")

	switch prType {
	case "http_raw":
		prConfig := process.HttpRawConfig{}
		if err := kfg.Unmarshal("process.config", &prConfig); err != nil {
			logErr.Fatalf("error parsing process configuration: %s\n", err.Error())
		}
		processorConfig = prConfig
	default:
		logErr.Fatalf("unknown processor type %s\n", prType)
	}

	procCtx, procCancel := context.WithCancel(context.Background())
	queueCtx, queueCancel := context.WithCancel(context.Background())

	queues := make([]queue.Queue, 0, 2)
	qFab := queue.Fabric{}
	for _, v := range queueConfig {
		q, err := qFab.Get(queueCtx, qType, v)
		if err != nil {
			logErr.Fatalf("error adding queue: %s\n", err.Error())
		}
		queues = append(queues, q)
	}

	prFab := process.Fabric{}
	proc, err := prFab.Get(prType, processorConfig)
	if err != nil {
		logErr.Fatal("error adding processor: %s", err.Error())
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	var pollFunc Poller
	{
		var err error
		if pollFunc, err = PollerFabric(pollConfig.Type); err != nil {
			logErr.Fatalf("error initializing poller: %s", err.Error())
		}
	}

	for i := 0; i < pollConfig.Concurrency; i++ {
		go pollFunc(procCtx, wg, queues, proc)
		wg.Add(1)
	}

	for {
		select {
		case sig := <-signals:
			logInfo.Printf("signal received: %s\n", sig)
			logInfo.Println("interrupting all jobs")
			procCancel()
			wg.Wait()
			queueCancel()
			logInfo.Println("cancelled all pollers")
			return
		}
	}
}

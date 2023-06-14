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

package poll

import (
	"context"
	"fmt"
	"github.com/Burmuley/priority-pubsub/process"
	"github.com/Burmuley/priority-pubsub/queue"
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

type Config struct {
	Type        string `koanf:"type"`
	Concurrency int    `koanf:"concurrency"`
}

type LaunchConfig struct {
	Queues          []queue.Queue
	Poller          Poller
	Processor       process.Processor
	QueueCancelFunc context.CancelFunc
	Concurrency     int
}

type Poller func(ctx context.Context, wg *sync.WaitGroup, queues []queue.Queue, proc process.Processor)

func New(poller string) (Poller, error) {
	switch poller {
	case "simple":
		return SimplePoller, nil
	}

	return nil, fmt.Errorf("no such Poller function %q", poller)
}

func Run(cfg LaunchConfig) {
	wg := &sync.WaitGroup{}
	prCtx, prCancel := context.WithCancel(context.Background())

	for i := 0; i < cfg.Concurrency; i++ {
		go cfg.Poller(prCtx, wg, cfg.Queues, cfg.Processor)
		wg.Add(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-signals:
			logInfo.Printf("signal received: %s\n", sig)
			logInfo.Println("interrupting all jobs")
			prCancel()
			wg.Wait()
			cfg.QueueCancelFunc()
			logInfo.Println("cancelled all pollers")
			return
		}
	}
}

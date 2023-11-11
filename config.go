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
	"fmt"
	"github.com/Burmuley/priority-pubsub/poll"
	"github.com/Burmuley/priority-pubsub/process"
	"github.com/Burmuley/priority-pubsub/queue"
	"github.com/Burmuley/priority-pubsub/transform"
	koanfjson "github.com/knadh/koanf/parsers/json"
	koanffile "github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

func getPollLaunchConfig() (*poll.LaunchConfig, error) {
	var queueConfig []any
	var processorConfig any

	// set config file name from cmd flag "config"
	cfgFlag := flag.String("config", "config.json", "path to the configuration file")
	flag.Parse()
	configFileName := *cfgFlag

	kfg := koanf.New(".")
	cfgFile := koanffile.Provider(configFileName)
	kParser := koanfjson.Parser()
	if err := kfg.Load(cfgFile, kParser); err != nil {
		return nil, fmt.Errorf("error loading configuration file: %w\n", err)
	}

	queueCtx, queueCancel := context.WithCancel(context.Background())

	// reading poller parameters
	pollConfig := poll.Config{}
	if err := kfg.Unmarshal("poller", &pollConfig); err != nil {
		return nil, fmt.Errorf("error parsing 'poller' configuration: %w", err)
	}

	transConfig := transform.Config{}
	if !kfg.Exists("transformer") {
		transConfig.Type = ""
	} else {
		if err := kfg.Unmarshal("transformer", &transConfig); err != nil {
			return nil, fmt.Errorf("error parsing 'transform' configuration: %w", err)
		}
	}

	// getting queues configuration
	qType := kfg.String("queues.type")

	switch qType {
	case "aws_sqs":
		var qConfig []queue.AwsSQSConfig
		if err := kfg.Unmarshal("queues.config", &qConfig); err != nil {
			return nil, fmt.Errorf("error parsing queues configuration: %w\n", err)
		}
		copySliceElems(qConfig, &queueConfig)
	case "gcp_pubsub":
		var qConfig []queue.GcpPubSubConfig
		if err := kfg.Unmarshal("queues.config", &qConfig); err != nil {
			return nil, fmt.Errorf("error parsing queues configuration: %w\n", err)
		}
		copySliceElems(qConfig, &queueConfig)
	default:
		return nil, fmt.Errorf("unknown queue type %s", qType)
	}

	// getting process configuration
	prType := kfg.String("processor.type")

	switch prType {
	case "http":
		prConfig := process.HttpConfig{}
		if err := kfg.Unmarshal("processor.config", &prConfig); err != nil {
			return nil, fmt.Errorf("error parsing process configuration: %w\n", err)
		}
		processorConfig = prConfig
	default:
		return nil, fmt.Errorf("unknown processor type %s\n", prType)
	}

	queues := make([]queue.Queue, 0, 2)
	for _, v := range queueConfig {
		q, err := queue.New(queueCtx, v)
		if err != nil {
			return nil, fmt.Errorf("error adding queue: %w\n", err)
		}
		queues = append(queues, q)
	}

	proc, err := process.New(processorConfig)
	if err != nil {
		return nil, fmt.Errorf("error adding processor: %w", err)
	}

	var pollFunc poll.Poller
	{
		var err error
		if pollFunc, err = poll.New(pollConfig.Type); err != nil {
			return nil, fmt.Errorf("error initializing poller: %w", err)
		}
	}

	var transFunc transform.TransformationFunc
	{
		var err error
		if transFunc, err = transform.New(transConfig.Type); err != nil {
			return nil, err
		}
	}

	return &poll.LaunchConfig{
		Queues:          queues,
		Poller:          pollFunc,
		Processor:       proc,
		TransformFunc:   transFunc,
		QueueCancelFunc: queueCancel,
		Concurrency:     pollConfig.Concurrency,
	}, nil
}

func copySliceElems[T any](source []T, target *[]any) {
	for _, v := range source {
		*target = append(*target, v)
	}
}

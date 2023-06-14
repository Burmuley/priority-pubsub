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

package process

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Burmuley/priority-pubsub/helpers"
	"github.com/Burmuley/priority-pubsub/queue"
	"net/http"
	"time"
)

const (
	HttpDaprDefaultHTTPMethod = "POST"
	HttpDaprDefaultTimeout    = 120
)

type snsEnvelope struct {
	Message string `json:"Message"`
}

type HttpDaprConfig struct {
	SubscriberUrl string `koanf:"subscriber_url"`
	Method        string `koanf:"method"`
	Timeout       int    `koanf:"timeout"`
	FatalCodes    []int  `koanf:"fatal_codes"`
}

type HttpDapr struct {
	config HttpDaprConfig
}

func NewHttpDapr(config HttpDaprConfig) (*HttpDapr, error) {
	if config.Method == "" {
		config.Method = HttpDaprDefaultHTTPMethod
	}

	supMethods := []string{"GET", "POST"}

	if !helpers.ItemInSlice(config.Method, supMethods) {
		return nil, fmt.Errorf("%w: method %q is not supported", ErrConfig, config.Method)
	}

	if config.SubscriberUrl == "" {
		return nil, fmt.Errorf("%w: field 'SubscriberUrl' is mandatory", ErrConfig)
	}

	if config.Timeout == 0 {
		config.Timeout = HttpDaprDefaultTimeout
	}

	dapr := &HttpDapr{
		config: config,
	}

	return dapr, nil
}

func (d *HttpDapr) Run(ctx context.Context, msg queue.Message) error {
	resChan := make(chan error)

	// parse SNS envelope
	snsData := &snsEnvelope{}
	err := json.Unmarshal(msg.Data(), snsData)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFatal, err)
	}

	go func() {
		client := http.Client{
			Timeout: time.Duration(d.config.Timeout) * time.Second,
		}

		req, err := http.NewRequestWithContext(ctx, d.config.Method, d.config.SubscriberUrl, bytes.NewBuffer([]byte(snsData.Message)))
		if err != nil {
			resChan <- fmt.Errorf("%w: %q", ErrFatal, err.Error())
			close(resChan)
			return
		}
		req.Header.Add("content-type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			resChan <- fmt.Errorf("%w: %w", ErrFail, err)
			close(resChan)
			return
		}

		if helpers.ItemInSlice(resp.StatusCode, d.config.FatalCodes) {
			resChan <- fmt.Errorf("%w: response status code %q", ErrFatal, resp.StatusCode)
			close(resChan)
			return
		}

		if resp.StatusCode > 200 && resp.StatusCode < 300 {
			resChan <- fmt.Errorf("%w: task execution has failed", ErrFail)
			close(resChan)
			return
		}

		resChan <- nil
		close(resChan)
	}()

	for {
		select {
		case res := <-resChan:
			return res
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(5 * time.Second)
		}
	}
}

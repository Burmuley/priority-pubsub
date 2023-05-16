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
	"fmt"
	"github.com/Burmuley/priority-pubsub/internal/helpers"
	"github.com/Burmuley/priority-pubsub/internal/queue"
	"net/http"
	"time"
)

const (
	RawDefaultHTTPMethod = "POST"
	RawDefaultTimeout    = 120
)

type HttpRawConfig struct {
	SubscriberUrl string `koanf:"subscriber_url"`
	Method        string `koanf:"method"`
	Timeout       int    `koanf:"timeout"`
	FatalCodes    []int  `koanf:"fatal_codes"`
}

type HttpRaw struct {
	config HttpRawConfig
}

func NewHttpRaw(config HttpRawConfig) (*HttpRaw, error) {
	if config.Method == "" {
		config.Method = RawDefaultHTTPMethod
	}

	supMethods := []string{"GET", "POST"}

	if !helpers.ItemInSlice(config.Method, supMethods) {
		return nil, fmt.Errorf("%w: method %q is not supported", ErrConfig, config.Method)
	}

	if config.SubscriberUrl == "" {
		return nil, fmt.Errorf("%w: field 'SubscriberUrl' is mandatory", ErrConfig)
	}

	if config.Timeout == 0 {
		config.Timeout = RawDefaultTimeout
	}

	raw := &HttpRaw{
		config: config,
	}

	return raw, nil
}

func (r *HttpRaw) Run(ctx context.Context, msg queue.Message) error {
	client := http.Client{
		Timeout: time.Duration(r.config.Timeout) * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, r.config.Method, r.config.SubscriberUrl, bytes.NewBuffer(msg.Data()))
	if err != nil {
		return fmt.Errorf("%w: %q", ErrFatal, err.Error())
	}

	resChan := make(chan error)

	go func() {
		resp, err := client.Do(req)
		if err != nil {
			resChan <- fmt.Errorf("%w: %q", ErrFail, err.Error())
			close(resChan)
			return
		}

		if helpers.ItemInSlice(resp.StatusCode, r.config.FatalCodes) {
			resChan <- fmt.Errorf("%w: response status code %q", ErrFatal, resp.StatusCode)
			close(resChan)
			return
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
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

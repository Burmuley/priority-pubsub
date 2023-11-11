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
	"github.com/Burmuley/priority-pubsub/queue"
	"github.com/Burmuley/priority-pubsub/transform"
	"net/http"
	"slices"
	"time"
)

const (
	HttpDefaultMethod      = "POST"
	HttpDefaultTimeout     = 120
	HttpDefaultContentType = "text/plain"
)

type HttpConfig struct {
	SubscriberUrl string `koanf:"subscriber_url"`
	Method        string `koanf:"method"`
	Timeout       int    `koanf:"timeout"`
	FatalCodes    []int  `koanf:"fatal_codes"`
	ContentType   string `koanf:"content_type"`
}

type Http struct {
	config HttpConfig
}

func NewHttp(config HttpConfig) (*Http, error) {
	if config.Method == "" {
		config.Method = HttpDefaultMethod
	}

	supMethods := []string{"GET", "POST"}

	if !slices.Contains(supMethods, config.Method) {
		return nil, fmt.Errorf("%w: method %q is not supported", ErrConfig, config.Method)
	}

	if config.SubscriberUrl == "" {
		return nil, fmt.Errorf("%w: field 'subscriber_url' is mandatory", ErrConfig)
	}

	if config.Timeout == 0 {
		config.Timeout = HttpDefaultTimeout
	}

	if config.ContentType == "" {
		config.ContentType = HttpDefaultContentType
	}

	raw := &Http{
		config: config,
	}

	return raw, nil
}

func (r *Http) Run(ctx context.Context, msg queue.Message, trans transform.TransformationFunc) error {
	resChan := make(chan error)
	data := msg.Data()

	{
		var err error
		if trans != nil {
			data, err = trans(data)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrFatal, err)
			}
		}
	}

	go func() {
		client := http.Client{
			Timeout: time.Duration(r.config.Timeout) * time.Second,
		}

		req, err := http.NewRequestWithContext(ctx, r.config.Method, r.config.SubscriberUrl, bytes.NewBuffer(data))
		if err != nil {
			resChan <- fmt.Errorf("%w: %q", ErrFatal, err.Error())
			close(resChan)
			return
		}
		req.Header.Add("content-type", r.config.ContentType)

		resp, err := client.Do(req)
		if err != nil {
			resChan <- fmt.Errorf("%w: %q", ErrFail, err.Error())
			close(resChan)
			return
		}
		req.Header.Add("content-type", r.config.ContentType)

		if slices.Contains(r.config.FatalCodes, resp.StatusCode) {
			resChan <- fmt.Errorf("%w: response status code %q", ErrFatal, resp.StatusCode)
			close(resChan)
			return
		}

		if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
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

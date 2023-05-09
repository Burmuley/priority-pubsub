package processor

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

type RawConfig struct {
	SubscriberUrl string
	Method        string
	Timeout       int
	FatalCodes    []int
}

type Raw struct {
	config RawConfig
}

func NewRaw(config RawConfig) (*Raw, error) {
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

	raw := &Raw{
		config: config,
	}

	return raw, nil
}

func (r *Raw) Run(ctx context.Context, msg queue.Message) error {
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

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

package poll_test

import (
	"context"
	"github.com/Burmuley/priority-pubsub/process"
	"github.com/Burmuley/priority-pubsub/queue"
	"github.com/Burmuley/priority-pubsub/transform"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Burmuley/priority-pubsub/poll"
)

type mockQueue struct{}

func (mq *mockQueue) QueueId() string {
	return ""
}

func (mq *mockQueue) ReceiveMessage() (queue.Message, error) {
	return nil, nil
}

func (mq *mockQueue) DeleteMessage(m queue.Message) error {
	return nil
}

func (mq *mockQueue) ReturnMessage(m queue.Message) error {
	return nil
}

func TestNew(t *testing.T) {
	t.Run("valid poller name", func(t *testing.T) {
		poller, err := poll.New("simple")
		if err != nil {
			t.Errorf("Expected no error but got %v", err)
		}
		if poller == nil {
			t.Error("Expected a Poller function but got nil")
		}
	})

	t.Run("invalid poller name", func(t *testing.T) {
		poller, err := poll.New("fake")
		if err == nil {
			t.Error("Expected an error but got nil")
		}
		if poller != nil {
			t.Error("Expected a nil Poller function but got a non-nil value")
		}
	})
}

func TestRun(t *testing.T) {

	// Define a mock poller function
	mockPoller := func(ctx context.Context, wg *sync.WaitGroup, queues []queue.Queue, proc process.Processor, trans transform.TransformationFunc) {
	}

	// Define a LaunchConfig with a mock queue and a mock poller
	cfg := poll.LaunchConfig{
		Queues:        []queue.Queue{&mockQueue{}},
		Poller:        mockPoller,
		Processor:     nil,
		TransformFunc: nil,
		Concurrency:   1,
	}

	t.Run("interrupt signal received", func(t *testing.T) {
		// Start the Run function in a separate goroutine
		go poll.Run(cfg)

		// Send an interrupt signal to the process
		p, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Fatalf("Failed to find the current process: %v", err)
		}
		err = p.Signal(syscall.SIGTERM)
		if err != nil {
			t.Fatalf("Failed to send signal to current process: %v", err)
		}

		// Wait for up to 1 second for the function to return
		select {
		case <-time.After(time.Second):
			t.Error("Expected Run function to return immediately after receiving SIGTERM signal")
		}
	})

	t.Run("start pollers", func(t *testing.T) {
		// Start the Run function in a separate goroutine
		go poll.Run(cfg)

		// Wait for up to 1 second for the poller to start
		select {
		case <-time.After(time.Second):
			// Check that the poller was started
			if len(cfg.Queues) != 1 {
				t.Error("Expected poller to start polling queue")
			}
		}
	})
}

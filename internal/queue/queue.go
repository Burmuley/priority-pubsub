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

package queue

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNoMessages = errors.New("no messages received")
	ErrNewQueue   = errors.New("error creating new queue instance")
	ErrConfig     = errors.New("queue configuration error")
	ErrReceiveMsg = errors.New("error when receiving message")
	ErrReturnMsg  = errors.New("error when returning message")
	ErrDeleteMsg  = errors.New("error when deleting message")
)

type Message interface {
	Id() string
	QueueId() string
	Data() []byte
}

type Queue interface {
	QueueId() string
	ReceiveMessage() (Message, error)
	DeleteMessage(m Message) error
	ReturnMessage(m Message) error
}

type Fabric struct{}

func (f Fabric) Get(ctx context.Context, qType string, config any) (Queue, error) {
	switch qType {
	case "aws_sqs":
		cfg, ok := config.(AwsSQSConfig)
		if !ok {
			return nil, fmt.Errorf("%w: AwsSQSConfig value expected", ErrConfig)
		}
		return NewSQSQueue(ctx, cfg)
	case "gcp_pubsub":
		cfg, ok := config.(GcpPubSubConfig)
		if !ok {
			return nil, fmt.Errorf("%w: GcpPubSubConfig value expected", ErrConfig)
		}
		return NewGcpPubSubQueue(ctx, cfg)
	}

	return nil, fmt.Errorf("queue type %s is not supported", qType)
}

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
	pubsub "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"strings"
	"time"
)

var (
	GcpPubSubDefaultAckDeadline = 60 * time.Second
)

type GcpPubSubConfig struct {
	SubscriptionId     string `koanf:"subscription_id"`
	AckDeadlineSeconds int64  `koanf:"ack_deadline_seconds"`
	Endpoint           string `koanf:"endpoint"`
}

type GcpPubSubMessage struct {
	messageId      string
	subscriptionId string
	ackId          string
	data           []byte
}

func (gm GcpPubSubMessage) Id() string {
	return gm.messageId
}

func (gm GcpPubSubMessage) QueueId() string {
	return gm.subscriptionId
}

func (gm GcpPubSubMessage) Data() []byte {
	return gm.data
}

type GcpPubSubQueue struct {
	subscriptionId string
	projectId      string
	client         *pubsub.SubscriberClient
	ackDeadline    int64
	context        context.Context
}

func NewGcpPubSubQueue(ctx context.Context, config GcpPubSubConfig) (Queue, error) {
	var err error

	if config.SubscriptionId == "" {
		return nil, fmt.Errorf("%w: parameter 'GcpPubSubConfig.SubscriptionId' is mandatory", ErrConfig)
	}

	if config.AckDeadlineSeconds == 0 {
		config.AckDeadlineSeconds = int64(GcpPubSubDefaultAckDeadline.Seconds())
	}

	q := &GcpPubSubQueue{
		subscriptionId: config.SubscriptionId,
		ackDeadline:    config.AckDeadlineSeconds,
		projectId:      strings.Split(config.SubscriptionId, "/")[1],
	}

	opts := make([]option.ClientOption, 0)
	if config.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(config.Endpoint))
	}

	q.client, err = pubsub.NewSubscriberClient(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNewQueue, err)
	}

	q.subscriptionId = config.SubscriptionId
	q.context = ctx
	return q, nil
}

func (gq *GcpPubSubQueue) QueueId() string {
	return gq.subscriptionId
}

func (gq *GcpPubSubQueue) ReceiveMessage() (Message, error) {
	req := &pubsubpb.PullRequest{
		Subscription: gq.subscriptionId,
		MaxMessages:  1,
	}

	res, err := gq.client.Pull(gq.context, req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReceiveMsg, err)
	}

	if len(res.ReceivedMessages) == 0 {
		return nil, ErrNoMessages
	}

	gcpMsg := res.ReceivedMessages[0]

	msg := &GcpPubSubMessage{
		subscriptionId: gq.subscriptionId,
		messageId:      gcpMsg.Message.MessageId,
		ackId:          gcpMsg.AckId,
		data:           gcpMsg.Message.Data,
	}

	// extend Ack Deadline for the message
	ackReq := &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       gq.subscriptionId,
		AckIds:             []string{msg.ackId},
		AckDeadlineSeconds: int32(gq.ackDeadline),
	}

	if err := gq.client.ModifyAckDeadline(gq.context, ackReq); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReceiveMsg, err)
	}

	return msg, nil
}

func (gq *GcpPubSubQueue) DeleteMessage(m Message) error {
	msg, ok := m.(*GcpPubSubMessage)
	if !ok {
		return fmt.Errorf("%w: expected *GcpPubSubMessage object", ErrDeleteMsg)
	}

	err := gq.client.Acknowledge(gq.context, &pubsubpb.AcknowledgeRequest{
		Subscription: gq.subscriptionId,
		AckIds:       []string{msg.ackId},
	})

	if err != nil {
		return fmt.Errorf("%w: %w", ErrDeleteMsg, err)
	}

	return nil
}

func (gq *GcpPubSubQueue) ReturnMessage(m Message) error {
	msg, ok := m.(*GcpPubSubMessage)
	if !ok {
		return fmt.Errorf("%w: expected *GcpPubSubMessage object", ErrDeleteMsg)
	}

	ackReq := &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       gq.subscriptionId,
		AckIds:             []string{msg.ackId},
		AckDeadlineSeconds: 0,
	}

	if err := gq.client.ModifyAckDeadline(gq.context, ackReq); err != nil {
		return fmt.Errorf("%w: %w", ErrReturnMsg, err)
	}

	return nil
}

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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	AWSSQSDefaultVisibilityTimeout = 30
)

type AwsSQSConfig struct {
	Name              string `koanf:"name"`
	VisibilityTimeout int64  `koanf:"visibility_timeout"`
	Endpoint          string `koanf:"endpoint"`
	Region            string `koanf:"region"`
}

type AwsSQSMessage struct {
	messageId     string
	receiptHandle string
	queueName     string
	data          []byte
}

func (m AwsSQSMessage) Id() string {
	return m.messageId
}

func (m AwsSQSMessage) QueueId() string {
	return m.queueName
}

func (m AwsSQSMessage) Data() []byte {
	return m.data
}

type AwsSQSQueue struct {
	queueName         string
	queueUrl          string
	session           *session.Session
	visibilityTimeout int64
}

func NewSQSQueue(_ context.Context, config AwsSQSConfig) (Queue, error) {
	if config.Name == "" {
		return nil, fmt.Errorf("%w: parameter 'AwsSQSConfig.Name' is mandatory", ErrConfig)
	}

	if config.VisibilityTimeout == 0 {
		config.VisibilityTimeout = AWSSQSDefaultVisibilityTimeout
	}

	awsCfg := &aws.Config{
		Endpoint: nil,
		Region:   nil,
	}

	if config.Endpoint != "" {
		awsCfg.Endpoint = &config.Endpoint
	}

	if config.Region != "" {
		awsCfg.Region = &config.Region
	}

	sess := session.Must(session.NewSession(awsCfg))

	q := &AwsSQSQueue{
		queueName:         config.Name,
		visibilityTimeout: config.VisibilityTimeout,
		session:           sess,
	}

	if err := q.refreshQueueUrl(); err != nil {
		return nil, err
	}

	return q, nil
}

func (s *AwsSQSQueue) QueueId() string {
	return s.queueName
}

func (s *AwsSQSQueue) ReceiveMessage() (Message, error) {
	svc := sqs.New(s.session)
	msgNum := int64(1)

	output, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: &msgNum,
		QueueUrl:            &s.queueUrl,
		VisibilityTimeout:   &s.visibilityTimeout,
	})

	if err != nil {
		return nil, err
	}

	if len(output.Messages) < 1 {
		return nil, ErrNoMessages
	}

	sqsMsg := output.Messages[0]

	msg := &AwsSQSMessage{
		messageId:     *sqsMsg.MessageId,
		receiptHandle: *sqsMsg.ReceiptHandle,
		queueName:     s.queueName,
		data:          []byte(*sqsMsg.Body),
	}

	return msg, nil
}

func (s *AwsSQSQueue) DeleteMessage(m Message) error {
	msg, ok := m.(*AwsSQSMessage)
	if !ok {
		return errors.New("message should be of type AwsSQSMessage")
	}

	svc := sqs.New(s.session)

	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &s.queueUrl,
		ReceiptHandle: &msg.receiptHandle,
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *AwsSQSQueue) ReturnMessage(m Message) error {
	msg, ok := m.(*AwsSQSMessage)
	if !ok {
		return errors.New("message should be of type AwsSQSMessage")
	}

	svc := sqs.New(s.session)
	visTimeout := int64(0)

	_, err := svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &s.queueUrl,
		ReceiptHandle:     &msg.receiptHandle,
		VisibilityTimeout: &visTimeout,
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *AwsSQSQueue) refreshQueueUrl() error {
	svc := sqs.New(s.session)

	output, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: &s.queueName})
	if err != nil {
		return err
	}

	s.queueUrl = *output.QueueUrl
	return nil
}

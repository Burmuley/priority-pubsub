package queue

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	SQSDefaultVisibilityTimeout = 30
)

type SQSConfig struct {
	Name              string
	VisibilityTimeout int64
	Endpoint          string
	Region            string
}

type SQSMessage struct {
	messageId     string
	receiptHandle string
	queueName     string
	data          []byte
}

func (m SQSMessage) Id() string {
	return m.messageId
}

func (m SQSMessage) QueueId() string {
	return m.queueName
}

func (m SQSMessage) Data() []byte {
	return m.data
}

type SQSQueue struct {
	queueName         string
	queueUrl          string
	session           *session.Session
	visibilityTimeout int64
}

func NewSQSQueue(config SQSConfig) (Queue, error) {
	if config.Name == "" {
		return nil, fmt.Errorf("%w: parameter 'SQSConfig.Name' is mandatory", ErrConfig)
	}

	if config.VisibilityTimeout == 0 {
		config.VisibilityTimeout = SQSDefaultVisibilityTimeout
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

	q := &SQSQueue{
		queueName:         config.Name,
		visibilityTimeout: config.VisibilityTimeout,
		session:           sess,
	}

	if err := q.refreshQueueUrl(); err != nil {
		return nil, err
	}

	return q, nil
}

func (s *SQSQueue) QueueId() string {
	return s.queueName
}

func (s *SQSQueue) ReceiveMessage() (Message, error) {
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

	msg := &SQSMessage{
		messageId:     *sqsMsg.MessageId,
		receiptHandle: *sqsMsg.ReceiptHandle,
		queueName:     s.queueName,
		data:          []byte(*sqsMsg.Body),
	}

	return msg, nil
}

func (s *SQSQueue) DeleteMessage(m Message) error {
	msg, ok := m.(*SQSMessage)
	if !ok {
		return errors.New("message should be of type SQSMessage")
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

func (s *SQSQueue) ReturnMessage(m Message) error {
	msg, ok := m.(*SQSMessage)
	if !ok {
		return errors.New("message should be of type SQSMessage")
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

func (s *SQSQueue) refreshQueueUrl() error {
	svc := sqs.New(s.session)

	output, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: &s.queueName})
	if err != nil {
		return err
	}

	s.queueUrl = *output.QueueUrl
	return nil
}

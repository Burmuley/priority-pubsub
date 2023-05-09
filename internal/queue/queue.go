package queue

import (
	"fmt"
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

func (f Fabric) Get(qType string, config any) (Queue, error) {
	switch qType {
	case "sqs":
		cfg, ok := config.(SQSConfig)
		if !ok {
			return nil, fmt.Errorf("%w: SQSConfig value expected", ErrConfig)
		}
		return NewSQSQueue(cfg)
	}

	return nil, fmt.Errorf("queue type %s is not supported", qType)
}

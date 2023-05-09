package processor

import (
	"github.com/Burmuley/priority-pubsub/internal/queue"
)

type Dapr struct {
	subscriberUrl string
	method        string
	timeout       int64
}

func NewDapr(config map[string]string) (*Dapr, error) {
	// TODO: implement me!
	return nil, nil
}

func (d *Dapr) Run(msg queue.Message) error {

	//TODO implement me
	panic("implement me")
}

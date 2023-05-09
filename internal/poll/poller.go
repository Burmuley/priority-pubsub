package poll

import (
	"github.com/Burmuley/priority-pubsub/internal/processor"
	"github.com/Burmuley/priority-pubsub/internal/queue"
)

type Poller struct {
	Queues    []queue.Queue
	Processor processor.Processor
}

//func (p *Poller)

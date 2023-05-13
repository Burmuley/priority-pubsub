package processor

import (
	"context"
	"errors"
	"fmt"
	"github.com/Burmuley/priority-pubsub/internal/queue"
)

var (
	ErrFail   = errors.New("process failed")
	ErrFatal  = errors.New("process end with fatal error")
	ErrConfig = errors.New("configuration error")
)

type Processor interface {
	Run(ctx context.Context, msg queue.Message) error
}

type Fabric struct{}

func (f Fabric) Get(prType string, config any) (Processor, error) {
	switch prType {
	case "http_raw":
		cfg, ok := config.(HttpRawConfig)
		if !ok {
			return nil, fmt.Errorf("%w: HttpRawConfig value expected", ErrConfig)
		}
		return NewHttpRaw(cfg)
	}

	return nil, fmt.Errorf("queue type %s is not supported", prType)
}

package processor

import (
	"context"
	"errors"
	"github.com/Burmuley/priority-pubsub/internal/queue"
)

var (
	ErrFail   error = errors.New("process failed")
	ErrFatal  error = errors.New("process end with fatal error")
	ErrConfig error = errors.New("configuration error")
)

type Processor interface {
	Run(ctx context.Context, msg queue.Message) error
}

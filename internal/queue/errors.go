package queue

import "errors"

var (
	ErrNoMessages error = errors.New("no messages received")
	ErrNewQueue   error = errors.New("error creating new queue instance")
	ErrConfig     error = errors.New("queue configuration error")
	//ErrFormat
)

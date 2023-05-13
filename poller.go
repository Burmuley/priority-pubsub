package main

import (
	"context"
	"errors"
	"github.com/Burmuley/priority-pubsub/internal/processor"
	"github.com/Burmuley/priority-pubsub/internal/queue"
	"sync"
	"time"
)

func Poller(ctx context.Context, wg *sync.WaitGroup, queues []queue.Queue, proc processor.Processor) {
	var message queue.Message
	var procErr, err error

	queueNames := make(map[string]queue.Queue)
	for _, q := range queues {
		queueNames[q.QueueId()] = q
	}

	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		default:
			message = nil
			message, err = receiveMessage(queues)
			if err != nil {
				if errors.Is(err, queue.ErrNoMessages) {
					logInfo.Println("no messages received from all queues")
					time.Sleep(2 * time.Second)
					continue
				}

				logErr.Printf("error while polling for messages: %s\n", err.Error())
				time.Sleep(5 * time.Second)
				continue
			}

			logInfo.Printf("got message %q from %q\n", message.Id(), message.QueueId())
			logInfo.Printf("processing message %q from %q\n", message.Id(), message.QueueId())

			messageQueue := queueNames[message.QueueId()]
			procErr = proc.Run(ctx, message)

			if procErr != nil {
				if errors.As(err, &processor.ErrFatal) {
					logErr.Printf("fatal error occurred during task processing: %s\n", procErr.Error())
					if err := messageQueue.DeleteMessage(message); err != nil {
						logErr.Printf("error deleting message %q to the queue %q: %s\n", message.Id(), message.QueueId(), err)
						message = nil
						continue
					}

					logInfo.Printf("successfully deleted message %q from the queue %q\n", message.Id(), message.QueueId())
					message = nil
					continue
				}

				logErr.Printf("message %q processing error: %s\n", message.Id(), procErr.Error())
				if err := messageQueue.ReturnMessage(message); err != nil {
					logErr.Printf("error returning message %q to the queue %q: %s\n", message.Id(), message.QueueId(), err)
					message = nil
					continue
				}

				logInfo.Printf("successfully returned message %q to the queue %q\n", message.Id(), message.QueueId())
				message = nil
				continue
			}

			logInfo.Printf("message %q from %q has been processed successfully\n", message.Id(), message.QueueId())
			if err := messageQueue.DeleteMessage(message); err != nil {
				logErr.Printf("error deleting message %q from the queue %q: %s\n", message.Id(), message.QueueId(), err)
				message = nil
			}

			logInfo.Printf("successfully deleted message %q from the queue %q\n", message.Id(), message.QueueId())
			message = nil
		}
	}
}

func receiveMessage(queues []queue.Queue) (queue.Message, error) {
	for _, q := range queues {
		message, err := q.ReceiveMessage()

		if err != nil {
			if errors.Is(err, queue.ErrNoMessages) {
				continue
			}
			return nil, err
		}

		return message, nil
	}

	return nil, queue.ErrNoMessages
}

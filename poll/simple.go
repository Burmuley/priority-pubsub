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

package poll

import (
	"context"
	"errors"
	"github.com/Burmuley/priority-pubsub/process"
	"github.com/Burmuley/priority-pubsub/queue"
	"github.com/Burmuley/priority-pubsub/transform"
	"sync"
	"time"
)

func SimplePoller(ctx context.Context, wg *sync.WaitGroup, queues []queue.Queue, proc process.Processor, trans transform.TransformationFunc) {
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
			procErr = proc.Run(ctx, message, trans)

			if procErr != nil {
				if errors.Is(procErr, process.ErrFatal) {
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

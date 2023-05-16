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

package process

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

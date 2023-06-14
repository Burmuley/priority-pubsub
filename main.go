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

package main

import (
	"github.com/Burmuley/priority-pubsub/poll"
	"log"
	"os"
)

var (
	logInfo = log.New(os.Stdout, "[PRIORITY_PUBSUB] [INFO] ", log.LstdFlags|log.Lmsgprefix)
	logErr  = log.New(os.Stderr, "[PRIORITY_PUBSUB] [ERROR] ", log.LstdFlags|log.Lmsgprefix)
)

func main() {
	logInfo.Println("Priority Pub/Sub started")

	launchConfig, err := getPollLaunchConfig()
	if err != nil {
		logErr.Fatal(err)
	}

	poll.Run(*launchConfig)
}

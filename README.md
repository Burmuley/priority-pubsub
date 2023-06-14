# Priority Pub/Sub

Service to abstract interaction of your application with message queue while consuming messages from multiple queues
considering queue priority.

The queue residing at the top of the list in the configuration has the highest priority and polled first.
Only when no messages on the current level left `Poller` starts checking other queues further down in the list.

You can define your own `Poller` implementation in the code and implement your own polling logic.

Each `Poller` should consume only one message and block until `Processor` return the result (or error).
With this you can control number of concurrent messages your application can handle in parallel.

Currently supported queues:
* AWS SQS
* GCP Pub/Sub

## Building

```shell
go build -o priority_pubsub .
```

## Usage

```shell
priority_pubsub -config <path to configuration file>
```

The default configuration file name is `config.json`.

## Configuration

### Configuration structure

```json
{
  "poller": {
    "type": "POLLER TYPE",
    "concurrency": "NUMBER OF CONCURRENT POLLERS"
  },
  "queues": {
    "type": "QUEUE TYPE",
    "config": [
      {
        "QUEUE SPECIFIC CONFIG #1"
      },
      {
        "QUEUE SPECIFIC CONFIG #2"
      }
    ]
  },
  "processor": {
    "type": "PROCESSOR TYPE",
    "config": {
      "PROCESSOR SPECIFIC CONFIG"
    }
  }
}
```

Configuration fields for `poller`:
* `type`: type of the `Poller` to use; currently only `simple` option is supported
* `concurrency`: number of concurrent `Poller` instances to run

Configuration fields for `processor`:
* `type` - type of the `Processor` to use for message processing; available values - `http_raw` and `http_dapr`
* `config` - processor specific configuration; currently both available `Processor` implementation support the same options:
   - `subscriber_url` - hte HTTP URL to forward messages for processing
   - `method` - HTTP method to use when submitting message to `subscriber_url`; default - `POST`
   - `timeout` - HTTP timeout to use, i.e. time to wait for message to be processed before failing the operation
   - `fatal_codes` - list of HTTP codes assumed as `Fatal`, i.e. when message should not be returned back to the queue for retry

Configuration fields for `queues`:
* `type` - queue type; currently supported `awssqs` (AWS SQS) and `gcppubsub` (GCP Pub/Sub)
* `config` - list of queue specific configurations; priority counts from the top, i.e. the top first queue definition has the highest priority

Configuration fields for `awssqs` queue type:
* `name` - name of the AWS SQS queue
* `visibility_timeout` - message [visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
   to set when consuming message from the queue
* `endpoint` - custom endpoint to use for interactions with AWS SQS; useful if you're testing with [Local Stack](https://localstack.cloud)
* `region` - AWS [Region](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html)

Configuration fields for `gcppubsub` queue type:
* `subscription_id` - the ID of the GCP Pub/Sub [subscription](https://cloud.google.com/pubsub/docs/pull)
* `ack_deadline` - timeout for [ACK](https://cloud.google.com/pubsub/docs/lease-management) for the consumed message (similar to AWS SQS Visibility Timeout)

#### Configuration example for AWS SQS with LocalStack:
```json
{
  "poll_concurrency": 4,
  "queues": {
    "type": "aws_sqs",
    "config": [
      {
        "name": "high-priority",
        "visibility_timeout": 600,
        "endpoint": "http://localhost:4566",
        "region": "us-west-2"
      },
      {
        "name": "low-priority",
        "visibility_timeout": 600,
        "endpoint": "http://localhost:4566",
        "region": "us-west-2"
      }
    ]
  },
  "processor": {
    "type": "http_raw",
    "config": {
      "subscriber_url": "http://localhost:5000/",
      "method": "POST",
      "timeout": 570,
      "fatal_codes": [412, 450]
    }
  }
}
```

#### Configuration example for GCP Pub/Sub:
```json
{
  "poll_concurrency": 4,
  "queues": {
    "type": "gcp_pubsub",
    "config": [
      {
        "subscription_id": "projects/test-project-156022/subscriptions/high-priority-sub",
        "ack_deadline": 600
      },
      {
        "subscription_id": "projects/test-project-156022/subscriptions/low-priority-sub",
        "ack_deadline": 600
      }
    ]
  },
  "processor": {
    "type": "http_raw",
    "config": {
      "subscriber_url": "http://localhost:5000/",
      "method": "POST",
      "timeout": 570,
      "fatal_codes": [412, 450]
    }
  }
}
```

**Note**: I wasn't able to make work lightweight `SubscriptionClient` with Pub/Sub emulator, 
to test it out you need to create reals resources in GCP.

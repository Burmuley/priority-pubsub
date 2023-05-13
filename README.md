# Priority Pub/Sub


## Configuration

AWS SQS with LocalStack example:
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

GCP Pub/Sub example:
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

To be done...

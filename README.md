# RabbitRetry

`RabbitRetry` is a Go library designed to simplify message publishing and processing with RabbitMQ, providing built-in retry mechanisms and error handling. This package abstracts the complexities of setting up message queues, exchanges, and bindings, allowing developers to focus on processing logic.

## Features

- Simplifies RabbitMQ message publishing and consuming.
- Built-in retry mechanisms for message processing.
- Easy-to-use interfaces for publishers and processors.
- Configurable settings for different RabbitMQ setups.

## Installation

To install the package, use:

```bash
go get github.com/sergeyslonimsky/rabbitretry
```
## Usage
### Example: Creating a Publisher
Here is a basic example demonstrating how to create a publisher that sends messages to an exchange with a specific routing key.

```go
package main

import (
    "context"
    "log"
    "github.com/sergeyslonimsky/rabbitretry"
)

type TestStruct struct {
    Val int `json:"val"`
}

type testPublisher struct {
    exchange   string
    routingKey string
    publisher  rabbitretry.Publisher
}

func (p *testPublisher) Publish(ctx context.Context, msg TestStruct) error {
    return p.publisher.Publish(ctx, p.exchange, p.routingKey, msg, rabbitretry.PublishOpts{
        ContentType: "application/json",
    })
}

func main() {
    ctx := context.Background()
    publisher := &testPublisher{
        exchange:   "my-exchange",
        routingKey: "my-routing-key",
        publisher:  rabbitretry.NewPublisher(),
    }

    err := publisher.Publish(ctx, TestStruct{Val: 10})
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
}
```
### Example: Creating a Processor
A processor handles incoming messages from the queue and processes them according to your application logic.

```go
package main

import (
    "context"
    "log"
    "github.com/sergeyslonimsky/rabbitretry"
)

type testProcessor struct{}

func (t *testProcessor) Process(ctx context.Context, msg rabbitretry.Message[TestStruct]) error {
    // Add your processing logic here
    log.Printf("Processing message: %+v", msg.Body)
    return nil
}

func (t *testProcessor) GetName() string {
    return "test.processor"
}

func main() {
    // Setup your server and add processor logic
}
```

package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/furdarius/rabbitroutine"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	conn      *Connector
	publisher rabbitroutine.Publisher
}

func NewPublisher(cfg Config) *Publisher {
	conn := NewConnector(cfg)

	return NewPublisherWithConnector(conn)
}

func NewPublisherWithConnector(conn *Connector) *Publisher {
	pool := rabbitroutine.NewPool(conn.conn)
	ensurePub := rabbitroutine.NewEnsurePublisher(pool)
	pub := rabbitroutine.NewRetryPublisher(
		ensurePub,
		rabbitroutine.PublishMaxAttemptsSetup(16),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(10*time.Millisecond)),
	)

	return &Publisher{
		conn:      conn,
		publisher: pub,
	}
}

type PublishOpts struct {
	ContentType string
	MessageID   string
	AppID       string
	UserID      string
	Priority    uint8
	Type        string
}

func (p *Publisher) Run(ctx context.Context) error {
	return p.conn.connect(ctx)
}

func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, message any, opts PublishOpts) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	return p.publisher.Publish(ctx, exchange, routingKey, amqp.Publishing{
		ContentType: opts.ContentType,
		Priority:    opts.Priority,
		MessageId:   opts.MessageID,
		Type:        opts.Type,
		UserId:      opts.UserID,
		AppId:       opts.AppID,
		Body:        body,
	})
}

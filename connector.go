package mq

import (
	"context"
	"time"

	"github.com/furdarius/rabbitroutine"
)

const (
	defaultReconnectionAttempts uint = 3
	defaultConnectionWaitTime        = 2 * time.Second
)

type connectorOpts struct {
	retriedListener      func(rabbitroutine.Retried)
	dialedListener       func(rabbitroutine.Dialed)
	amqpNotifiedListener func(rabbitroutine.AMQPNotified)
	rabbitRoutineConfig  rabbitroutine.Config
}

type ConnectorOpts func(config *connectorOpts)

func WithConnectorReconnectionOpts(reconnectAttempts uint, wait time.Duration) ConnectorOpts {
	return func(config *connectorOpts) {
		config.rabbitRoutineConfig.ReconnectAttempts = reconnectAttempts
		config.rabbitRoutineConfig.Wait = wait
	}
}

func WithRetriedListener(listener func(rabbitroutine.Retried)) ConnectorOpts {
	return func(config *connectorOpts) {
		config.retriedListener = listener
	}
}

func WithDialedListener(listener func(rabbitroutine.Dialed)) ConnectorOpts {
	return func(config *connectorOpts) {
		config.dialedListener = listener
	}
}

func WithAMQPNotifiedListener(listener func(rabbitroutine.AMQPNotified)) ConnectorOpts {
	return func(config *connectorOpts) {
		config.amqpNotifiedListener = listener
	}
}

type Connector struct {
	config config
	conn   *rabbitroutine.Connector
}

func NewConnector(cfg config, opts ...ConnectorOpts) *Connector {
	defaultOpts := connectorOpts{
		rabbitRoutineConfig: rabbitroutine.Config{
			ReconnectAttempts: defaultReconnectionAttempts,
			Wait:              defaultConnectionWaitTime,
		},
	}

	for _, opt := range opts {
		opt(&defaultOpts)
	}

	conn := rabbitroutine.NewConnector(defaultOpts.rabbitRoutineConfig)

	if defaultOpts.retriedListener != nil {
		conn.AddRetriedListener(defaultOpts.retriedListener)
	}

	if defaultOpts.dialedListener != nil {
		conn.AddDialedListener(defaultOpts.dialedListener)
	}

	if defaultOpts.amqpNotifiedListener != nil {
		conn.AddAMQPNotifiedListener(defaultOpts.amqpNotifiedListener)
	}

	return &Connector{
		config: cfg,
		conn:   conn,
	}
}

func (c *Connector) connect(ctx context.Context) error {
	return c.conn.Dial(ctx, c.config.DSN())
}

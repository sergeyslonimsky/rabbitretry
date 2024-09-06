package mq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mq "github.com/sergeyslonimsky/rabbitretry"
)

func TestConfig_dsn(t *testing.T) {
	t.Parallel()

	cfg := mq.Config{
		Host:     "host",
		Port:     "1234",
		User:     "user",
		Password: "pass",
	}

	assert.Equal(t, "amqp://user:pass@host:1234", cfg.DSN())
}

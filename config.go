package mq

import "fmt"

type config interface {
	DSN() string
}

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
}

func (c Config) DSN() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s", c.User, c.Password, c.Host, c.Port)
}

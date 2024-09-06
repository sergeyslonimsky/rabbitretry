package mq

import "context"

type Message[I any] struct {
	Payload I
}

type Processor[I any] interface {
	Process(context.Context, Message[I]) error
	GetName() string
}

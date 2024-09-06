package mq

import (
	"context"
	"fmt"

	"github.com/furdarius/rabbitroutine"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conn      *Connector
	consumers []rabbitroutine.Consumer
}

func NewServer(cfg Config) *Server {
	return NewServerWithConnector(NewConnector(cfg))
}

func NewServerWithConnector(conn *Connector) *Server {
	return &Server{
		conn: conn,
	}
}

func (s *Server) AddConsumer(consumer rabbitroutine.Consumer) {
	s.consumers = append(s.consumers, consumer)
}

func (s *Server) Run(ctx context.Context) error {
	if len(s.consumers) == 0 {
		return nil
	}

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		if err := s.conn.connect(ctx); err != nil {
			return fmt.Errorf("connect: %w", err)
		}

		return nil
	})

	for _, consumer := range s.consumers {
		g.Go(func() error {
			if err := s.conn.conn.StartConsumer(ctx, consumer); err != nil {
				return fmt.Errorf("connect: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	return nil
}

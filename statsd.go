package statsdpub

import (
	"context"
	"fmt"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/gojekfarm/ziggurat"
	"time"
)

type Client struct {
	client  statsd.Statter
	host    string
	prefix  string
	handler ziggurat.Handler
}

func NewPublisher(opts ...func(c *Client)) *Client {
	c := &Client{}
	for _, opt := range opts {
		opt(c)
	}
	if c.prefix == "" {
		c.prefix = "ziggurat_statsd"
	}
	if c.host == "" {
		c.host = "localhost:8125"
	}
	return c
}

func (s *Client) Run(ctx context.Context) error {
	config := &statsd.ClientConfig{
		Prefix:  s.prefix,
		Address: s.host,
	}
	client, clientErr := statsd.NewClientWithConfig(config)
	if clientErr != nil {
		return clientErr
	}
	s.client = client
	go func() {
		done := ctx.Done()
		<-done
		if s.client != nil {
			s.client.Close()
		}
	}()
	go GoRoutinePublisher(ctx, 10*time.Second, s)
	return nil
}

func (s *Client) constructFullMetricStr(metricName, tags string) string {
	return metricName + "," + tags + "," + "app_name=" + s.prefix
}

func (s *Client) IncCounter(metricName string, value int64, arguments map[string]string) error {
	tags := constructTags(arguments)
	finalMetricName := s.constructFullMetricStr(metricName, tags)

	return s.client.Inc(finalMetricName, value, 1.0)
}

func (s *Client) Gauge(metricName string, value int64, arguments map[string]string) error {
	tags := constructTags(arguments)
	finalMetricName := s.constructFullMetricStr(metricName, tags)
	return s.client.Gauge(finalMetricName, value, 1.0)
}

func (s *Client) PublishHandlerMetrics(handler ziggurat.Handler) ziggurat.Handler {
	return ziggurat.HandlerFunc(func(ctx context.Context, event ziggurat.Event) error {
		route := event.Headers()[ziggurat.HeaderMessageRoute]
		arguments := map[string]string{"route": route}
		startTime := time.Now()
		err := handler.HandleEvent(ctx, event)
		endTime := time.Now()
		diffTimeInMS := endTime.Sub(startTime).Milliseconds()
		var publishErr error
		publishErr = s.Gauge("handler_func_exec_time", diffTimeInMS, arguments)
		switch err {
		case nil:
			publishErr = s.IncCounter("message_processing_failure_skip_count", 1, arguments)
		default:
			publishErr = s.IncCounter("message_processing_success_count", 1, arguments)
		}
		fmt.Errorf("statsd puiblish error: %w", publishErr)
		return err
	})
}

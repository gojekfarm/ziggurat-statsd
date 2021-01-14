package main

import (
	"context"
	"fmt"
	"github.com/gojekfarm/ziggurat"
	"github.com/gojekfarm/ziggurat-statsd/statsd"
	"github.com/gojekfarm/ziggurat/kafka"
)

func main() {
	statsdPub := statsd.NewPublisher()
	kafkaStreams := &kafka.Streams{
		RouteGroup: kafka.RouteGroup{
			"plain-text-log": {
				BootstrapServers: "localhost:9092",
				OriginTopics:     "plain-text-log",
				ConsumerGroupID:  "plain_text_consumer",
				ConsumerCount:    2,
			},
		},
	}
	z := &ziggurat.Ziggurat{}
	handler := statsdPub.PublishHandlerMetrics(ziggurat.HandlerFunc(func(event ziggurat.Event) ziggurat.ProcessStatus {
		fmt.Printf("received event: %s\n", event.Value())
		return ziggurat.ProcessingSuccess
	}))
	z.StartFunc(func(ctx context.Context) {
		statsdPub.Run(ctx)
	})
	<-z.Run(context.Background(), kafkaStreams, handler)
}

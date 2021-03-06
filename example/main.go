package main

import (
	"context"
	"github.com/gojekfarm/ziggurat"
	"github.com/gojekfarm/ziggurat-statsd/statsd"
	"github.com/gojekfarm/ziggurat/kafka"
	"github.com/gojekfarm/ziggurat/logger"
	"github.com/gojekfarm/ziggurat/mw"
	"github.com/gojekfarm/ziggurat/router"
	"time"
)

func main() {
	statsdPub := statsd.NewPublisher()
	l := logger.NewJSONLogger("info")
	kafkaStreams := &kafka.Streams{
		StreamConfig: kafka.StreamConfig{{
			BootstrapServers: "localhost:9092",
			OriginTopics:     "plain-text-log",
			ConsumerGroupID:  "plain_text_consumer",
			ConsumerCount:    2,
			RouteGroup:       "plain-text-log",
		}},
	}
	z := &ziggurat.Ziggurat{}
	r := router.New()
	r.HandleFunc("plain-text-log", func(ctx context.Context, event ziggurat.Event) error {
		time.Sleep(2 * time.Second)
		return nil
	})
	mwLogger := mw.ProcessingStatusLogger{Logger: l}
	h := r.Compose(mwLogger.LogStatus, statsdPub.PublishHandlerMetrics)
	z.StartFunc(func(ctx context.Context) {
		statsdPub.Run(ctx)
	})
	<-z.Run(context.Background(), kafkaStreams, h)
}

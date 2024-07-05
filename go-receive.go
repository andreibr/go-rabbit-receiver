package main

import (
	//"log"
	"time"
	
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	log "github.com/sirupsen/logrus"
	amqp "github.com/rabbitmq/amqp091-go"
)

var now = time.Now()

func failOnError(err error, msg string) {
	log.SetFormatter(&log.JSONFormatter{})
	if err != nil {
		log.WithFields(log.Fields{"time": now.Format(time.RFC3339), "error": err}).Info(" - General error!")
	}
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	tracer.Start(
		tracer.WithGlobalTag("env", "dev-abr"),
		tracer.WithGlobalTag("sistemas", "backend"),
		tracer.WithGlobalTag("time", "dev-abr"),
	)
	span := tracer.StartSpan("amqp.dial", tracer.ResourceName("rabbit.connect"))
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq-server.default:5672/")
	log.WithFields(log.Fields{"time": now.Format(time.RFC3339), "error": err}).Info(" - Failed to connect to RabbitMQ")
	span.Finish(tracer.WithError(err))
	defer conn.Close()

	span = tracer.StartSpan("amqp.channel", tracer.ResourceName("rabbit.channel"))
	ch, err := conn.Channel()
	log.WithFields(log.Fields{"time": now.Format(time.RFC3339), "error": err}).Info(" - Failed to open a Channel")
	span.Finish(tracer.WithError(err))
	defer ch.Close()

	span = tracer.StartSpan("amqp.queue", tracer.ResourceName("rabbit.queue"))
	q, err := ch.QueueDeclare(
		"teste", // name
		true,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	log.WithFields(log.Fields{"time": now.Format(time.RFC3339), "error": err}).Info(" - Failed to declare a Queue")
	span.Finish(tracer.WithError(err))

	span = tracer.StartSpan("amqp.consume", tracer.ResourceName("rabbit.consume"))
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	log.WithFields(log.Fields{"time": now.Format(time.RFC3339), "error": err}).Info(" - Failed to register a Consumer")
	span.Finish(tracer.WithError(err))

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.WithFields(log.Fields{"time": now.Format(time.RFC3339), "msg": d.Body, }).Info(" - message consumed")
		}
	}()

	log.WithFields(log.Fields{"msg": now.Format(time.RFC3339)}).Info(" - Wait for messages - CRTL+C to finish")
	<- forever	
	defer tracer.Stop()
}



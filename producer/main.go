package main

import (
	"context"
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

type Message struct {
	DataId int    `json:"data_id"`
	Type   string `json:"type"`
}

func main() {
	conn, err := amqp.Dial("amqp://user:password@host.docker.internal:5672/")
	if err != nil {
		log.Fatal().Msgf("%+v: %s", err, "Failed to connect to RabbitMQ")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal().Msgf("%+v: %s", err, "Failed to open a channel")
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"generate_file", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		log.Fatal().Msgf("%+v: %s", err, "Failed to declare a queue")
	}

	content := []Message{
		{
			DataId: 3,
			Type:   "csv",
		},
		{
			DataId: 3,
			Type:   "xlsx",
		},
		{
			DataId: 4,
			Type:   "txt",
		},
	}
	for _, v := range content {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		body, err := json.Marshal(v)
		if err != nil {
			log.Fatal().Msgf("%+v: %s\n", err, "Failed marshalling json body")
		}
		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		if err != nil {
			log.Fatal().Msgf("%+v: %s\n", err, "Failed to publish a message")
		}
		log.Printf(" [x] Sent %+v\n", v)
	}
}

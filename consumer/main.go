package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Message struct {
	DataId int    `json:"data_id"`
	Type   string `json:"type"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano})
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

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

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatal().Msgf("%+v: %s", err, "Failed to set QoS")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatal().Msgf("%+v: %s", err, "Failed to register a consumer")
	}

	var wg sync.WaitGroup
	wg.Add(1) // Increment the counter, but never call wg.Done()

	go func() {
		for d := range msgs {
			var message Message
			err := json.Unmarshal(d.Body, &message)
			if err != nil {
				log.Printf("%+v: %s\n", err, "Failed unmarshalling json body")
				d.Ack(false) // Acknowledge the message even if failed
				continue
			}

			if message.Type == "csv" {
				log.Info().Msg("Generating CSV")
				err = CSV3()
				if err != nil {
					log.Printf("%+v: %s\n", err, "Failed generating csv")
					d.Ack(false) // Acknowledge the message even if failed
					continue
				}
				log.Info().Msg("Done Generating CSV")
			} else if message.Type == "xlsx" {
				log.Info().Msg("Generating XLSX")
				err = XLSX3()
				if err != nil {
					log.Printf("%+v: %s\n", err, "Failed generating xlsx")
					d.Ack(false) // Acknowledge the message even if failed
					continue
				}
				log.Info().Msg("Done Generating XLSX")
			} else if message.Type == "txt" {
				log.Info().Msg("Generating TXT")
				err = TXT4()
				if err != nil {
					log.Printf("%+v: %s\n", err, "Failed generating txts")
					d.Ack(false) // Acknowledge the message even if failed
					continue
				}
				log.Info().Msg("Done Generating TXT")
			} else if message.Type == "pdf" {
				log.Info().Msg("Generating PDF")
				err = PDF3()
				if err != nil {
					log.Printf("%+v: %s\n", err, "Failed generating pdf")
					d.Ack(false) // Acknowledge the message even if failed
					continue
				}
				log.Info().Msg("Done Generating PDF")
			} else {
				log.Printf("%s: %+v\n", "Invalid message", message)
				d.Ack(false) // Acknowledge the message even if failed
				continue
			}
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
}

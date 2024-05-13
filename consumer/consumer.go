package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Failed to load .env file")
	}
	rabbitUser := os.Getenv("RABBITMQ_USER")
	rabbitPass := os.Getenv("RABBITMQ_PASS")
	// Format the connection string using the retrieved user and password
	connStr := fmt.Sprintf("amqp://%s:%s@localhost:5672", rabbitUser, rabbitPass)

	// Dial Rabbit MQ and establish a connection
	conn, err := amqp.Dial(connStr)
	onError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	onError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"demo-queue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	onError(err, "Failed to declare a queue")

	// We're about to tell the server to deliver us the messages from the queue. Since it will push us messages asynchronously, we will read the messages from a channel (returned by amqp::Consume) in a goroutine.
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	onError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		// Ranging over the messages
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever // blocking
}

func onError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"time"
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
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Println("Failed to Connect to Rabbit Mq")
		log.Fatal(err)
	}
	log.Println("Connection successfully established to Rabbit Mq")

	defer conn.Close()

	// Initiate a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Error starting the channel")
	}

	defer ch.Close()

	// we must declare a queue for us to send to; then we can publish a message to the queue:
	q, err := ch.QueueDeclare(
		"demo-queue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)

	if err != nil {
		log.Fatal("Error declaring Queue")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	body := "Demo Message to be published to Rabbit Mq"

	err = ch.PublishWithContext(
		ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)

	if err != nil {
		log.Fatalf("Error while publishing the message to the Queue %s", q.Name)
	}
	log.Print("Message successfully delivered")
}

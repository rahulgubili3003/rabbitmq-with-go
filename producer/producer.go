package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"time"
)

// RabbitMsg Define a struct to be sent via Rabbit Mq
type RabbitMsg struct {
	MsgId   int    `json:"msg_id"`
	Message string `json:"message"`
	SentBy  string `json:"sent_by"`
}

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

	// Instantiate the RabbitMsg struct
	rabbitBodyFirst := RabbitMsg{
		MsgId:   1,
		Message: "Hello. This is the first message",
		SentBy:  "Rahul",
	}

	// Marshal to Json
	jsonBody, err := json.Marshal(rabbitBodyFirst)
	if err != nil {
		log.Fatalf("Error marshalling the Rabbit Msg struct: %s", err)
	}

	// Publish the message in Json format
	err = ch.PublishWithContext(
		ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBody,
		},
	)

	if err != nil {
		log.Fatalf("Error while publishing the message to the Queue %s", q.Name)
	}
	log.Print("Message successfully delivered")
}

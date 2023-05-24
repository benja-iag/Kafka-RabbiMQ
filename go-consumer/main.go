package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

var KAFKA_HOST = os.Getenv("KAFKA_HOST")
var rabbitQueueName string

type Message struct {
	Time     time.Time `json:"time"`
	Value    string    `json:"value"`
	DeviceID int       `json:"deviceID"`
}

func printValue(value *Message, brokerName string) {
	fmt.Println()
	fmt.Println("------------" + brokerName + "------------")
	fmt.Println("MESSAGE RECIEVED:")
	fmt.Printf("timestamp: %s  - actual time:%s\n", value.Time, time.Now())
	fmt.Printf("Difference in ms: %d \n", time.Now().Sub(value.Time).Milliseconds())
	fmt.Printf("DeviceID: %d - value: %s\n", value.DeviceID, value.Value)
	fmt.Println("------------" + brokerName + "------------")
	fmt.Println()
}

func kafkaConsume(reader *kafka.Reader) {
	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("Error reading message", err.Error())
			panic("Error reading message")
		}
		var value Message
		json.Unmarshal(msg.Value, &value)
		printValue(&value, "KAFKA")
	}
}
func connectKafka() *kafka.Reader {
	fmt.Println("Trying Kafka connection with KAFKA_BROKER", KAFKA_HOST)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{KAFKA_HOST},
		Topic:   "test-topic",
		GroupID: "my-group",
	})
	return reader
}
func connectRabbit(tries int) (*amqp.Channel, error) {
	conn, err := amqp.Dial("amqp://username:password@rabbitmq:5672/")
	if err != nil {
		if tries == 15 {
			fmt.Println("Error connecting to RabbitMQ")
			return nil, err
		}
		fmt.Println("Error connecting to RabbitMQ")
		fmt.Println("LETS TRY AGAIN IN 5 SECONDS")
		time.Sleep(5 * time.Second)
		return connectRabbit(tries + 1)
	}
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error opening channel")
		return nil, err
	}
	err = ch.ExchangeDeclare(
		"logs",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("Error declaring exchange")
		return nil, err
	}
	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("Error declaring queue")
		return nil, err
	}
	err = ch.QueueBind(
		q.Name,
		"",
		"logs",
		false,
		nil)
	if err != nil {
		fmt.Println("Error binding queue")
		return nil, err
	}
	rabbitQueueName = q.Name
	return ch, nil

}
func rabbitConsume(ch *amqp.Channel) {
	msgs, err := ch.Consume(
		rabbitQueueName,
		"",
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println("Error consuming queue")
		fmt.Println(err.Error())
		panic("Error consuming queue")
	}
	fmt.Println("RabbitMQ connected")
	var forever chan struct{}
	go func() {
		for message := range msgs {
			var value Message
			json.Unmarshal(message.Body, &value)
			printValue(&value, "RABBITMQ")
		}
	}()
	<-forever
}
func main() {
	kafkaReader := connectKafka()
	rabbitReader, err := connectRabbit(0)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go kafkaConsume(kafkaReader)
	go rabbitConsume(rabbitReader)
	wg.Wait()
}

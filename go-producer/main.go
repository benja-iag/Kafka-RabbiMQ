package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

var KAFKA_HOST = os.Getenv("KAFKA_HOST")
var RABBITMQ_HOST = os.Getenv("RABBITMQ_HOST")

//

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

type Message struct {
	Time     time.Time `json:"time"`
	Value    string    `json:"value"`
	DeviceID int       `json:"deviceID"`
}

func randMessage(n int) string {
	message := make([]byte, n)
	for i := range message {
		message[i] = charset[rand.Intn(len(charset))]
	}
	return string(message)
}

func connectKafka(ctx context.Context, tries int) (*kafka.Writer, error) {
	fmt.Println("Trying Kafka connection with KAFKA_BROKER:", KAFKA_HOST)
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{KAFKA_HOST},
		Topic:   "test-topic",
	})
	err := producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.Itoa(1)),
		Value: []byte("Testing kafka conection"),
	})
	if err != nil {
		fmt.Println("LETS TRY AGAIN AFTER 5 SECONDS")
		time.Sleep(5 * time.Second)
		if tries == 15 {
			return nil, err
		}
		return connectKafka(ctx, tries+1)
	} else {
		fmt.Println("Kafka connection successfull")
		return producer, nil
	}
}
func connectRabbitmq(tries int) (*amqp.Channel, error) {
	conn, err := amqp.Dial("amqp://username:password@rabbitmq:5672/")
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ")
		if tries == 15 {
			return nil, err
		}
		fmt.Println("LETS TRY AGAIN AFTER 5 SECONDS")
		time.Sleep(5 * time.Second)
		return connectRabbitmq(tries + 1)
	}
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ Channel")
		panic(err)
	}
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		fmt.Println("Error declaring exchange")
		panic(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = ch.PublishWithContext(ctx, "logs", "", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("akjshd"),
	})
	if err != nil {
		fmt.Println("Error publishing message")
		panic(err)
	}
	fmt.Println("RabbitMQ connection successfull")
	return ch, nil
}

func KafkaProducer(ctx context.Context, producer *kafka.Writer, deviceID int, interval time.Duration) {
	for {
		message := &Message{
			Time:     time.Now(),
			Value:    randMessage(rand.Intn(100) + 1),
			DeviceID: deviceID,
		}
		value, err := json.Marshal(message)
		if err != nil {
			fmt.Println("Error marshaling json:", err)
		}
		producer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(deviceID)),
			Value: []byte(string(value)),
		})
		time.Sleep(interval)
	}

}

func RabbitProducer(producer *amqp.Channel, deviceID int, interval time.Duration) {
	for {
		ctx := context.Background()
		message := &Message{
			Time:     time.Now(),
			Value:    randMessage(rand.Intn(100) + 1),
			DeviceID: deviceID,
		}
		value, err := json.Marshal(message)
		if err != nil {
			fmt.Println("Error marshaling json:", err)
		}
		err = producer.PublishWithContext(
			ctx,
			"logs",
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(string(value)),
			},
		)
		time.Sleep(interval)
	}
}
func main() {
	fmt.Println("Starting producer...")
	time.Sleep(15 * time.Second)
	ctxKafka := context.Background()
	kafkaConnection, err := connectKafka(ctxKafka, 0)
	if err != nil {
		fmt.Println("Error connecting to Kafka")
		panic(err)
	}
	rabbitConnection, err := connectRabbitmq(0)
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ")
		panic(err)
	}
	devices := 3 // this variable represents the number of threads that will be created
	secondsInterval := 4
	fmt.Println("Number of devices:", devices)
	fmt.Println("Interval between messages:", secondsInterval, "seconds")

	var wg sync.WaitGroup
	for i := 0; i < devices; i++ {
		wg.Add(i)
		go KafkaProducer(ctxKafka, kafkaConnection, i, 1*time.Second)
		go RabbitProducer(rabbitConnection, i, time.Duration(secondsInterval)*time.Second)
	}
	wg.Wait()
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

var KAFKA_HOST = os.Getenv("KAFKA_HOST")
var rabbitQueueName string
var MAX_SIZE_LIST = 200

type Message struct {
	Time     time.Time `json:"time"`
	Value    string    `json:"value"`
	DeviceID int       `json:"deviceID"`
}
type MessageType struct {
	Message    Message
	brokerName string
}
type SaferMessageList struct {
	consumerList []MessageType
	mu           sync.Mutex
}

func printValue(value *Message, brokerName string, list *SaferMessageList) error {
	list.mu.Lock()
	if len(list.consumerList) == MAX_SIZE_LIST {
		list.mu.Unlock()
		return fmt.Errorf("Max size completed")
	}
	list.consumerList = append(list.consumerList, MessageType{
		Message:    *value,
		brokerName: brokerName,
	})
	list.mu.Unlock()
	fmt.Println()
	fmt.Println("------------" + brokerName + "------------ \t count: " + strconv.Itoa(len(list.consumerList)))
	fmt.Println("MESSAGE RECIEVED:")
	fmt.Printf("timestamp: %s  - actual time:%s\n", value.Time, time.Now())
	fmt.Printf("Difference in ms: %d \n", time.Now().Sub(value.Time).Milliseconds())
	fmt.Printf("DeviceID: %d - value: %s\n", value.DeviceID, value.Value)
	fmt.Println("------------" + brokerName + "------------")
	fmt.Println()
	return nil
}

func kafkaConsume(reader *kafka.Reader, list *SaferMessageList, wg *sync.WaitGroup) {
	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("Error reading message", err.Error())
			panic("Error reading message")
		}
		var value Message
		json.Unmarshal(msg.Value, &value)
		err = printValue(&value, "KAFKA", list)
		if err != nil {
			wg.Done()
			return
		}
	}
}
func connectKafka() *kafka.Reader {
	fmt.Println("Trying Kafka connection with KAFKA_BROKER", KAFKA_HOST)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{KAFKA_HOST},
		Topic:   "test-topic",
		GroupID: "my-group",
	})
	fmt.Println("Kafka connected successfully")
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
func rabbitConsume(ch *amqp.Channel, list *SaferMessageList, wg *sync.WaitGroup) {
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
	fmt.Println("RabbitMQ connected successfully")
	var forever chan struct{}
	go func() {
		for message := range msgs {
			var value Message
			json.Unmarshal(message.Body, &value)
			err := printValue(&value, "RABBITMQ", list)
			if err != nil {
				wg.Done()
				return
			}
		}
	}()
	<-forever
}

func consume(kafkaReader *kafka.Reader, rabbitReader *amqp.Channel) []MessageType {
	saferList := &SaferMessageList{
		consumerList: []MessageType{},
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go kafkaConsume(kafkaReader, saferList, &wg)
	go rabbitConsume(rabbitReader, saferList, &wg)
	wg.Wait()
	return saferList.consumerList
}
func main() {

	for test := 0; test < 20; test++ {
		kafkaReader := connectKafka()
		rabbitReader, err := connectRabbit(0)
		if err != nil {
			panic(err)
		}
		list := consume(kafkaReader, rabbitReader)
		countRabbit := 0
		countKafka := 0
		for i := 0; i < MAX_SIZE_LIST; i++ {
			if list[i].brokerName == "KAFKA" {
				countKafka++
			} else {
				countRabbit++
			}
		}
		fmt.Println("FINAL COUNT IN " + strconv.Itoa(MAX_SIZE_LIST) + " MESSAGES CONSUMED")
		fmt.Println("KAFKA MESSAGES:", countKafka)
		fmt.Println("RABBITMQ MESSAGES:", countRabbit)
		fmt.Println("TRYING AGAIN AFTER 15 SECONDS")
		time.Sleep(15 * time.Second)
	}
}

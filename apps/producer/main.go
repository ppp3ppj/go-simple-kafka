package main

import (
	"log"
	"time"

	"github.com/ppp3ppj/go-simple-kafka/config"
	"github.com/ppp3ppj/go-simple-kafka/models"
	"github.com/ppp3ppj/go-simple-kafka/pkg/utils"
	"github.com/segmentio/kafka-go"
)

func main() {
	// Connection part
	cfg := config.KafkaConnCfg{
        Url: "localhost:9092",
        Topic: "shop",
    }
    conn := utils.KafkaConn(cfg)

    // Check topic if already exists or not
    if !utils.IsTopicAlreadyExists(conn, cfg.Topic) {
        topicConfigs := []kafka.TopicConfig{
            {
                Topic: cfg.Topic,
                NumPartitions: 1,
                ReplicationFactor: 1,
            },
        }

        err := conn.CreateTopics(topicConfigs...)
        if err != nil {
            panic(err.Error())
        }
    }

    // Mock data
    data := func() []kafka.Message {
        products := []models.Product{
            {
                Id:    "2dc7cf08-e238-4faa-bd5f-f1cfe2e0b565",
                Title: "Coffee",
            },
            {
                Id:    "4c56ec5b-d638-42f2-ae1d-38b6fc6d2122",
                Title: "Tea",
            },
            {
                Id:    "36da5a84-f333-4ecf-a2fe-130c3e8d4ef1",
                Title: "Milk",
            },
        }

        // Convert into kafka.Message{}
        messages := make([]kafka.Message, 0)
        for _, p := range products {
            messages = append(messages, kafka.Message{
                Value: utils.CompressToJsonBytes(&p),
            })
        }

        return messages
    }()

    // Set timeout
    conn.SetReadDeadline(time.Now().Add(10 * time.Second))
    _, err := conn.WriteMessages(data...)
    if err != nil {
        log.Fatal("Failed to write messages: ", err)
    }

    // Close connection
    if err := conn.Close(); err != nil {
        log.Fatal("Failed to close writer: ", err)
    }
}

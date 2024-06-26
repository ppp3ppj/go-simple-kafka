package main

import (
	"fmt"
	"log"

	"github.com/ppp3ppj/go-simple-kafka/config"
	"github.com/ppp3ppj/go-simple-kafka/pkg/utils"
)

func main() {
    cfg := config.KafkaConnCfg{
        Url: "localhost:9092",
        Topic: "shop",
    }
    conn := utils.KafkaConn(cfg)

    for {
        message, err := conn.ReadMessage(10e3)
        if err != nil {
            break
        }
        fmt.Println(string(message.Value))
    }

    if err := conn.Close(); err != nil {
        log.Fatal("Failed to close connection: ", err)
    }
}

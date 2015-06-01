package main

import (
	. "github.com/Shopify/sarama"

  "log"
  "os"
  "os/signal"
  "sync"
)

func main() {
  config := NewConfig()
  config.Producer.Return.Successes = true
  master, err := NewConsumer([]string{os.Getenv("KAFKA_HOST")}, config)
  if err != nil {
      panic(err)
  }

  defer func() {
      if err := master.Close(); err != nil {
          panic(err)
      }
  }()

  consumer, err := master.ConsumePartition("demoday", 0, OffsetOldest)
  if err != nil {
      log.Fatalln(err)
  }

  var (
      wg       sync.WaitGroup
      msgCount int
  )

  wg.Add(1)
  go func() {
      defer wg.Done()
      for message := range consumer.Messages() {
          log.Printf("Consumed message with offset %d", message.Offset)
          msgCount++
      }
  }()

  wg.Add(1)
  go func() {
      defer wg.Done()
      for err := range consumer.Errors() {
          log.Println(err)
      }
  }()

  // Wait for an interrupt signal to trigger the shutdown
  signals := make(chan os.Signal, 1)
  signal.Notify(signals, os.Interrupt)
  <-signals
  consumer.AsyncClose()

  // Wait for the Messages and Errors channel to be fully drained.
  wg.Wait()
  log.Println("Processed", msgCount, "messages.")
}


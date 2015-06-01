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
  producer, err := NewAsyncProducer([]string{os.Getenv("KAFKA_HOST")}, config)
  if err != nil {
      panic(err)
  }

  // Trap SIGINT to trigger a graceful shutdown.
  signals := make(chan os.Signal, 1)
  signal.Notify(signals, os.Interrupt)

  var (
      wg                          sync.WaitGroup
      enqueued, successes, errors int
  )

  wg.Add(1)
  go func() {
      defer wg.Done()
      for _ = range producer.Successes() {
          successes++
      }
  }()

  wg.Add(1)
  go func() {
      defer wg.Done()
      for err := range producer.Errors() {
          log.Println(err)
          errors++
      }
  }()

	for {
    ProducerLoop:
    for {
        message := &ProducerMessage{Topic: "demoday", Value: StringEncoder("testing 123")}
        select {
        case producer.Input() <- message:
            enqueued++

        case <-signals:
            producer.AsyncClose() // Trigger a shutdown of the producer.
            break ProducerLoop
        }
    }

    wg.Wait()

    log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
	}
}

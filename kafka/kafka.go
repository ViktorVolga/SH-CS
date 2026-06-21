package kafka

import (
	"errors"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type Kafka struct {
	SaramaConfig      *sarama.Config
	SaramaAdmin       sarama.ClusterAdmin
	TopicExists       map[string]bool
	MapMutex          sync.Mutex
	Producer          sarama.SyncProducer
	Consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
}

func NewKafka() (*Kafka, error) {
	kafka := &Kafka{
		SaramaConfig: sarama.NewConfig(),
		TopicExists:  make(map[string]bool),
	}
	kafka.SaramaConfig.Producer.Return.Successes = true
	kafka.SaramaConfig.Consumer.Return.Errors = true
	var err error
	kafka.SaramaAdmin, err = sarama.NewClusterAdmin([]string{"localhost:9092"}, kafka.SaramaConfig)
	if err != nil {
		log.Printf("Can't create admin for kafka: %v", err)
	}
	return kafka, err
}

func (k *Kafka) Close() error {
	if k.SaramaAdmin != nil {
		return k.SaramaAdmin.Close()
	}
	return nil
}

func TopicExists(kafka *Kafka, name string) bool {
	kafka.MapMutex.Lock()
	defer kafka.MapMutex.Unlock()

	value, ok := kafka.TopicExists[name]
	if ok {
		return value
	} else {
		return false
	}
}

func CreateTopic(kafka *Kafka, name string, numPartitions int, replicationFactor int) error {
	kafka.MapMutex.Lock()
	defer kafka.MapMutex.Unlock()

	if ab, exists := kafka.TopicExists[name]; exists && ab {
		return nil // already exists
	}

	err := kafka.SaramaAdmin.CreateTopic(name, &sarama.TopicDetail{
		NumPartitions:     int32(numPartitions),
		ReplicationFactor: int16(replicationFactor),
	}, false)
	if err != nil {
		if topicErr, ok := err.(*sarama.TopicError); ok {
			if topicErr.Err == sarama.ErrTopicAlreadyExists {
				kafka.TopicExists[name] = true
				return nil
			}
		} else {
			return err
		}
	} else {
		kafka.TopicExists[name] = true
		return nil
	}
	return nil
}

func CreateProducer(kafka *Kafka) error {
	if kafka.Producer != nil {
		return nil
	}
	var err error
	kafka.Producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, kafka.SaramaConfig)
	if err != nil {
		log.Printf("error creating Producer: %v", err)
		return err
	}
	return nil
}

func PushStringMessage(kafka *Kafka, topic string, text string) error {
	if kafka.Producer == nil {
		return errors.New("producer is nil")
	}
	message := &sarama.ProducerMessage{
		Topic: topic,                      // Имя топика, куда отправляем
		Value: sarama.StringEncoder(text), // Значение сообщения (как строка)
		// Key: sarama.StringEncoder("some-key"), // key for parttitioning
	}

	_, _, sendErr := kafka.Producer.SendMessage(message)

	if sendErr != nil {
		log.Println("can't send message")
	} else {
		log.Println("message sended")
	}

	return nil
}

func PushBinaryMessage(kafka *Kafka, topic string, binaryData []byte) error {
	if kafka.Producer == nil {
		return errors.New("producer is nil")
	}
	message := &sarama.ProducerMessage{
		Topic: topic,                          // Имя топика, куда отправляем
		Value: sarama.ByteEncoder(binaryData), // Значение сообщения (как строка)
		// Key: sarama.StringEncoder("some-key"), // key for parttitioning
	}

	_, _, sendErr := kafka.Producer.SendMessage(message)

	if sendErr != nil {
		log.Println("can't send message")
	} else {
		log.Println("message sended to kafka")
	}

	return nil
}

func PrepareToWork(kafka *Kafka) {
	err := CreateTopic(kafka, "temperature", 1, 1)
	if err != nil {
		log.Println("can't create topic temperature")
		return
	}

	err = CreateProducer(kafka)
	if err != nil {
		log.Println("can't create Producer")
		return
	}

	log.Println("Ready to work with kafka")
}

func CreateConsumer(kafka *Kafka, topic string) {
	var err error
	kafka.Consumer, err = sarama.NewConsumer([]string{"localhost:9092"}, kafka.SaramaConfig)
	if err != nil {
		log.Fatalf("error with creating consumer: %v", err)
	}
	kafka.partitionConsumer, err = kafka.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("error while gettting partition of topic: %v", err)
	}
}

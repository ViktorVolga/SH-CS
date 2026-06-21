package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type Address struct {
	Ip   string `json:"ip"`
	Port string `json:"port"`
}

type ServerConfig struct {
	ServerAddress Address `json:"address"`
	TLS           string  `json:"tls"`
	SertPath      string  `json:"sertificate"`
}

type LoggerSettings struct {
	LogPath string `json:"sh-cs-log-path"`
}

type PostgresSettings struct {
	PostgresAddress Address `json:"address"`
	User            string  `json:"user"`
	Password        string  `json:"password"`
	DtbName         string  `json:"dtbName"`
}

type KafkaSettings struct {
	KafkaAddress Address `json:"address"`
}

type Config struct {
	Redis    Address          `json:"redis"`
	Logger   LoggerSettings   `json:"loger"`
	Server   ServerConfig     `json:"server"`
	Postgres PostgresSettings `json:"postgres"`
	Kafka    KafkaSettings    `json:"kafka"`
}

func ReadConfig(path string) (*Config, error) {
	log.Println("Loading config {}", path)
	data, err := os.ReadFile("config.json")
	if err != nil {
		fmt.Println("File reading error:", err)
		return nil, fmt.Errorf(fmt.Sprint("File reading error:", err))
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println("Json parsing error:", err)
		return nil, fmt.Errorf(fmt.Sprint("Json parsing error:", err))
	}
	return &config, nil
}

func SaveEmptyConfig(path string) (*Config, error) {
	var config Config
	config.Redis.Ip = ""
	config.Redis.Port = ""
	config.Logger.LogPath = ""
	config.Server.TLS = "true"
	config.Server.SertPath = ""
	config.Server.ServerAddress.Ip = ""
	config.Server.ServerAddress.Port = ""
	config.Postgres.PostgresAddress.Ip = ""
	config.Postgres.PostgresAddress.Port = ""
	config.Postgres.Password = ""
	config.Kafka.KafkaAddress.Ip = ""
	config.Kafka.KafkaAddress.Port = ""

	jsonData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprint("serialization error:", err))
	}
	err = os.WriteFile(path, jsonData, 0644)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprint("Writing ro file error:", err))
	}
	return &config, nil
}

func NewConfig(path string) (*Config, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return SaveEmptyConfig(path)
		}
		return nil, fmt.Errorf(fmt.Sprint("Cheking file error:", err))
	}
	return ReadConfig(path)
}

package main

import (
	"os"
	"sh-cs/config"
	"sh-cs/server"
)

func main() {
	var path = os.Args[1]
	cfg, err := config.NewConfig(path)
	if err != nil {
		println("error while reading/creating config: %s", err)
	}
	println("cfg:", cfg.Redis.Ip)
	server.RunServer()
}

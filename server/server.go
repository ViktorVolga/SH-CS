package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"sh-cs/kafka"
	messagehandler "sh-cs/messageHandler"
	"time"
)

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "localhost",
			Organization: []string{"Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0), // 1 год
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return tls.X509KeyPair(certPEM, keyPEM)
}

func RunServer() {
	var serverAddr string = ":8443"
	var poolSize int = 2
	cert, err := generateSelfSignedCert()
	if err != nil {
		log.Fatal("can't generate sertificat", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := tls.Listen("tcp", serverAddr, config)
	if err != nil {
		log.Fatal("Starting listener error:", err)
	}
	defer listener.Close()

	log.Printf("Server running on %s (TLS)", serverAddr)

	connections := make(chan net.Conn, poolSize)

	m_kafka, kafkaErr := kafka.NewKafka()
	if kafkaErr != nil {
		log.Println("can't create Kafka")
	}
	kafka.PrepareToWork(m_kafka)

	for i := 0; i < poolSize; i++ {
		go worker(connections, m_kafka)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		connections <- conn
	}
}

func worker(connections <-chan net.Conn, m_kafka *kafka.Kafka) {
	for conn := range connections {
		handleConnection(conn, m_kafka)
	}
}

func handleConnection(conn net.Conn, m_kafka *kafka.Kafka) {
	log.Println("started handle connection")
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("reading error")
			break
		}
		data := buffer[:n]
		_, err = messagehandler.NewTemperatureMessage(data)
		kafka.PushBinaryMessage(m_kafka, "temperature", data)
	}
}

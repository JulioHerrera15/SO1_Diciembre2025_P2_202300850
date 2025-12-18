package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "proyecto2/proto"
	"github.com/IBM/sarama"
	"google.golang.org/grpc"
)

type VentaMsg struct {
	ProductoID      string  `json:"producto_id"`
	Precio          float64 `json:"precio"`
	CantidadVendida int32   `json:"cantidad_vendida"`
}

type server struct {
	pb.UnimplementedProductSaleServiceServer
	producer sarama.SyncProducer
}

func (s *server) ProcesarVenta(ctx context.Context, req *pb.ProductSaleRequest) (*pb.ProductSaleResponse, error) {
	// 1. Preparar mensaje JSON
	msg := VentaMsg{
		ProductoID:      req.GetProductoId(),
		Precio:          req.GetPrecio(),
		CantidadVendida: req.GetCantidadVendida(),
	}
	msgBytes, _ := json.Marshal(msg)

	// 2. Crear mensaje de Kafka
	kafkaMsg := &sarama.ProducerMessage{
		Topic: "ventas-topic",
		Value: sarama.StringEncoder(msgBytes),
	}

	// 3. Enviar a Kafka
	partition, offset, err := s.producer.SendMessage(kafkaMsg)
	if err != nil {
		log.Printf("❌ Error enviando a Kafka: %v", err)
		return &pb.ProductSaleResponse{Estado: "Error en Kafka"}, nil
	}

	fmt.Printf("✅ Enviado a Kafka [Particion: %d, Offset: %d]: %s\n", partition, offset, req.GetProductoId())

	return &pb.ProductSaleResponse{Estado: "Procesado y en cola"}, nil
}

func main() {
	kafkaUrl := os.Getenv("KAFKA_BROKER_URL")
	if kafkaUrl == "" {
		kafkaUrl = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	
	fmt.Printf("🔌 Conectando a Kafka en: %s ...\n", kafkaUrl)
	var producer sarama.SyncProducer
	var err error

	for i := 0; i < 5; i++ {
		producer, err = sarama.NewSyncProducer([]string{kafkaUrl}, config)
		if err == nil { break }
		log.Printf("Intento %d fallido: %v. Reintentando...", i+1, err)
		time.Sleep(5 * time.Second)
	}

	if err != nil { log.Fatalf("❌ Falló Kafka: %v", err) }
	defer producer.Close()
	fmt.Println("✅ Conexión exitosa a Kafka")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil { log.Fatalf("❌ Falló puerto: %v", err) }

	s := grpc.NewServer()
	pb.RegisterProductSaleServiceServer(s, &server{producer: producer})

	fmt.Println("🚀 Worker gRPC listo en puerto 50051")
	if err := s.Serve(lis); err != nil { log.Fatalf("❌ Falló serve: %v", err) }
}
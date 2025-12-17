package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama" // Cliente de Kafka
	"github.com/go-redis/redis/v8" // Cliente de Redis/Valkey
)

// Estructura del mensaje (coincide con lo que el Worker publica)
type VentaMsg struct {
	ProductoID      string  `json:"producto_id"`
	Precio          float64 `json:"precio"`
	CantidadVendida int32   `json:"cantidad_vendida"`
}

// Contexto para el cliente Redis
var ctx = context.Background()

func main() {
	// 1. Configuración de Kafka (URL del servicio en Kubernetes)
	kafkaBrokers := os.Getenv("KAFKA_BROKER_URL")
	if kafkaBrokers == "" {
		kafkaBrokers = "my-cluster-kafka-bootstrap.kafka.svc:9092" // Default para K8s
	}
	
	// 2. Configuración de Valkey (URL del servicio en Kubernetes)
	valkeyAddr := os.Getenv("VALKEY_ADDRESS")
	if valkeyAddr == "" {
		valkeyAddr = "valkey-service:6379" // Default para K8s
	}

	// 3. Conectar a Valkey/Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: valkeyAddr,
	})
	
	// Reintentos de conexión
	for i := 0; i < 5; i++ {
		_, err := rdb.Ping(ctx).Result()
		if err == nil {
			fmt.Println("✅ Conexión exitosa a Valkey/Redis")
			break
		}
		log.Printf("Intento %d fallido con Valkey: %v. Reintentando en 5s...", i+1, err)
		time.Sleep(5 * time.Second)
	}
	defer rdb.Close()

	// 4. Conectar a Kafka Consumer
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Leer desde el principio del tópico
	consumer, err := sarama.NewConsumer([]string{kafkaBrokers}, config)
	if err != nil {
		log.Fatalf("❌ Error creando consumidor Kafka: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("ventas-topic", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("❌ Error creando consumidor de partición: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("🔌 Consumidor Kafka escuchando en '%s' (Valkey: %s)...\n", kafkaBrokers, valkeyAddr)

	// 5. Manejar señales de interrupción para cerrar limpiamente
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 6. Loop de procesamiento de mensajes
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var venta VentaMsg
			if err := json.Unmarshal(msg.Value, &venta); err != nil {
				log.Printf("❌ Error al deserializar mensaje: %v", err)
				continue
			}
			
			// Guardar en Valkey
			key := fmt.Sprintf("venta:%s:%d", venta.ProductoID, msg.Offset) // Clave única
			val := string(msg.Value) // Guardamos el JSON original
			
			err := rdb.Set(ctx, key, val, 0).Err() // 0 = sin expiración
			if err != nil {
				log.Printf("❌ Error guardando en Valkey: %v", err)
			} else {
				fmt.Printf("📦 Valkey guardó: %s - Offset %d\n", venta.ProductoID, msg.Offset)
			}

		case <-sigchan:
			fmt.Println("\n[!] Consumidor detenido.")
			return
		}
	}
}
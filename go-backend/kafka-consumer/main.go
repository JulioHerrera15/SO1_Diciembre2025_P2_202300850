package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
)

type VentaMsg struct {
	Categoria       int     `json:"categoria"`
	ProductoID      string  `json:"producto_id"`
	Precio          float64 `json:"precio"`
	CantidadVendida int32   `json:"cantidad_vendida"`
}

var ctx = context.Background()

// Mapa para traducir el ID numérico a nombre
var categorias = map[int]string{
	0: "Electronica",
	1: "Ropa",
	2: "Hogar",
	3: "Belleza",
}

func main() {
	kafkaBrokers := os.Getenv("KAFKA_BROKER_URL")
	if kafkaBrokers == "" { kafkaBrokers = "my-cluster-kafka-brokers.kafka.svc.cluster.local:9092" }
	
	valkeyAddr := os.Getenv("VALKEY_ADDRESS")
	if valkeyAddr == "" { valkeyAddr = "valkey-service:6379" }

	// Conectar a Valkey
	rdb := redis.NewClient(&redis.Options{Addr: valkeyAddr})
	
	fmt.Printf("[CONNECT] Conectando Consumidor a Kafka: %s y Valkey: %s\n", kafkaBrokers, valkeyAddr)

	// Configurar Kafka
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer([]string{kafkaBrokers}, config)
	if err != nil { log.Fatalf("[ERROR] Error Kafka: %v", err) }
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("ventas-topic", 0, sarama.OffsetNewest)
	if err != nil { log.Fatalf("[ERROR] Error Partición: %v", err) }
	defer partitionConsumer.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("[RUNNING] Consumidor Inteligente v5 LISTO. Procesando métricas...")

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var venta VentaMsg
			if err := json.Unmarshal(msg.Value, &venta); err != nil { continue }
			
			catName := categorias[venta.Categoria]
			if catName == "" { catName = "Desconocido" }

			// --- 1. GUARDAR DATA CRUDA (Log) ---
			rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: fmt.Sprintf("stream:precios:%s", catName),
				MaxLen: 1000,
				Values: map[string]interface{}{
					"precio": venta.Precio,
					"producto": venta.ProductoID,
				},
			})

			// --- 2. ACTUALIZAR ESTADÍSTICAS (Para los paneles Stat y Pie) ---
			
			// A) Contadores por Categoría (Barra)
			rdb.HIncrBy(ctx, "stats:categoria:count", catName, 1)
			
			// B) Suma de Precios para Promedio (Barra)
			rdb.HIncrByFloat(ctx, "stats:categoria:sum_precio", catName, venta.Precio)

			// C) Ranking de Productos (General) - ZSET
			rdb.ZIncrBy(ctx, "stats:productos:ranking", float64(venta.CantidadVendida), venta.ProductoID)

			// D) Ranking de Productos (Por Categoría) - ZSET
			rdb.ZIncrBy(ctx, fmt.Sprintf("stats:productos:ranking:%s", catName), float64(venta.CantidadVendida), venta.ProductoID)

			// E) Precio Máximo y Mínimo (General)
			currMax, _ := rdb.Get(ctx, "stats:precio:max").Float64()
			if venta.Precio > currMax {
				rdb.Set(ctx, "stats:precio:max", venta.Precio, 0)
			}
			currMin, _ := rdb.Get(ctx, "stats:precio:min").Float64()
			if currMin == 0 || venta.Precio < currMin {
				rdb.Set(ctx, "stats:precio:min", venta.Precio, 0)
			}

			// Agrega "FIXED" al principio para reconocerlo en el log
			fmt.Printf("[FIXED] %s ($%.2f) - Cat: %s\n", venta.ProductoID, venta.Precio, catName)

		case <-sigchan:
			return
		}
	}
}
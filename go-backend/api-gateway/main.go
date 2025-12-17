package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "proyecto2/proto"
)

// Estructura JSON que viene de Rust
type VentaInput struct {
	Categoria       int32   `json:"categoria"`
	ProductoID      string  `json:"producto_id"`
	Precio          float64 `json:"precio"`
	CantidadVendida int32   `json:"cantidad_vendida"`
}

func main() {
	// 1. Conectar con el Servidor gRPC (Worker)
	// Nota: "localhost" funciona ahora porque estamos en la misma PC. 
	// En Kubernetes, aquí pondremos el nombre del servicio DNS.
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewProductSaleServiceClient(conn)

	// 2. Iniciar Servidor HTTP (Gin)
	r := gin.Default()

	r.POST("/procesar", func(c *gin.Context) {
		var json VentaInput
		
		// Leer JSON
		if err := c.ShouldBindJSON(&json); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		fmt.Printf("📨 Intermediario recibió HTTP: %s. Enviando a gRPC...\n", json.ProductoID)

		// 3. Enviar a gRPC
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		r_grpc, err := client.ProcesarVenta(ctx, &pb.ProductSaleRequest{
			Categoria:       pb.CategoriaProducto(json.Categoria),
			ProductoId:      json.ProductoID,
			Precio:          json.Precio,
			CantidadVendida: json.CantidadVendida,
		})

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Fallo en gRPC: " + err.Error()})
			return
		}

		// Responder a Rust
		c.JSON(http.StatusOK, gin.H{"status": "Enviado a gRPC", "respuesta_worker": r_grpc.Estado})
	})

	fmt.Println("🚀 Intermediario Go corriendo en puerto 3000")
	r.Run(":3000") // Escuchar en puerto 3000
}
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	// Importamos el código generado por el proto
	pb "proyecto2/proto" 

	"google.golang.org/grpc"
)

// Estructura del servidor
type server struct {
	pb.UnimplementedProductSaleServiceServer
}

// Implementación de la función ProcesarVenta definida en el Proto
func (s *server) ProcesarVenta(ctx context.Context, req *pb.ProductSaleRequest) (*pb.ProductSaleResponse, error) {
	// 1. Imprimir lo que recibimos (Simulando procesamiento)
	fmt.Printf("🔧 Worker gRPC recibió: ID=%s, Precio=%.2f, Cantidad=%d\n", 
		req.GetProductoId(), req.GetPrecio(), req.GetCantidadVendida())

	// AQUI MÁS ADELANTE AGREGAREMOS EL CÓDIGO DE KAFKA

	// 2. Responder al cliente que todo salió bien
	return &pb.ProductSaleResponse{Estado: "Exito al procesar en Go"}, nil
}

func main() {
	// Escuchar en el puerto 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("❌ Falló al escuchar: %v", err)
	}

	// Crear servidor gRPC
	s := grpc.NewServer()
	
	// Registrar nuestro servicio
	pb.RegisterProductSaleServiceServer(s, &server{})

	fmt.Println("🚀 Servidor gRPC (Worker) corriendo en puerto 50051...")
	
	// Iniciar
	if err := s.Serve(lis); err != nil {
		log.Fatalf("❌ Falló al servir: %v", err)
	}
}
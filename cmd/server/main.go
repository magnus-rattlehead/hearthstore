package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	firestorepb "google.golang.org/genproto/googleapis/firestore/v1"

	"github.com/stiviguranjaku/hearthstore/internal/server"
	"github.com/stiviguranjaku/hearthstore/internal/storage"
)

func main() {
	port := flag.Int("port", 8080, "gRPC listen port")
	dataDir := flag.String("data-dir", "./data", "Directory for SQLite database files")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	store, err := storage.New(*dataDir)
	if err != nil {
		log.Fatalf("failed to open storage: %v", err)
	}
	defer store.Close()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	firestorepb.RegisterFirestoreServer(grpcServer, server.New(store))
	reflection.Register(grpcServer)

	log.Printf("hearthstore listening on :%d (data-dir: %s)", *port, *dataDir)
	log.Printf("set FIRESTORE_EMULATOR_HOST=localhost:%d in your client", *port)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

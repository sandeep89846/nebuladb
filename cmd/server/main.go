package main

import (
	"log"
	"net"

	"github.com/sandeep89846/nebuladb/api/proto/nebulapb"
	"github.com/sandeep89846/nebuladb/internal/index"
	"github.com/sandeep89846/nebuladb/internal/server"
	"github.com/sandeep89846/nebuladb/internal/storage"
	"github.com/sandeep89846/nebuladb/pkg/vec"
	"google.golang.org/grpc"
)

func main() {
	port := ":50051"
	walPath := "nebula.wal"

	log.Println(" Starting NebulaDB...")

	cfg := index.DefaultConfig()
	cfg.EfConstruction = 200 // Higher quality graph
	cfg.M = 32               // for better recall

	idx := index.NewHNSW(cfg)

	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	log.Println(" Replaying WAL to restore state...")
	count := 0
	err = wal.Replay(func(id string, v vec.Vector) {

		if err := idx.Insert(id, v); err != nil {
			log.Printf("Replay error for ID %s: %v", id, err)
		}
		count++
	})
	if err != nil {
		log.Fatalf("WAL Replay failed: %v", err)
	}
	log.Printf(" Restored %d vectors from disk.", count)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv := server.NewServer(idx, wal)
	nebulapb.RegisterVectorServiceServer(grpcServer, srv)

	log.Printf(" NebulaDB Engine ready on %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

package server

import (
	"context"
	"log"

	"github.com/sandeep89846/nebuladb/api/proto/nebulapb"
	"github.com/sandeep89846/nebuladb/internal/index"
	"github.com/sandeep89846/nebuladb/internal/storage"
	"github.com/sandeep89846/nebuladb/pkg/vec"
)

// Server implements the gRPC VectorService.
type Server struct {
	nebulapb.UnimplementedVectorServiceServer

	idx *index.HNSW
	wal *storage.WAL
}

func NewServer(idx *index.HNSW, wal *storage.WAL) *Server {
	return &Server{
		idx: idx,
		wal: wal,
	}
}

// Insert handles adding vectors to both WAL and Index.
func (s *Server) Insert(ctx context.Context, req *nebulapb.InsertRequest) (*nebulapb.InsertResponse, error) {

	if len(req.Vector) == 0 {
		return &nebulapb.InsertResponse{Success: false, Error: "empty vector"}, nil
	}

	v := vec.Vector(req.Vector)
	if err := s.wal.WriteInsert(req.Id, v); err != nil {
		log.Printf("WAL write error: %v", err)
		return &nebulapb.InsertResponse{Success: false, Error: "persistence failed"}, nil
	}

	if err := s.idx.Insert(req.Id, v); err != nil {
		return &nebulapb.InsertResponse{Success: false, Error: err.Error()}, nil
	}

	return &nebulapb.InsertResponse{Success: true}, nil
}

// Search handles query requests.
func (s *Server) Search(ctx context.Context, req *nebulapb.SearchRequest) (*nebulapb.SearchResponse, error) {
	matches, err := s.idx.Search(vec.Vector(req.Vector), int(req.K))
	if err != nil {
		return nil, err
	}

	// Convert internal matches to Proto matches
	pbMatches := make([]*nebulapb.SearchResponse_Match, len(matches))
	for i, m := range matches {
		pbMatches[i] = &nebulapb.SearchResponse_Match{
			Id:    m.ID,
			Score: m.Score,
		}
	}

	return &nebulapb.SearchResponse{Matches: pbMatches}, nil
}

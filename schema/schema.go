package schema

import (
	"context"
	"time"

	"github.com/hashicorp/go-memdb"
)

type Job struct {
	Id               string
	Name             string
	Request          any
	RequestTimestamp string
	Timestamp        string
	Status           string
	Delay            time.Duration
	Ctx              context.Context
}

func Schema() *memdb.DBSchema {
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"job": {
				Name: "job",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Id"},
					},
				},
			},
		},
	}
}

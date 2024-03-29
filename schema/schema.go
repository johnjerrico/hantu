package schema

import "github.com/hashicorp/go-memdb"

type Job struct {
	Id               string
	Name             string
	Checksum         string
	Request          interface{}
	RequestTimestamp string
	Timestamp        string
	Status           string
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

// Package collection provides functionality for getting statistics of mongodb collection.
package collection

import (
	"context"
	"fmt"

	"github.com/ZorinArsenij/mgo-schema-stat/internal/schema"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	statCollectionSize = 10000
)

// Stat represents collection statistics.
type Stat struct {
	StorageSize float64
	IndexesSize map[string]float64
}

// Collection represents collection info and document generation function.
type Collection struct {
	Name        string
	generateDoc func() bson.D

	schema  map[string]schema.FieldType
	indexes map[string]schema.Index
}

// New creates collection from schema.
func New(name string, schm schema.Collection) (*Collection, error) {
	generator, err := schm.GetDocGenerator()
	if err != nil {
		return nil, fmt.Errorf("get document generator: %w", err)
	}

	return &Collection{
		Name:        name,
		generateDoc: generator,

		schema:  schm.Schema,
		indexes: schm.Indexes,
	}, err
}

// Stat gets statistic by filling with generated data mongodb collection
func (c Collection) Stat(db *mongo.Database, size uint64) (*Stat, error) {
	if err := c.create(db); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	if err := c.fill(db); err != nil {
		return nil, fmt.Errorf("fill collection: %w", err)
	}

	rawStat, err := c.collectStat(db)
	if err != nil {
		return nil, fmt.Errorf("stat collection: %w", err)
	}

	scale := float64(size) / float64(statCollectionSize)

	stat := Stat{
		StorageSize: float64(rawStat["storageSize"].(int32)) * scale,
		IndexesSize: make(map[string]float64),
	}

	for name, size := range rawStat["indexSizes"].(bson.M) {
		stat.IndexesSize[name] = float64(size.(int32)) * scale
	}

	return &stat, err
}

func (c Collection) create(db *mongo.Database) error {
	if err := db.CreateCollection(context.Background(), c.Name); err != nil {
		return err
	}

	for name, index := range c.indexes {
		keys := make([]bson.E, 0, len(index.Parts))

		for _, part := range index.Parts {
			keys = append(keys, bson.E{
				Key:   part.Key,
				Value: part.Value,
			})
		}

		partialFilterExpression := make([]bson.E, 0, len(index.PartialFilterExpression))

		for key, value := range index.PartialFilterExpression {
			partialFilterExpression = append(partialFilterExpression, bson.E{
				Key:   key,
				Value: value,
			})
		}

		if _, err := db.Collection(c.Name).Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: keys,
			Options: &options.IndexOptions{
				Name:                    &name,
				Unique:                  &index.Unique,
				PartialFilterExpression: partialFilterExpression,
			},
		}); err != nil {
			return fmt.Errorf("create index %s: %w", name, err)
		}
	}

	return nil
}

func (c Collection) fill(db *mongo.Database) error {
	for i := 1; i <= statCollectionSize/1000; i++ {
		batch := make([]interface{}, 0, 1000)

		for j := 0; j < 1000; j++ {
			batch = append(batch, c.generateDoc())
		}

		if _, err := db.Collection(c.Name).InsertMany(context.Background(), batch, nil); err != nil {
			return err
		}
	}

	return nil
}

func (c Collection) collectStat(db *mongo.Database) (bson.M, error) {
	if res := db.RunCommand(context.Background(), bson.D{{
		Key:   "validate",
		Value: c.Name,
	}, {
		Key:   "full",
		Value: true,
	}}, nil); res.Err() != nil {
		return nil, fmt.Errorf("validate data: %w", res.Err())
	}

	if res := db.RunCommand(context.Background(), bson.D{{
		Key:   "reIndex",
		Value: c.Name,
	}}, nil); res.Err() != nil {
		return nil, fmt.Errorf("reindex: %w", res.Err())
	}

	res := db.RunCommand(context.Background(), bson.D{{
		Key:   "collStats",
		Value: c.Name,
	}}, nil)

	if res.Err() != nil {
		return nil, fmt.Errorf("collStats: %w", res.Err())
	}

	var stat bson.M
	if err := res.Decode(&stat); err != nil {
		return nil, fmt.Errorf("decode stat: %w", err)
	}

	return stat, nil
}

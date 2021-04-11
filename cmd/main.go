package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"

	"github.com/ZorinArsenij/mgo-schema-stat/internal/collection"
	"github.com/ZorinArsenij/mgo-schema-stat/internal/schema"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	schemaPath := flag.String("schema", "schema/schema.json", "path to file with schema")
	mongoAddr := flag.String("mongo_addr", "127.0.0.1:27017", "mongodb running instance addr")
	flag.Parse()

	schm, err := schema.ParseFromFile(*schemaPath)
	if err != nil {
		log.Fatal(err)
	}

	collections := make([]collection.Collection, 0, len(schm.Collections))

	for name, s := range schm.Collections {
		col, err := collection.New(name, s)
		if err != nil {
			log.Fatalf("failed to init collection %s: %s", name, err)
		}

		collections = append(collections, *col)
	}

	uri := &url.URL{
		Scheme: "mongodb",
		Host:   *mongoAddr,
		Path:   "/",
	}

	client, err := mongo.Connect(context.Background(), options.Client().
		ApplyURI(uri.String()).
		SetDirect(true))
	if err != nil {
		log.Fatalf("failed to connect to mongo instance: %s", err)
	}

	if err := client.Database("test_db").Drop(context.Background()); err != nil {
		log.Fatalf("failed to drop database: %s", err)
	}

	db := client.Database("test_db")

	var totalStorageSize, totalIndexSize float64

	for _, col := range collections {
		stat, err := col.Stat(db, schm.Collections[col.Name].Len)
		if err != nil {
			log.Fatalf("failed to get stat for %s: %s", col.Name, err)
		}

		totalStorageSize += stat.StorageSize

		fmt.Printf("Collection %s stat:\n", col.Name)
		fmt.Printf("Storage size: %.3f gb\n", stat.StorageSize/1024/1024/1024)
		for index, size := range stat.IndexesSize {
			totalIndexSize += size
			fmt.Printf("Index %s size: %.3f gb\n", index, size/1024/1024/1024)
		}

		fmt.Println()
	}

	fmt.Printf("Total stat:\n")
	fmt.Printf("Storage size: %.3f gb\n", totalStorageSize/1024/1024/1024)
	fmt.Printf("Index size: %.3f gb\n", totalIndexSize/1024/1024/1024)
}

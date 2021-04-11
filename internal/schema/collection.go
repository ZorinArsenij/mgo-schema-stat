package schema

import (
	"fmt"
	"math/rand"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	objectType   = "object"
	arrayType    = "array"
	doubleType   = "double"
	stringType   = "string"
	binDataType  = "binData"
	objectIDType = "objectId"
	boolType     = "bool"
	intType      = "int"
	longType     = "long"

	defaultStringMaxLen     uint64 = 10
	defaultBinDataMaxLen    uint64 = 16
	defaultArrayItemsMaxLen uint64 = 1
)

// FieldType represent document field properties
type FieldType struct {
	BsonType   string               `json:"bsonType"`
	Properties map[string]FieldType `json:"properties"`
	Enum       []interface{}        `json:"enum"`
	Items      []FieldType          `json:"items"`
	MaxLength  uint64               `json:"maxLength"`
	MaxItems   uint64               `json:"maxItems"`
}

// Index represents document index properties
type Index struct {
	Parts []struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	} `json:"parts"`
	Unique                  bool                   `json:"unique"`
	PartialFilterExpression map[string]interface{} `json:"partialFilterExpression"`
}

// Collection represents collection schema and properties
type Collection struct {
	Schema  map[string]FieldType `json:"schema"`
	Indexes map[string]Index     `json:"indexes"`
	Len     uint64               `json:"len"`
}

// GetDocGenerator returns collection document generation function
func (c Collection) GetDocGenerator() (func() bson.D, error) {
	generators := make(map[string]func() interface{}, len(c.Schema))

	for name, t := range c.Schema {
		generate, err := getFieldGenerator(t)
		if err != nil {
			return nil, fmt.Errorf("get field generator: %w", err)
		}

		generators[name] = generate
	}

	return func() bson.D {
		doc := make(bson.D, 0, len(c.Schema))

		for name := range c.Schema {
			doc = append(doc, bson.E{
				Key:   name,
				Value: generators[name](),
			})
		}

		return doc
	}, nil
}

func getFieldGenerator(fieldType FieldType) (func() interface{}, error) {
	if len(fieldType.Enum) != 0 {
		return func() interface{} {
			return fieldType.Enum[rand.Int()%len(fieldType.Enum)]
		}, nil
	}

	switch fieldType.BsonType {
	case doubleType:
		return func() interface{} {
			return rand.Float32()
		}, nil
	case stringType:
		maxLen := defaultStringMaxLen
		if fieldType.MaxLength != 0 {
			maxLen = fieldType.MaxLength
		}

		return func() interface{} {
			return string(randBytes(maxLen))
		}, nil
	case binDataType:
		maxLen := defaultBinDataMaxLen
		if fieldType.MaxLength != 0 {
			maxLen = fieldType.MaxLength
		}

		return func() interface{} {
			return randBytes(maxLen)
		}, nil
	case objectIDType:
		return func() interface{} {
			return primitive.NewObjectID()
		}, nil
	case boolType:
		return func() interface{} {
			return rand.Uint32()%2 == 0
		}, nil
	case intType:
		return func() interface{} {
			return rand.Int31()
		}, nil
	case longType:
		return func() interface{} {
			return rand.Int63()
		}, nil
	case objectType:
		propGenerators := make(map[string]func() interface{}, len(fieldType.Properties))

		for name, t := range fieldType.Properties {
			generate, err := getFieldGenerator(t)
			if err != nil {
				return nil, fmt.Errorf("unsupported nested filed %s type: %w", name, err)
			}

			propGenerators[name] = generate
		}

		return func() interface{} {
			props := make(bson.D, 0, len(fieldType.Properties))

			for name := range fieldType.Properties {
				props = append(props, bson.E{
					Key:   name,
					Value: propGenerators[name](),
				})
			}

			return props
		}, nil
	case arrayType:
		generators := make([]func() interface{}, 0, len(fieldType.Items))

		for _, t := range fieldType.Items {
			generate, err := getFieldGenerator(t)
			if err != nil {
				return nil, fmt.Errorf("unsupported array item type: %w", err)
			}

			generators = append(generators, generate)
		}

		maxItemsLen := defaultArrayItemsMaxLen
		if fieldType.MaxItems != 0 {
			maxItemsLen = fieldType.MaxItems
		}

		return func() interface{} {
			items := make(bson.A, 0, maxItemsLen)

			for i := 0; i < int(maxItemsLen); i++ {
				items = append(items, generators[rand.Int()%len(fieldType.Items)]())
			}

			return items
		}, nil
	default:
		return nil, fmt.Errorf("unsupported filed type")
	}
}

func randBytes(length uint64) []byte {
	var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

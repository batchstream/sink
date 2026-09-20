package mongodb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/batchstream/sink-go/uri"
	"github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoNativeURISelectsDatabase(t *testing.T) {
	store := &Store{store: "primary"}
	target, err := uri.New("primary", []string{"业务"})
	if err != nil {
		t.Fatal(err)
	}
	document := bson.D{{Key: "find", Value: "products"}}
	payload, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.NativeRequest{URI: target.String(), ContentType: "application/bson", Payload: payload}
	database, command, err := store.validateNativeCommand(request, true)
	if err != nil || database != "业务" || command[0].Key != "find" || command[0].Value != "products" {
		t.Fatalf("native target changed: %s %+v %v", database, command, err)
	}
	for _, invalid := range []string{"sink://primary", "sink://other/catalog", "sink://primary/catalog/products/s:id", "sink://primary/catalog%2Fother", "sink://primary/a.b", "sink://primary/catalog/products%00"} {
		request.URI = invalid
		if _, _, err := store.validateNativeCommand(request, true); err == nil {
			t.Errorf("accepted invalid database URI %q", invalid)
		}
	}
}

func TestMongoNativeCollectionURIBindsCommands(t *testing.T) {
	store := &Store{store: "primary"}
	target, err := uri.New("primary", []string{"业务", "商品/目录"})
	if err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "id", Value: bson.NewObjectID()}, {Key: "date", Value: bson.DateTime(123)}, {Key: "count", Value: int64(1<<53 + 1)}}
	for _, collection := range []string{"", "商品/目录"} {
		value := bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: filter}}
		payload, err := bson.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		original := bytes.Clone(payload)
		request := storage.NativeRequest{URI: target.String(), ContentType: "application/bson", Payload: payload}
		database, command, err := store.validateNativeCommand(request, true)
		if err != nil || database != "业务" || command[0].Value != "商品/目录" {
			t.Fatalf("collection binding: %s %+v %v", database, command, err)
		}
		encoded, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(bson.Raw(original).Lookup("filter").Value, bson.Raw(encoded).Lookup("filter").Value) || !bytes.Equal(payload, original) {
			t.Fatal("binding changed native BSON arguments or caller payload")
		}
	}
	request := storage.NativeRequest{URI: target.String()}
	database, command, err := store.validateNativeCommand(request, true)
	if err != nil || database != "业务" || len(command) != 1 || command[0].Key != "find" || command[0].Value != "商品/目录" {
		t.Fatalf("default collection query: %s %+v %v", database, command, err)
	}
}

func TestMongoNativeCollectionURIRejectsConflictingCommands(t *testing.T) {
	store := &Store{store: "primary"}
	cases := []struct {
		name    string
		command bson.D
		query   bool
	}{
		{name: "other collection", command: bson.D{{Key: "find", Value: "other"}}, query: true},
		{name: "database aggregate", command: bson.D{{Key: "aggregate", Value: 1}}, query: true},
		{name: "database command", command: bson.D{{Key: "ping", Value: 1}}},
		{name: "database placeholder", command: bson.D{{Key: "dbStats", Value: ""}}},
		{name: "duplicate target", command: bson.D{{Key: "find", Value: ""}, {Key: "find", Value: "other"}}, query: true},
		{name: "empty document", command: bson.D{}, query: true},
		{name: "writing query", command: bson.D{{Key: "aggregate", Value: ""}, {Key: "pipeline", Value: bson.A{bson.D{{Key: "$out", Value: "other"}}}}}, query: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			payload, err := bson.Marshal(test.command)
			if err != nil {
				t.Fatal(err)
			}
			request := storage.NativeRequest{URI: "sink://primary/catalog/products", ContentType: "application/bson", Payload: payload}
			if _, _, err := store.validateNativeCommand(request, test.query); err == nil {
				t.Fatal("accepted conflicting or invalid command")
			}
		})
	}
	for _, request := range []storage.NativeRequest{
		{URI: "sink://primary/catalog"},
		{URI: "sink://primary/catalog/products", ContentType: "application/json"},
		{URI: "sink://primary/catalog/products", ContentType: "application/bson", Payload: []byte("invalid")},
	} {
		if _, _, err := store.validateNativeCommand(request, true); err == nil {
			t.Fatalf("invalid query accepted: %+v", request)
		}
	}
	request := storage.NativeRequest{URI: "sink://primary/catalog/products"}
	if _, _, err := store.validateNativeCommand(request, false); err == nil || !strings.Contains(err.Error(), "invalid BSON command") {
		t.Fatalf("Execute must require an explicit command: %v", err)
	}
}

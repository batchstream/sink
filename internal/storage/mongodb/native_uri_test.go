package mongodb

import (
	"testing"

	"github.com/liran/sink-go/uri"
	"github.com/liran/sink/internal/storage"
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
	for _, invalid := range []string{"sink://primary", "sink://other/catalog", "sink://primary/catalog/products", "sink://primary/catalog%2Fother", "sink://primary/a.b"} {
		request.URI = invalid
		if _, _, err := store.validateNativeCommand(request, true); err == nil {
			t.Errorf("accepted invalid database URI %q", invalid)
		}
	}
}

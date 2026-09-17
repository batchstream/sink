package config

import (
	"github.com/liran/sink-go/uri"

	"fmt"
	"strings"
)

func resolveStorage(prefix string, file storageFile) (Storage, error) {
	var loaded Storage
	loaded.Name = strings.TrimSpace(file.Name)
	if !uri.ValidStore(loaded.Name) {
		return loaded, fmt.Errorf("%s.name must be a canonical lowercase Store name", prefix)
	}
	v := validator{}
	loaded.Driver = Driver(strings.TrimSpace(string(file.Driver)))
	switch loaded.Driver {
	case DriverMongoDB:
		mongo := &loaded.MongoDB
		var err error
		mongo.URI, err = resolveSecret(prefix+".mongodb.uri", file.MongoDB.URI, file.MongoDB.URIFile)
		if err != nil {
			return loaded, err
		}
		if mongo.URI == "" {
			return loaded, fmt.Errorf("%s.mongodb.uri is required when driver is mongodb", prefix)
		}
		mongo.MetadataField = valueOrDefault(file.MongoDB.MetadataField, "__sink")
		mongo.MaxConcurrentWrites = v.integer(prefix+".mongodb.max_concurrent_writes", file.MongoDB.MaxConcurrentWrites, 64)
		mongo.MaxConcurrentGroups = v.integer(prefix+".mongodb.max_concurrent_groups", file.MongoDB.MaxConcurrentGroups, 16)
	case DriverElasticsearch, DriverOpenSearch:
		search := &loaded.Search
		search.Endpoints = nonEmptyValues(file.Search.Endpoints)
		if len(search.Endpoints) == 0 {
			return loaded, fmt.Errorf("%s.search.endpoints is required for a search driver", prefix)
		}
		var err error
		search.Username, err = resolveSecret(prefix+".search.username", file.Search.Username, file.Search.UsernameFile)
		if err != nil {
			return loaded, err
		}
		search.Password, err = resolveSecret(prefix+".search.password", file.Search.Password, file.Search.PasswordFile)
		if err != nil {
			return loaded, err
		}
		search.APIKey, err = resolveSecret(prefix+".search.api_key", file.Search.APIKey, file.Search.APIKeyFile)
		if err != nil {
			return loaded, err
		}
		if (search.Username == "") != (search.Password == "") {
			return loaded, fmt.Errorf("%s.search.username and %s.search.password must be configured together", prefix, prefix)
		}
		if search.APIKey != "" && search.Username != "" {
			return loaded, fmt.Errorf("%s.search.api_key cannot be combined with basic authentication", prefix)
		}
	default:
		return loaded, fmt.Errorf("%s.driver must be mongodb, elasticsearch, or opensearch", prefix)
	}
	loaded.Kafka = resolveKafka(prefix+".kafka", file.Kafka, &v)
	return loaded, v.err
}

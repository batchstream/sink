package config

import (
	"fmt"
	"strings"
)

func resolveStorage(prefix string, file storageFile) (Storage, error) {
	var loaded Storage
	loaded.DatabaseID = strings.TrimSpace(file.DatabaseID)
	loaded.Name = strings.TrimSpace(file.Name)
	if loaded.Name == "" {
		return loaded, fmt.Errorf("%s.name is required", prefix)
	}
	v := validator{}
	loaded.Driver = Driver(strings.TrimSpace(string(file.Driver)))
	switch loaded.Driver {
	case DriverMongoDB:
		mongo := &loaded.MongoDB
		mongo.URI = strings.TrimSpace(file.MongoDB.URI)
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
		search.Username = strings.TrimSpace(file.Search.Username)
		search.Password = strings.TrimSpace(file.Search.Password)
		search.APIKey = strings.TrimSpace(file.Search.APIKey)
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

package mongodb

import "go.mongodb.org/mongo-driver/v2/bson"

// Cursor options belong to the command, and writing stages belong to a
// pipeline. Matching their names inside filters or literal values rejects data.
func forbiddenNativeQuery(command bson.D) bool {
	for _, field := range command {
		switch field.Key {
		case "tailable", "awaitData", "noCursorTimeout", "singleBatch":
			return true
		case "pipeline":
			if command[0].Key == "aggregate" && forbiddenNativePipeline(field.Value, 0) {
				return true
			}
		}
	}
	return false
}

func forbiddenNativePipeline(value any, depth int) bool {
	if depth > 100 {
		return true
	}
	pipeline, ok := value.(bson.A)
	if !ok {
		return true
	}
	for _, value := range pipeline {
		stage, ok := value.(bson.D)
		if !ok || len(stage) != 1 {
			return true
		}
		operator := stage[0]
		switch operator.Key {
		case "$out", "$merge", "$changeStream":
			return true
		case "$facet":
			facets, ok := operator.Value.(bson.D)
			if !ok {
				return true
			}
			for _, facet := range facets {
				if forbiddenNativePipeline(facet.Value, depth+1) {
					return true
				}
			}
		case "$lookup", "$unionWith":
			options, _ := operator.Value.(bson.D)
			for _, option := range options {
				if option.Key == "pipeline" && forbiddenNativePipeline(option.Value, depth+1) {
					return true
				}
			}
		}
	}
	return false
}

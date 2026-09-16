// Package testuri constructs canonical URI fixtures for tests.
package testuri

import "github.com/liran/sink-go/uri"

func Resource(store string, segments []string) string {
	address, err := uri.New(store, segments)
	if err != nil {
		panic(err)
	}
	return address.String()
}
func Record(store string, segments []string, key uri.Key) string {
	address, err := uri.AppendKey(Resource(store, segments), key)
	if err != nil {
		panic(err)
	}
	return address.String()
}
func Address(store string, segments []string, key uri.Key) uri.Address {
	address, err := uri.Parse(Record(store, segments, key))
	if err != nil {
		panic(err)
	}
	return address
}
func Dataset(store, namespace, dataset string, bson bool) string {
	segments := []string{dataset}
	if bson {
		segments = []string{namespace, dataset}
	}
	return Resource(store, segments)
}

func WithSegment(value string, index int, segment string) string {
	address, err := uri.Parse(value)
	if err != nil {
		panic(err)
	}
	parts := address.Segments()
	if index < 0 {
		index += len(parts)
	}
	parts[index] = segment
	return Resource(address.Store(), parts)
}
func WithKey(value string, key uri.Key) string {
	segment, err := uri.FormatKey(key)
	if err != nil {
		panic(err)
	}
	return WithSegment(value, -1, segment)
}
func Key(address uri.Address) uri.Key {
	parts := address.Segments()
	key, err := uri.ParseKey(parts[len(parts)-1])
	if err != nil {
		panic(err)
	}
	return key
}

func BatchKey(address uri.Address) (string, error) {
	parts := address.Segments()
	resource, err := uri.New(address.Store(), parts[:len(parts)-1])
	if err != nil {
		return "", err
	}
	return resource.String(), nil
}
func WithStore(value string, store string) string {
	address, err := uri.Parse(value)
	if err != nil {
		panic(err)
	}
	return Resource(store, address.Segments())
}

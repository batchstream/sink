// Package search implements document storage compatible with Elasticsearch and OpenSearch.
package search

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/batchstream/sink-protocol/uri"

	"github.com/batchstream/sink/internal/storage"
)

const (
	ContentTypeJSON         = "application/json"
	defaultRequestTimeout   = 30 * time.Second
	defaultMaxResponseSize  = 64 << 20
	defaultEndpointCooldown = 5 * time.Second
	defaultIdleConnections  = 128
)

type Driver string

const (
	DriverElasticsearch Driver = "elasticsearch"
	DriverOpenSearch    Driver = "opensearch"
)

type Options struct {
	Driver          Driver
	Endpoints       []string
	Store           string
	Username        string
	Password        string
	APIKey          string
	HTTPClient      *http.Client
	MaxResponseSize int64
}

type Store struct {
	driver          Driver
	endpoints       []*endpointState
	logicalStore    string
	username        string
	password        string
	apiKey          string
	client          *http.Client
	transport       *http.Transport
	maxResponseSize int64
	nextEndpoint    atomic.Uint64
	scanSizes       scanSizeCache
}

type endpointState struct {
	value      *url.URL
	retryAfter atomic.Int64
}

func New(opts Options) (*Store, error) {
	if opts.Driver != DriverElasticsearch && opts.Driver != DriverOpenSearch {
		return nil, fmt.Errorf("create search storage: unsupported driver %q", opts.Driver)
	}
	if opts.Store == "" {
		return nil, errors.New("create search storage: logical store is required")
	}
	if len(opts.Endpoints) == 0 {
		return nil, errors.New("create search storage: at least one endpoint is required")
	}
	if (opts.Username == "") != (opts.Password == "") {
		return nil, errors.New("create search storage: username and password must be configured together")
	}
	if opts.APIKey != "" && opts.Username != "" {
		return nil, errors.New("create search storage: API key and basic authentication are mutually exclusive")
	}
	if opts.MaxResponseSize < 0 {
		return nil, errors.New("create search storage: max response size cannot be negative")
	}

	endpoints := make([]*endpointState, 0, len(opts.Endpoints))
	for _, rawEndpoint := range opts.Endpoints {
		endpoint, err := parseEndpoint(rawEndpoint)
		if err != nil {
			return nil, err
		}
		state := &endpointState{value: endpoint}
		endpoints = append(endpoints, state)
	}
	client := opts.HTTPClient
	var transport *http.Transport
	if client == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
		// Retain a concurrent burst instead of closing all but two connections
		// per endpoint. The Store-wide limit bounds idle sockets across endpoints.
		transport.MaxIdleConns = defaultIdleConnections
		transport.MaxIdleConnsPerHost = defaultIdleConnections
		client = &http.Client{Timeout: defaultRequestTimeout, Transport: transport}
	}
	// A redirect can resend a mutation or move a read away from its configured
	// endpoint. Apply one policy without changing a caller-owned client.
	copiedClient := *client
	copiedClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	client = &copiedClient
	maxResponseSize := opts.MaxResponseSize
	if maxResponseSize == 0 {
		maxResponseSize = defaultMaxResponseSize
	}
	store := &Store{
		driver:          opts.Driver,
		endpoints:       endpoints,
		logicalStore:    opts.Store,
		username:        opts.Username,
		password:        opts.Password,
		apiKey:          opts.APIKey,
		client:          client,
		transport:       transport,
		maxResponseSize: maxResponseSize,
	}
	return store, nil
}

// Close releases this Store's idle connections after its requests have drained.
// A supplied HTTPClient remains owned by its caller.
func (s *Store) Close() {
	if s.transport != nil {
		s.transport.CloseIdleConnections()
	}
}

func parseEndpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("create search storage: parse endpoint: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("create search storage: endpoint scheme must be http or https")
	}
	if endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("create search storage: endpoint must contain only scheme, host, and an optional path")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	return endpoint, nil
}

type resolvedDocument struct {
	index string
	id    string
}

func (s *Store) resolve(address storage.Address) (resolvedDocument, error) {
	var resolved resolvedDocument
	if address.Store() != s.logicalStore {
		err := fmt.Errorf("logical store %q is not configured", address.Store())
		return resolved, storage.InvalidArgumentError(err)
	}
	segments := address.Segments()
	if len(segments) != 2 {
		return resolved, storage.InvalidArgumentError(errors.New("search record URI requires index/typed-key"))
	}
	key, err := uri.ParseKey(segments[1])
	if err != nil {
		return resolved, storage.InvalidArgumentError(err)
	}
	index := segments[0]
	id, err := documentID(key)
	if err != nil {
		return resolved, storage.InvalidArgumentError(err)
	}

	resolved.index = index
	resolved.id = id
	return resolved, nil
}

func (s *Store) BatchKey(address storage.Address) (string, error) {
	resolved, err := s.resolve(address)
	if err != nil {
		return "", err
	}
	return resolved.index, nil
}

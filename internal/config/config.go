// Package config loads and validates Sink runtime configuration without opening dependencies.
package config

import "time"

type Mode string

const (
	ModeGateway Mode = "gateway"
	ModeEngine  Mode = "engine"
	ModeWorker  Mode = "worker"
)

type Driver string

const (
	DriverMongoDB       Driver = "mongodb"
	DriverElasticsearch Driver = "elasticsearch"
	DriverOpenSearch    Driver = "opensearch"
)

// Config contains resolved runtime values. Construct it with Load or Decode.
type Config struct {
	Logging         Logging
	Memory          Memory
	Gateway         Gateway
	Mode            Mode
	GRPC            GRPC
	Health          Health
	Prometheus      Prometheus
	Storage         Storage
	Service         Service
	ShutdownTimeout time.Duration
}

// Memory is one process-local capacity shared by all request classes.
// MaxBytes == 0 selects runtime detection.
type Memory struct {
	MaxBytes     int
	BurstPercent int
	WaitTimeout  time.Duration
}

type GRPC struct {
	Address                string
	MaxReceiveMessageBytes int
	MaxSendMessageBytes    int
}

type Health struct {
	Address string
}

type Prometheus struct {
	Enabled bool
	Address string
}

type Service struct {
	Request   Request
	Execution Execution
	Publish   Publish
	Batching  Batching
	Merge     Merge
}

type Request struct {
	Timeout       time.Duration
	MaxOperations int
	MaxReadBytes  int
}

type Execution struct {
	MaxSnapshotBytes int
	MaxOutputBytes   int
	MongoDB          MongoDB
	MaxRequests      int
	MaxBytes         int
	Queue            AdmissionQueue
	Scan             Scan
}

type AdmissionQueue struct {
	MaxRequests int
	MaxBytes    int
	MaxWait     time.Duration
}

type Scan struct {
	MaxRequests   int
	MaxBytes      int
	AdmissionWait time.Duration
}

type Publish struct {
	MaxRequests int
	MaxBytes    int
}

type Batching struct {
	MaxWait       time.Duration
	MaxOperations int
	MaxBytes      int
	Queue         BatchQueue
}

type BatchQueue struct {
	MaxOperations int
	MaxBytes      int
}

type Merge struct {
	MaxAttempts int
	Lua         Lua
}

type Lua struct {
	Timeout           time.Duration
	MaxSourceBytes    int
	MaxResultBytes    int
	MaxCachedPrograms int
	MaxInstructions   int
}

type Storage struct {
	Name    string
	Driver  Driver
	MongoDB MongoDB
	Search  Search
	Kafka   Kafka
}

type MongoDB struct {
	URI                 string
	MetadataField       string
	MaxConcurrentWrites int
	MaxConcurrentGroups int
}

type Search struct {
	Endpoints []string
	Username  string
	Password  string
	APIKey    string
}

type Kafka struct {
	Enabled    bool
	Brokers    []string
	Topic      Topic
	Producer   Producer
	Consumer   Consumer
	DeadLetter DeadLetter
}

type Topic struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Retention         time.Duration
	MinInSyncReplicas int
	MaxRecordBytes    int
}

type Producer struct {
	MaxBufferedBytes int
}

type Consumer struct {
	GroupID           string
	MaxPollRecords    int
	ProcessingTimeout time.Duration
	Retry             Retry
}

type Retry struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
}

type DeadLetter struct {
	Topic     string
	Retention time.Duration
}

// Gateway owns only routing and bounded forwarding resources.
type Gateway struct {
	MaxRequestsPerStore int
	Routes              []Route
	DNSRefreshInterval  time.Duration
	IdleTimeout         time.Duration
	MaxConnections      int
	MaxRequests         int
	MaxBytes            int
	MaxFanout           int
}

// Route belongs to the Gateway configuration and names one Store's Engines.
type Route struct {
	Store  string   `yaml:"store"`
	Target string   `yaml:"target"`
	TLS    RouteTLS `yaml:"tls"`
}

type RouteTLS struct {
	Insecure   bool   `yaml:"insecure"`
	ServerName string `yaml:"server_name"`
}

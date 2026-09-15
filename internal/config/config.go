// Package config loads and validates Sink runtime configuration without opening dependencies.
package config

import "time"

type Mode string

const (
	ModeServer Mode = "server"
	ModeWorker Mode = "worker"
	ModeAll    Mode = "all"
)

type Driver string

const (
	DriverMongoDB       Driver = "mongodb"
	DriverElasticsearch Driver = "elasticsearch"
	DriverOpenSearch    Driver = "opensearch"
)

// Config contains resolved runtime values. Construct it with Load or Decode.
type Config struct {
	Mode            Mode
	GRPC            GRPC
	Prometheus      Prometheus
	Storages        []Storage
	Service         Service
	ShutdownTimeout time.Duration
}

type GRPC struct {
	Address                string
	MaxReceiveMessageBytes int
	MaxSendMessageBytes    int
}

type Prometheus struct {
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
	MaxRequests         int
	MaxBytes            int
	MaxRequestsPerStore int
	Queue               AdmissionQueue
	Scan                Scan
}

type AdmissionQueue struct {
	MaxRequests         int
	MaxBytes            int
	MaxRequestsPerStore int
	MaxWait             time.Duration
}

type Scan struct {
	MaxRequests         int
	MaxBytes            int
	MaxRequestsPerStore int
	AdmissionWait       time.Duration
}

type Publish struct {
	MaxRequestsPerStore int
	MaxRequests         int
	MaxBytes            int
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
	Limits  StoreLimits
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

type StoreLimits struct {
	// Zero means this store shares the global execution byte limit.
	MaxExecutionBytes int
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

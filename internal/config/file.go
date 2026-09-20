package config

import "time"

// File types preserve omission so defaults can depend on other configured limits.
// Runtime code receives only resolved values through Config.
type configFile struct {
	Logging loggingFile `yaml:"logging"`
	Memory  memoryFile  `yaml:"memory"`

	Gateway         *gatewayFile   `yaml:"forwarding"`
	Mode            Mode           `yaml:"mode"`
	GRPC            *gRPCFile      `yaml:"grpc"`
	Health          healthFile     `yaml:"health"`
	Prometheus      prometheusFile `yaml:"prometheus"`
	Request         *requestFile   `yaml:"request"`
	Execution       *executionFile `yaml:"execution"`
	Batching        *batchingFile  `yaml:"batching"`
	Producer        *producerFile  `yaml:"producer"`
	Consumer        *consumerFile  `yaml:"consumer"`
	ShutdownTimeout *time.Duration `yaml:"shutdown_timeout"`
}

type gRPCFile struct {
	Address                string    `yaml:"address"`
	MaxReceiveMessageBytes *byteSize `yaml:"max_receive_message_bytes"`
	MaxSendMessageBytes    *byteSize `yaml:"max_send_message_bytes"`
}

type healthFile struct {
	Address string `yaml:"address"`
}

type prometheusFile struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
}

type requestFile struct {
	MaxOperations *int `yaml:"max_operations"`
}

type executionFile struct {
	Merge mergeFile `yaml:"merge"`
}

// Store files contain shared identity and dependency policy, never role tuning.
type storeFile struct {
	Name    string      `yaml:"name"`
	Storage storageFile `yaml:"storage"`
	Kafka   kafkaFile   `yaml:"kafka"`
}

type batchingFile struct {
	MaxWait       *time.Duration `yaml:"max_wait"`
	MaxOperations *int           `yaml:"max_operations"`
	MaxBytes      *byteSize      `yaml:"max_bytes"`
	Queue         batchQueueFile `yaml:"queue"`
}

type batchQueueFile struct {
	MaxOperations *int      `yaml:"max_operations"`
	MaxBytes      *byteSize `yaml:"max_bytes"`
}

type mergeFile struct {
	MaxAttempts *int    `yaml:"max_attempts"`
	Lua         luaFile `yaml:"lua"`
}

type luaFile struct {
	Timeout           *time.Duration `yaml:"timeout"`
	MaxSourceBytes    *byteSize      `yaml:"max_source_bytes"`
	MaxResultBytes    *byteSize      `yaml:"max_result_bytes"`
	MaxCachedPrograms *int           `yaml:"max_cached_programs"`
	MaxInstructions   *int           `yaml:"max_instructions"`
}

type storageFile struct {
	Driver  Driver      `yaml:"driver"`
	MongoDB mongoDBFile `yaml:"mongodb"`
	Search  searchFile  `yaml:"search"`
}

type mongoDBFile struct {
	URI           *string `yaml:"uri"`
	URIFile       *string `yaml:"uri_file"`
	MetadataField string  `yaml:"metadata_field"`
}

type searchFile struct {
	Endpoints    []string `yaml:"endpoints"`
	Username     *string  `yaml:"username"`
	UsernameFile *string  `yaml:"username_file"`
	Password     *string  `yaml:"password"`
	PasswordFile *string  `yaml:"password_file"`
	APIKey       *string  `yaml:"api_key"`
	APIKeyFile   *string  `yaml:"api_key_file"`
}

type kafkaFile struct {
	Enabled           bool      `yaml:"enabled"`
	Brokers           []string  `yaml:"brokers"`
	Partitions        *int      `yaml:"partitions"`
	ReplicationFactor *int      `yaml:"replication_factor"`
	MinInSyncReplicas *int      `yaml:"min_insync_replicas"`
	MaxRecordBytes    *byteSize `yaml:"max_record_bytes"`
	Topic             topicFile `yaml:"topic"`
	DeadLetter        topicFile `yaml:"dead_letter"`
}

type topicFile struct {
	Name      string         `yaml:"name"`
	Retention *time.Duration `yaml:"retention"`
}

type producerFile struct {
	MaxBufferedBytes *byteSize `yaml:"max_buffered_bytes"`
}

type consumerFile struct {
	GroupID           string         `yaml:"group_id"`
	MaxPollRecords    *int           `yaml:"max_poll_records"`
	ProcessingTimeout *time.Duration `yaml:"processing_timeout"`
	Retry             retryFile      `yaml:"retry"`
}

type retryFile struct {
	MaxAttempts *int           `yaml:"max_attempts"`
	Backoff     *time.Duration `yaml:"backoff"`
	MaxBackoff  *time.Duration `yaml:"max_backoff"`
}

type gatewayFile struct {
	DNSRefreshInterval *time.Duration `yaml:"dns_refresh_interval"`
	Routes             []Route        `yaml:"routes"`
	IdleTimeout        *time.Duration `yaml:"idle_timeout"`
	MaxConnections     *int           `yaml:"max_connections"`

	MaxFanout *int `yaml:"max_fanout"`
}

type memoryFile struct {
	MaxBytes             *byteSize `yaml:"max_bytes"`
	HighWatermarkPercent *int      `yaml:"high_watermark_percent"`
	LowWatermarkPercent  *int      `yaml:"low_watermark_percent"`
}

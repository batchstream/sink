package config

import "time"

// File types preserve omission so defaults can depend on other configured limits.
// Runtime code receives only resolved values through Config.
type configFile struct {
	Memory          memoryFile     `yaml:"memory"`
	Storage         *storageFile   `yaml:"storage"`
	Gateway         *gatewayFile   `yaml:"gateway"`
	Mode            Mode           `yaml:"mode"`
	GRPC            gRPCFile       `yaml:"grpc"`
	Health          healthFile     `yaml:"health"`
	Prometheus      prometheusFile `yaml:"prometheus"`
	Service         serviceFile    `yaml:"service"`
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

type serviceFile struct {
	Request   requestFile   `yaml:"request"`
	Execution executionFile `yaml:"execution"`
	Publish   publishFile   `yaml:"publish"`
	Batching  batchingFile  `yaml:"batching"`
	Merge     mergeFile     `yaml:"merge"`
}

type requestFile struct {
	Timeout       *time.Duration `yaml:"timeout"`
	MaxOperations *int           `yaml:"max_operations"`
	MaxReadBytes  *byteSize      `yaml:"max_read_bytes"`
}

type executionFile struct {
	MaxRequests *int               `yaml:"max_requests"`
	MaxBytes    *byteSize          `yaml:"max_bytes"`
	Queue       admissionQueueFile `yaml:"queue"`
	Scan        scanFile           `yaml:"scan"`
}

type admissionQueueFile struct {
	MaxRequests *int           `yaml:"max_requests"`
	MaxBytes    *byteSize      `yaml:"max_bytes"`
	MaxWait     *time.Duration `yaml:"max_wait"`
}

type scanFile struct {
	MaxRequests   *int           `yaml:"max_requests"`
	MaxBytes      *byteSize      `yaml:"max_bytes"`
	AdmissionWait *time.Duration `yaml:"admission_wait"`
}

type publishFile struct {
	MaxRequests *int      `yaml:"max_requests"`
	MaxBytes    *byteSize `yaml:"max_bytes"`
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
	Name    string      `yaml:"name"`
	Driver  Driver      `yaml:"driver"`
	MongoDB mongoDBFile `yaml:"mongodb"`
	Search  searchFile  `yaml:"search"`
	Kafka   kafkaFile   `yaml:"kafka"`
}

type mongoDBFile struct {
	URI                 *string `yaml:"uri"`
	URIFile             *string `yaml:"uri_file"`
	MetadataField       string  `yaml:"metadata_field"`
	MaxConcurrentWrites *int    `yaml:"max_concurrent_writes"`
	MaxConcurrentGroups *int    `yaml:"max_concurrent_groups"`
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
	Enabled    bool           `yaml:"enabled"`
	Brokers    []string       `yaml:"brokers"`
	Topic      topicFile      `yaml:"topic"`
	Producer   producerFile   `yaml:"producer"`
	Consumer   consumerFile   `yaml:"consumer"`
	DeadLetter deadLetterFile `yaml:"dead_letter"`
}

type topicFile struct {
	Name              string         `yaml:"name"`
	Partitions        *int           `yaml:"partitions"`
	ReplicationFactor *int           `yaml:"replication_factor"`
	Retention         *time.Duration `yaml:"retention"`
	MinInSyncReplicas *int           `yaml:"min_insync_replicas"`
	MaxRecordBytes    *byteSize      `yaml:"max_record_bytes"`
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

type deadLetterFile struct {
	Topic     string         `yaml:"topic"`
	Retention *time.Duration `yaml:"retention"`
}

type gatewayFile struct {
	MaxRequestsPerStore *int           `yaml:"max_requests_per_store"`
	DNSRefreshInterval  *time.Duration `yaml:"dns_refresh_interval"`
	Routes              []Route        `yaml:"routes"`
	IdleTimeout         *time.Duration `yaml:"idle_timeout"`
	MaxConnections      *int           `yaml:"max_connections"`
	MaxRequests         *int           `yaml:"max_requests"`
	MaxBytes            *byteSize      `yaml:"max_bytes"`
	MaxFanout           *int           `yaml:"max_fanout"`
}

type memoryFile struct {
	MaxBytes     *byteSize      `yaml:"max_bytes"`
	BurstPercent *int           `yaml:"burst_percent"`
	WaitTimeout  *time.Duration `yaml:"wait_timeout"`
}

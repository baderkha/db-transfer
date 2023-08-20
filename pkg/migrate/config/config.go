package config

// Config : configuration for the job
type Config[S any, T any] struct {
	MaxConcurrency  int `json:"max_concurrency"`
	BatchRecordSize int `json:"max_batch_record_size"`
	SourceConfig    S   `json:"source"`
	Target          T   `json:"target"`
}

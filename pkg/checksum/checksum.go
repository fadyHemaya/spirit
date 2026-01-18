package checksum

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
)

var (
	// Query template for row checksums
	queryTemplate = "SELECT CRC32(CONCAT(%s)) as row_checksum, CONCAT_WS(',', %s) as pk FROM %s WHERE %s"
)

type Checker interface {
	// Run performs the checksum operation.
	Run(ctx context.Context) error
	GetProgress() string
	StartTime() time.Time
	ExecTime() time.Duration
}

type CheckerConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          *slog.Logger
	FixDifferences  bool
	Watermark       string // optional; defines a watermark to start from
	MaxRetries      int
	Applier         applier.Applier // optional; indicates it is a distributed checker
	SampleRate      int             // optional; sample every Nth chunk (0 = disabled, check all chunks)
	WatermarkDate   string          // optional; only checksum rows with created_at >= this date (YYYY-MM-DD)
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          slog.Default(),
		FixDifferences:  false,
		MaxRetries:      3,
	}
}

// NewChecker creates a new checksum object.
func NewChecker(db *sql.DB, chunker table.Chunker, feed *repl.Client, config *CheckerConfig) (Checker, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbconfig must be non-nil")
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.Applier != nil {
		return &DistributedChecker{
			concurrency:    config.Concurrency,
			db:             db,
			feed:           feed,
			chunker:        chunker,
			dbConfig:       config.DBConfig,
			logger:         config.Logger,
			fixDifferences: config.FixDifferences,
			maxRetries:     config.MaxRetries,
			applier:        config.Applier,
		}, nil
	}
	return &SingleChecker{
		concurrency:    config.Concurrency,
		db:             db,
		feed:           feed,
		chunker:        chunker,
		dbConfig:       config.DBConfig,
		logger:         config.Logger,
		fixDifferences: config.FixDifferences,
		maxRetries:     config.MaxRetries,
		sampleRate:     config.SampleRate,
		watermarkDate:  config.WatermarkDate,
	}, nil
}

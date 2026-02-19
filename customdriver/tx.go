package customdriver

import (
	"database/sql/driver"
	"log/slog"
	"time"
)

var (
	_ driver.Tx = (*customTx)(nil)
)

type customTx struct {
	tx     driver.Tx
	logger *slog.Logger
}

func (t *customTx) Commit() error {
	start := time.Now()
	err := t.tx.Commit()
	duration := time.Since(start)

	if err != nil {
		t.logger.Error("transaction commit failed",
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		t.logger.Info("transaction committed",
			slog.Duration("duration", duration),
		)
	}

	return err
}

func (t *customTx) Rollback() error {
	start := time.Now()
	err := t.tx.Rollback()
	duration := time.Since(start)

	if err != nil {
		t.logger.Error("transaction rollback failed",
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		t.logger.Info("transaction rolled back",
			slog.Duration("duration", duration),
		)
	}

	return err
}

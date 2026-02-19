package customdriver

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"
)

var (
	_ driver.Stmt              = (*customStmt)(nil)
	_ driver.StmtExecContext   = (*customStmt)(nil)
	_ driver.StmtQueryContext  = (*customStmt)(nil)
	_ driver.NamedValueChecker = (*customStmt)(nil)
)

type customStmt struct {
	stmt   driver.Stmt
	logger *slog.Logger
	query  string
}

func (s *customStmt) Close() error {
	return s.stmt.Close()
}

func (s *customStmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s *customStmt) Exec(args []driver.Value) (driver.Result, error) {
	start := time.Now()
	result, err := s.stmt.Exec(args)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("stmt execution failed",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		s.logger.Info("stmt executed",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
		)
	}

	return result, err
}

func (s *customStmt) Query(args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	rows, err := s.stmt.Query(args)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("stmt query failed",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		s.logger.Info("stmt queried",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
		)
	}

	return rows, err
}

func (s *customStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmtExecCtx, ok := s.stmt.(driver.StmtExecContext)
	if !ok {
		// fallback
		dargs := make([]driver.Value, len(args))
		for i, nv := range args {
			dargs[i] = nv.Value
		}
		return s.Exec(dargs)
	}

	start := time.Now()
	result, err := stmtExecCtx.ExecContext(ctx, args)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("stmt execution failed",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		s.logger.Info("stmt executed",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
		)
	}

	return result, err
}

func (s *customStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stmtQueryCtx, ok := s.stmt.(driver.StmtQueryContext)
	if ok {
		// fallback
		dargs := make([]driver.Value, len(args))
		for i, nv := range args {
			dargs[i] = nv.Value
		}
		return s.Query(dargs)
	}
	start := time.Now()
	rows, err := stmtQueryCtx.QueryContext(ctx, args)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("stmt query context failed",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		s.logger.Info("stmt queried context ",
			slog.String("query", s.query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
		)
	}

	return rows, err
}

func (c *customStmt) CheckNamedValue(nv *driver.NamedValue) error {
	if checker, ok := c.stmt.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(nv)
	}

	return driver.ErrSkip
}

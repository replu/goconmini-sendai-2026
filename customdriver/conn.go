package customdriver

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"
)

var (
	_ driver.Conn               = (*customConn)(nil)
	_ driver.Pinger             = (*customConn)(nil)
	_ driver.SessionResetter    = (*customConn)(nil)
	_ driver.Validator          = (*customConn)(nil)
	_ driver.QueryerContext     = (*customConn)(nil)
	_ driver.ExecerContext      = (*customConn)(nil)
	_ driver.ConnPrepareContext = (*customConn)(nil)
	_ driver.ConnBeginTx        = (*customConn)(nil)
	_ driver.NamedValueChecker  = (*customConn)(nil)
)

type customConn struct {
	conn   driver.Conn
	logger *slog.Logger
}

func (c *customConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &customStmt{
		stmt:   stmt,
		logger: c.logger,
		query:  query,
	}, nil
}

func (c *customConn) Close() error {
	return c.conn.Close()
}

func (c *customConn) Begin() (driver.Tx, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return nil, err
	}

	c.logger.Info("transaction started")
	return &customTx{
		tx:     tx,
		logger: c.logger,
	}, nil
}

func (c *customConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if connCtx, ok := c.conn.(driver.ConnPrepareContext); ok {
		stmt, err := connCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return &customStmt{
			stmt:   stmt,
			logger: c.logger,
			query:  query,
		}, nil
	}
	return c.Prepare(query)
}

func (c *customConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var result driver.Result
	var err error

	start := time.Now()
	if execerCtx, ok := c.conn.(driver.ExecerContext); ok {
		result, err = execerCtx.ExecContext(ctx, query, args)
		// そのままerrを返してもErrSkipを返せるがその場合無駄にログがでる
		if err == driver.ErrSkip {
			c.logger.Warn("original driver does not support ExecerContext")
			return nil, driver.ErrSkip
		}
	} else {
		c.logger.Warn("original driver does not support ExecerContext")
		return nil, driver.ErrSkip
	}

	duration := time.Since(start)
	if err != nil {
		c.logger.Error("sql execution failed",
			slog.String("query", query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		c.logger.Info("sql executed",
			slog.String("query", query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
		)
	}

	return result, err
}

func (c *customConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var rows driver.Rows
	var err error

	start := time.Now()
	queryerCtx, ok := c.conn.(driver.QueryerContext)
	if ok {
		rows, err = queryerCtx.QueryContext(ctx, query, args)
		// そのままerrを返してもErrSkipを返せるがその場合無駄にログがでる
		if err == driver.ErrSkip {
			c.logger.Warn("original driver does not support QueryerContext")
			return nil, driver.ErrSkip
		}
	} else {
		c.logger.Warn("original driver does not support QueryerContext")
		return nil, driver.ErrSkip
	}

	duration := time.Since(start)
	if err != nil {
		c.logger.Error("sql query failed",
			slog.String("query", query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
	} else {
		c.logger.Info("sql queried",
			slog.String("query", query),
			slog.Any("args", args),
			slog.Duration("duration", duration),
		)
	}

	return rows, err
}

func (c *customConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connBeginTx, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err := connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		c.logger.Info("transaction started",
			"isolation", opts.Isolation,
			"read_only", opts.ReadOnly,
		)
		return &customTx{
			tx:     tx,
			logger: c.logger,
		}, nil
	}
	return c.Begin()
}

func (c *customConn) Ping(ctx context.Context) error {
	pinger, ok := c.conn.(driver.Pinger)
	if !ok {
		// ラップ元が Pinger を実装していない場合、軽量クエリで疎通確認
		var rows driver.Rows
		rows, err := c.QueryContext(ctx, "SELECT 1", nil)
		if err != nil {
			return err
		}

		return rows.Close()
	}

	return pinger.Ping(ctx)
}

func (c *customConn) ResetSession(ctx context.Context) error {
	if resetter, ok := c.conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}

	return nil
}

func (c *customConn) IsValid() bool {
	if validator, ok := c.conn.(driver.Validator); ok {
		return validator.IsValid()
	}

	// ラップ元が Validator を実装していない場合は有効とみなす
	return true
}

func (c *customConn) CheckNamedValue(nv *driver.NamedValue) error {
	if checker, ok := c.conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(nv)
	}

	return driver.ErrSkip
}

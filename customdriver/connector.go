package customdriver

import (
	"context"
	"database/sql/driver"
	"log/slog"
)

var (
	_ driver.Connector = (*CustomConnector)(nil)
)

type CustomConnector struct {
	connector driver.Connector
	driver    *CustomDriver
	logger    *slog.Logger
}

func NewCustomConnector(connector driver.Connector, logger *slog.Logger) *CustomConnector {
	return &CustomConnector{
		connector: connector,
		driver: &CustomDriver{
			driver: connector.Driver(),
			logger: logger,
		},
		logger: logger,
	}
}

func (cc *CustomConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := cc.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return &customConn{
		conn:   conn,
		logger: cc.logger,
	}, nil
}

func (cc *CustomConnector) Driver() driver.Driver {
	return cc.driver
}

// DriverContext 未対応ドライバー向けのフォールバック
type dsnConnector struct {
	dsn    string
	driver *CustomDriver
}

func (c *dsnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}

func (c *dsnConnector) Driver() driver.Driver {
	return c.driver
}

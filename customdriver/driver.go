package customdriver

import (
	"database/sql/driver"
	"log/slog"
)

var (
	_ driver.Driver        = (*CustomDriver)(nil)
	_ driver.DriverContext = (*CustomDriver)(nil)
)

type CustomDriver struct {
	driver driver.Driver
	logger *slog.Logger
}

func NewCustomDriver(drv driver.Driver, logger *slog.Logger) *CustomDriver {
	return &CustomDriver{
		driver: drv,
		logger: logger,
	}
}

func (d *CustomDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.driver.Open(name)
	if err != nil {
		return nil, err
	}

	return &customConn{
		conn:   conn,
		logger: d.logger,
	}, nil
}

// 内部ドライバーが DriverContext をサポートする場合はその OpenConnector に委譲し、
// サポートしない場合は dsnConnector フォールバックを返す
func (d *CustomDriver) OpenConnector(name string) (driver.Connector, error) {
	if dc, ok := d.driver.(driver.DriverContext); ok {
		connector, err := dc.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &CustomConnector{
			connector: connector,
			driver:    d,
			logger:    d.logger,
		}, nil
	}

	return &dsnConnector{
		dsn:    name,
		driver: d,
	}, nil
}

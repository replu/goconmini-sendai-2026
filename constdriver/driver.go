package constdriver

import (
	"database/sql"
	"database/sql/driver"
)

var (
	_ driver.Driver = (*Driver)(nil)
)

type Driver struct {
}

func init() {
	sql.Register("const-driver", &Driver{})
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	return &Conn{}, nil
}

package constdriver

import (
	"database/sql/driver"
)

var (
	_ driver.Conn = (*Conn)(nil)
)

type Conn struct {
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{}, nil
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, nil
}

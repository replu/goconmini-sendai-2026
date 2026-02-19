package constdriver

import (
	"database/sql/driver"
)

var (
	_ driver.Stmt = (*Stmt)(nil)
)

type Stmt struct {
}

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return 1
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return &Rows{
		index: 0,
		data: [][]driver.Value{
			{1, "Alice"},
			{2, "Bob"},
		},
	}, nil
}

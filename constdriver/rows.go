package constdriver

import (
	"database/sql/driver"
	"io"
)

var (
	_ driver.Rows = (*Rows)(nil)
)

type Rows struct {
	index int
	data  [][]driver.Value
}

func (r *Rows) Columns() []string {
	return []string{"id", "name"}
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.index])
	r.index++
	return nil
}

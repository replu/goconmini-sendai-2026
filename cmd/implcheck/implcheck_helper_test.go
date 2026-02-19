package implcheck_test

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

func check[T any](t *testing.T, v any) {
	t.Helper()
	var zero T
	iface := fmt.Sprintf("%T", &zero)[1:]
	if _, ok := v.(T); !ok {
		t.Logf("%T does NOT implement %s", v, iface)
	} else {
		t.Logf("%T implements %s", v, iface)
	}
}

func driverCheck(t *testing.T, d driver.Driver) {
	t.Helper()

	check[driver.Driver](t, d)
	check[driver.DriverContext](t, d)
}

func connectorCheck(t *testing.T, c driver.Connector) {
	t.Helper()

	check[driver.Connector](t, c)
}

func connCheck(t *testing.T, c driver.Conn) {
	t.Helper()

	check[driver.Conn](t, c)
	check[driver.ConnBeginTx](t, c)
	check[driver.ConnPrepareContext](t, c)
	check[driver.Execer](t, c)
	check[driver.ExecerContext](t, c)
	check[driver.Queryer](t, c)
	check[driver.QueryerContext](t, c)
	check[driver.QueryerContext](t, c)
	check[driver.Pinger](t, c)
	check[driver.SessionResetter](t, c)
	check[driver.Validator](t, c)
	check[driver.NamedValueChecker](t, c)
}

func stmtCheck(t *testing.T, s driver.Stmt) {
	t.Helper()

	check[driver.Stmt](t, s)
	check[driver.StmtExecContext](t, s)
	check[driver.StmtQueryContext](t, s)
	check[driver.ColumnConverter](t, s)
	check[driver.NamedValueChecker](t, s)
}

func rowsCheck(t *testing.T, r driver.Rows) {
	t.Helper()

	check[driver.Rows](t, r)
	check[driver.RowsNextResultSet](t, r)
	check[driver.RowsColumnTypeDatabaseTypeName](t, r)
	check[driver.RowsColumnTypeLength](t, r)
	check[driver.RowsColumnTypeNullable](t, r)
	check[driver.RowsColumnTypePrecisionScale](t, r)
	check[driver.RowsColumnTypeScanType](t, r)
}

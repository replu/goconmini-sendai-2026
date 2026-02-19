package implcheck_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var mysqlConnector driver.Connector

func setupMySQL(pool *dockertest.Pool) func() {
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "8.0",
		Env: []string{
			"MYSQL_ROOT_PASSWORD=test",
			"MYSQL_DATABASE=testdb",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start mysql: %s", err)
	}

	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Passwd = "test"
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("localhost:%s", resource.GetPort("3306/tcp"))
	cfg.DBName = "testdb"

	mysqlConnector, err = mysql.NewConnector(cfg)
	if err != nil {
		log.Fatalf("Could not create mysql connector: %s", err)
	}

	err = pool.Retry(func() error {
		conn, err := mysqlConnector.Connect(context.Background())
		if err != nil {
			return err
		}
		return conn.Close()
	})
	if err != nil {
		log.Fatalf("Could not connect to mysql: %s", err)
	}

	return func() { _ = pool.Purge(resource) }
}

func TestMySQL_Driver(t *testing.T) {
	d := mysqlConnector.Driver()

	driverCheck(t, d)
}

func TestMySQL_Connector(t *testing.T) {
	connectorCheck(t, mysqlConnector)
}

func TestMySQL_Conn(t *testing.T) {
	conn, err := mysqlConnector.Connect(context.Background())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	connCheck(t, conn)
}

func TestMySQL_Stmt(t *testing.T) {
	conn, err := mysqlConnector.Connect(context.Background())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}
	defer stmt.Close()

	stmtCheck(t, stmt)
}

func TestMySQL_BinaryRows(t *testing.T) {
	conn, err := mysqlConnector.Connect(context.Background())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	rowsCheck(t, rows)
}

func TestMySQL_TestRows(t *testing.T) {
	conn, err := mysqlConnector.Connect(context.Background())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	c, ok := conn.(driver.Queryer)
	if !ok {
		t.Fatalf("failed to cast Queryer")
	}
	rows, err := c.Query("SELECT 1", []driver.Value{})
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	rowsCheck(t, rows)
}

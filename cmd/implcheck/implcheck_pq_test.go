package implcheck_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"testing"

	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var pqConnector driver.Connector

func setupPQ(pool *dockertest.Pool) func() {
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=test",
			"POSTGRES_DB=testdb",
			"listen_addresses='*'",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start postgres: %s", err)
	}

	dsn := fmt.Sprintf("host=localhost port=%s user=postgres password=test dbname=testdb sslmode=disable", resource.GetPort("5432/tcp"))
	pqConnector, err = pq.NewConnector(dsn)
	if err != nil {
		log.Fatalf("Could not create pq connector: %s", err)
	}

	err = pool.Retry(func() error {
		conn, err := pqConnector.Connect(context.Background())
		if err != nil {
			return err
		}
		return conn.Close()
	})
	if err != nil {
		log.Fatalf("Could not connect to postgres: %s", err)
	}

	return func() { _ = pool.Purge(resource) }
}

func TestPQ_Driver(t *testing.T) {
	d := pqConnector.Driver()

	driverCheck(t, d)
}

func TestPQ_Connector(t *testing.T) {
	connectorCheck(t, pqConnector)
}

func TestPQ_Conn(t *testing.T) {
	conn, err := pqConnector.Connect(context.Background())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	connCheck(t, conn)
}

func TestPQ_Stmt(t *testing.T) {
	conn, err := pqConnector.Connect(context.Background())
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

func TestPQ_Rows(t *testing.T) {
	conn, err := pqConnector.Connect(context.Background())
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

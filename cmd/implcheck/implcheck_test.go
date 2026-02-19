package implcheck_test

import (
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}
	if err := pool.Client.Ping(); err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	purgeMySQL := setupMySQL(pool)
	purgePQ := setupPQ(pool)

	code := m.Run()

	purgeMySQL()
	purgePQ()

	os.Exit(code)
}

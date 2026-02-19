package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/lib/pq"
	"github.com/replu/goconmini-sendai-2026/customdriver"
	"github.com/replu/goconmini-sendai-2026/sqlc/postgresqlquery"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	connector, err := pq.NewConnector("host=localhost port=55432 user=postgres password=postgres dbname=goconmini2026 sslmode=disable")
	if err != nil {
		logger.Error(err.Error())
		return
	}
	db := sql.OpenDB(customdriver.NewCustomConnector(connector, logger))

	queries := postgresqlquery.New(db)
	res, err := queries.GetUserByName(ctx, "Alice")
	if err != nil {
		logger.Error(err.Error())
		return
	}

	logger.Info(fmt.Sprintf("id: %d, name: %q", res.ID, res.Name))
}

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/replu/goconmini-sendai-2026/customdriver"
	"github.com/replu/goconmini-sendai-2026/sqlc/mysqlquery"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	connector, err := mysql.MySQLDriver{}.OpenConnector("root:root@tcp(localhost:43306)/goconmini2026?parseTime=true")
	if err != nil {
		logger.Error(err.Error())
		return
	}
	db := sql.OpenDB(customdriver.NewCustomConnector(connector, logger))

	queries := mysqlquery.New(db)
	res, err := queries.GetUserByName(ctx, "Alice")
	if err != nil {
		logger.Error(err.Error())
		return
	}

	logger.Info(fmt.Sprintf("id: %d, name: %q", res.ID, res.Name))
}

package test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/go-sql-driver/mysql"
)

func BenchmarkConnectMySQL(b *testing.B) {
	db, err := sql.Open("mysql",
		"root:123@tcp(127.0.0.1:3306)/test")
	if err != nil {
		b.Fatalf("mysql connect error : %s", err.Error())
		return
	}

	ctx := context.Background()

	db.SetMaxIdleConns(0)
	b.ResetTimer()

	b.Run("noConnPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = db.ExecContext(ctx, "SELECT 1")
		}
	})

	db.SetMaxIdleConns(1)
	b.ResetTimer()

	b.Run("hasConnPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = db.ExecContext(ctx, "SELECT 1")
		}
	})
	stats := db.Stats()
	spew.Dump(stats)
}

package testutils

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/castaneai/spadmin"
	"github.com/castaneai/spankeys"

	"cloud.google.com/go/spanner"
)

func SelectOne(ctx context.Context, sql string, c *spanner.Client, ptr interface{}) error {
	stmt := spanner.NewStatement(sql)
	iter := c.Single().Query(ctx, stmt)
	defer iter.Stop()
	r, err := iter.Next()
	if err != nil {
		return err
	}
	return r.Column(0, ptr)
}

func CountsRow(ctx context.Context, sql string, c *spanner.Client) (int64, error) {
	stmt := spanner.NewStatement(sql)
	iter := c.Single().Query(ctx, stmt)
	defer iter.Stop()
	r, err := iter.Next()
	if err != nil {
		return 0, err
	}
	var cnt int64
	if err := r.Column(0, &cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func PrepareDatabase(ctx context.Context, ddls []string) error {
	dsn, err := makeDSNFromEnv()
	if err != nil {
		return err
	}
	admin, err := spadmin.NewClient(dsn.Parent())
	if err != nil {
		return err
	}
	exists, err := admin.DatabaseExists(ctx, dsn.DatabaseID())
	if err != nil {
		return err
	}
	if exists {
		if err := admin.DropDatabase(ctx, dsn.DatabaseID()); err != nil {
			return err
		}
	}
	if err := admin.CreateDatabase(ctx, dsn.DatabaseID(), ddls); err != nil {
		return err
	}
	return nil
}

func makeDSNFromEnv() (spankeys.DSN, error) {
	projectID := os.Getenv("SPANNER_PROJECT_ID")
	if projectID == "" {
		return "", errors.New("env: SPANNER_PROJECT_ID not set")
	}
	instance := os.Getenv("SPANNER_INSTANCE_ID")
	if instance == "" {
		return "", errors.New("env: SPANNER_INSTANCE_ID not set")
	}
	database := os.Getenv("SPANNER_DATABASE_ID")
	if database == "" {
		return "", errors.New("env: SPANNER_DATABASE_ID not set")
	}
	return spankeys.DSN(fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database)), nil
}

func NewSpannerClient(ctx context.Context) (*spanner.Client, error) {
	dsn, err := makeDSNFromEnv()
	if err != nil {
		return nil, err
	}
	return spanner.NewClient(ctx, string(dsn))
}

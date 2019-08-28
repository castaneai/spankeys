package testutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/castaneai/spadmin"

	"cloud.google.com/go/spanner"
)

func PrepareDatabase(ctx context.Context, ddls []string) error {
	projectID := os.Getenv("SPANNER_TEST_PROJECT_ID")
	if projectID == "" {
		return errors.New("env: SPANNER_TEST_PROJECT_ID not set")
	}
	instance := os.Getenv("SPANNER_TEST_INSTANCE")
	if instance == "" {
		return errors.New("env: SPANNER_TEST_INSTANCE not set")
	}
	database := os.Getenv("SPANNER_TEST_DATABASE")
	if database == "" {
		return errors.New("env: SPANNER_TEST_DATABASE not set")
	}
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database)
	dsnParent := strings.Join(strings.Split(dsn, "/")[:4], "/")

	admin, err := spadmin.NewClient(dsnParent)
	if err != nil {
		return err
	}
	exists, err := admin.DatabaseExists(ctx, database)
	if err != nil {
		return err
	}
	if exists {
		if err := admin.DropDatabase(ctx, database); err != nil {
			return err
		}
	}
	if err := admin.CreateDatabase(ctx, database, ddls); err != nil {
		return err
	}
	return nil
}

func NewSpannerClient(ctx context.Context) (*spanner.Client, error) {
	projectID := os.Getenv("SPANNER_TEST_PROJECT_ID")
	instance := os.Getenv("SPANNER_TEST_INSTANCE")
	database := os.Getenv("SPANNER_TEST_DATABASE")
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database)
	return spanner.NewClient(ctx, dsn)
}

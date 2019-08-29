package spankeys_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/castaneai/spankeys"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"

	"cloud.google.com/go/spanner"

	"github.com/castaneai/spankeys/testutils"
)

func TestPartitionsKeySets(t *testing.T) {
	tableName := "PartitioningTest"

	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{fmt.Sprintf(`
CREATE TABLE %s (
    ID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID)
`, tableName), fmt.Sprintf(`
CREATE INDEX %s_Name ON %s(Name)
`, tableName, tableName)}); err != nil {
		t.Fatal(err)
	}

	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if _, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var ms []*spanner.Mutation
			for i := 0; i < 5000; i++ {
				id := uuid.Must(uuid.NewRandom()).String()
				name := uuid.Must(uuid.NewRandom()).String()
				ms = append(ms, spanner.Insert(tableName, []string{"ID", "Name"}, []interface{}{id, name}))
			}
			return tx.BufferWrite(ms)
		}); err != nil {
			t.Fatal(err)
		}
	}

	{
		cnt, err := testutils.CountsRow(ctx, fmt.Sprintf("select count(*) from %s", tableName), c)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(5*5000), cnt)
	}

	pkCols, err := spankeys.GetPrimaryKeyColumns(ctx, c, tableName)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(pkCols))
	assert.Equal(t, "ID", pkCols[0].Name)

	// TODO: why 19998 not 20000?
	keysets, err := spankeys.PartitionsKeySets(ctx, c, tableName, pkCols, 20000-2)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(keysets))

	// delete all rows by partitioned keysets
	for _, ks := range keysets {
		if _, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var ms []*spanner.Mutation
			ms = append(ms, spanner.Delete(tableName, ks))
			return tx.BufferWrite(ms)
		}); err != nil {
			t.Fatal(err)
		}
	}

	{
		cnt, err := testutils.CountsRow(ctx, fmt.Sprintf("select count(*) from %s", tableName), c)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), cnt)
	}
}

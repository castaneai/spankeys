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

	// total mutations (20000) = KeyRange 1 mutation + 19999 indexed rows
	keysets, err := spankeys.PartitionsKeyRanges(ctx, c, tableName, pkCols, 19999, 100000)
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

func TestPartitionsCompositeKeySets(t *testing.T) {
	tableName := "CompositePartitioningTest"

	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{fmt.Sprintf(`
CREATE TABLE %s (
    ID1 STRING(36) NOT NULL,
    ID2 STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID1, ID2)
`, tableName)}); err != nil {
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
				id1 := uuid.Must(uuid.NewRandom()).String()
				id2 := uuid.Must(uuid.NewRandom()).String()
				name := uuid.Must(uuid.NewRandom()).String()
				ms = append(ms, spanner.Insert(tableName, []string{"ID1", "ID2", "Name"}, []interface{}{id1, id2, name}))
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
	assert.Equal(t, 2, len(pkCols))
	assert.Equal(t, "ID1", pkCols[0].Name)
	assert.Equal(t, "ID2", pkCols[1].Name)

	// total mutations (20000) = KeyRange 1 mutation + 19999 indexed rows
	// for delete ranges, keys only first key part, so using pkCols[:1]
	keysets, err := spankeys.PartitionsKeyRanges(ctx, c, tableName, pkCols[:1], 19999, 100000)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(keysets))
	assert.Equal(t, int64(19999), keysets[0].RowCount)
	assert.Equal(t, int64(5001), keysets[1].RowCount)

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

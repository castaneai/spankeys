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

	mutationBatchSize, err := spankeys.CalcMutationBatchSize(ctx, c, tableName)
	if err != nil {
		t.Fatal(err)
	}
	keysets, err := spankeys.PartitionsKeyRanges(ctx, c, tableName, pkCols, mutationBatchSize, 100000)
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

	mutationBatchSize, err := spankeys.CalcMutationBatchSize(ctx, c, tableName)
	if err != nil {
		t.Fatal(err)
	}
	keysets, err := spankeys.PartitionsKeyRanges(ctx, c, tableName, pkCols[:1], mutationBatchSize, 100000)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(keysets))

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

func TestPartitionsInterleavedKeySets(t *testing.T) {
	parentTableName := "PartitioningTestParent"
	childTableName := "PartitioningTestChild"

	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{fmt.Sprintf(`
CREATE TABLE %s (
    ParentID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ParentID)
`, parentTableName), fmt.Sprintf(`
CREATE INDEX %s_Name ON %s(Name)
`, parentTableName, parentTableName), fmt.Sprintf(`
CREATE TABLE %s (
    ParentID STRING(36) NOT NULL,
    ChildID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ParentID, ChildID), INTERLEAVE IN PARENT %s ON DELETE CASCADE
`, childTableName, parentTableName), fmt.Sprintf(`
CREATE INDEX %s_Name ON %s(Name)
`, childTableName, childTableName)}); err != nil {
		t.Fatal(err)
	}

	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var ms []*spanner.Mutation
			for i := 0; i < 2500; i++ {
				parentID := uuid.Must(uuid.NewRandom()).String()
				childID := uuid.Must(uuid.NewRandom()).String()
				name := uuid.Must(uuid.NewRandom()).String()
				ms = append(ms, spanner.Insert(parentTableName, []string{"ParentID", "Name"}, []interface{}{parentID, name}))
				ms = append(ms, spanner.Insert(childTableName, []string{"ParentID", "ChildID", "Name"}, []interface{}{parentID, childID, name}))
			}
			return tx.BufferWrite(ms)
		}); err != nil {
			t.Fatal(err)
		}
	}

	{
		cnt, err := testutils.CountsRow(ctx, fmt.Sprintf("select count(*) from %s", parentTableName), c)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(5*5000), cnt)
	}

	{
		cnt, err := testutils.CountsRow(ctx, fmt.Sprintf("select count(*) from %s", childTableName), c)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(5*5000), cnt)
	}

	pkCols, err := spankeys.GetPrimaryKeyColumns(ctx, c, parentTableName)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(pkCols))
	assert.Equal(t, "ParentID", pkCols[0].Name)

	// total mutations (20000) = KeyRange 1 mutation + 19999 indexed rows
	mutationBatchSize, err := spankeys.CalcMutationBatchSize(ctx, c, parentTableName)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mutation batchsize: %d", mutationBatchSize)
	// for delete ranges, keys only first key part, so using pkCols[:1]
	keysets, err := spankeys.PartitionsKeyRanges(ctx, c, parentTableName, pkCols[:1], mutationBatchSize, 100000)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(keysets))

	// delete all rows by partitioned keysets
	for _, ks := range keysets {
		if _, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var ms []*spanner.Mutation
			ms = append(ms, spanner.Delete(parentTableName, ks))
			return tx.BufferWrite(ms)
		}); err != nil {
			t.Fatal(err)
		}
	}

	{
		cnt, err := testutils.CountsRow(ctx, fmt.Sprintf("select count(*) from %s", parentTableName), c)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), cnt)
	}
	{
		cnt, err := testutils.CountsRow(ctx, fmt.Sprintf("select count(*) from %s", childTableName), c)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), cnt)
	}
}

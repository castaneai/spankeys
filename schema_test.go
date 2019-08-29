package spankeys_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/castaneai/spankeys/testutils"

	"github.com/castaneai/spankeys"
)

func TestGetPrimaryKeyColumns(t *testing.T) {
	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{`
CREATE TABLE SinglePK (
    ID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID)
`, `
CREATE TABLE CompositePK (
    ID1 STRING(36) NOT NULL,
	ID2 STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID1, ID2)
`, `
CREATE TABLE InterleavedPK (
    ID STRING(36) NOT NULL,
	ChildID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID, ChildID),
INTERLEAVE IN PARENT SinglePK ON DELETE CASCADE
`}); err != nil {
		t.Fatal(err)
	}

	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		pks, err := spankeys.GetPrimaryKeyColumns(ctx, c, "SinglePK")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 1, len(pks))
		assert.Equal(t, "ID", pks[0].Name)
	}

	{
		pks, err := spankeys.GetPrimaryKeyColumns(ctx, c, "CompositePK")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(pks))
		assert.Equal(t, "ID1", pks[0].Name)
		assert.Equal(t, "ID2", pks[1].Name)
	}

	{
		pks, err := spankeys.GetPrimaryKeyColumns(ctx, c, "InterleavedPK")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(pks))
		assert.Equal(t, "ID", pks[0].Name)
		assert.Equal(t, "ChildID", pks[1].Name)
	}
}

func TestGetIndexes(t *testing.T) {
	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{`
CREATE TABLE SinglePK (
    ID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID)
`, `
CREATE TABLE CompositePK (
    ID1 STRING(36) NOT NULL,
	ID2 STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID1, ID2)
`, `
CREATE TABLE InterleavedPK (
    ID STRING(36) NOT NULL,
	ChildID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID, ChildID),
INTERLEAVE IN PARENT SinglePK ON DELETE CASCADE
`, `
CREATE INDEX SinglePK_Name ON SinglePK(Name)
`}); err != nil {
		t.Fatal(err)
	}

	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	idxs, err := spankeys.GetIndexes(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, true, len(idxs) > 0)

	for _, idx := range idxs {
		if idx.Name == "SinglePK_Name" {
			assert.Equal(t, 1, len(idx.Columns))
			assert.Equal(t, "Name", idx.Columns[0].Name)
			assert.Equal(t, false, idx.Columns[0].IsNullable)
		}
	}
}

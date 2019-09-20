package spankeys_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/castaneai/spankeys/testutils"

	"github.com/castaneai/spankeys"
)

func TestGetTables(t *testing.T) {
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

	ts, err := spankeys.GetTables(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(ts))
	for _, tbl := range ts {
		if tbl.Name == "SinglePK" {
			assert.Nil(t, tbl.Interleave)
		}
		if tbl.Name == "CompositePK" {
			assert.Nil(t, tbl.Interleave)
		}
		if tbl.Name == "InterleavedPK" {
			assert.Equal(t, "SinglePK", tbl.Interleave.Table)
		}
	}
}

func TestGetColumns(t *testing.T) {
	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{`
CREATE TABLE TestGetColumns (
    ID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
	Age INT64 NOT NULL,
) PRIMARY KEY (ID)
`}); err != nil {
		t.Fatal(err)
	}

	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cols, err := spankeys.GetColumns(ctx, c, "TestGetColumns")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(cols))
	assert.Equal(t, "ID", cols[0].Name)
	assert.Equal(t, int64(1), cols[0].OrdinalPosition)
	assert.Equal(t, "Name", cols[1].Name)
	assert.Equal(t, int64(2), cols[1].OrdinalPosition)
	assert.Equal(t, "Age", cols[2].Name)
	assert.Equal(t, int64(3), cols[2].OrdinalPosition)
}

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
		assert.Equal(t, int64(1), pks[0].OrdinalPosition)
		assert.Equal(t, "ID2", pks[1].Name)
		assert.Equal(t, int64(2), pks[1].OrdinalPosition)
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
CREATE INDEX SinglePK_Name ON SinglePK(Name)
`, `
CREATE TABLE CompositePK (
    ID1 STRING(36) NOT NULL,
	ID2 STRING(36) NOT NULL,
    Name STRING(255),
) PRIMARY KEY (ID1, ID2)
`, `
CREATE NULL_FILTERED INDEX CompositePK_Name ON CompositePK(Name)
`, `
CREATE TABLE InterleavedPK (
    ID STRING(36) NOT NULL,
	ChildID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ID, ChildID),
INTERLEAVE IN PARENT SinglePK ON DELETE CASCADE
`, `
CREATE UNIQUE INDEX InterleavedPK_ID_Name ON InterleavedPK(ID, Name), INTERLEAVE IN SinglePK
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
	assert.Equal(t, 6, len(idxs))

	for _, idx := range idxs {
		if idx.Name == "SinglePK_Name" {
			assert.Equal(t, 1, len(idx.Columns))
			assert.Equal(t, "", idx.ParentTable)
			assert.Equal(t, false, idx.IsUnique)
			assert.Equal(t, false, idx.IsNullFiltered)
			assert.Equal(t, false, idx.IsPrimaryKey)
			assert.Equal(t, "Name", idx.Columns[0].Name)
		} else if idx.Name == "CompositePK_Name" {
			assert.Equal(t, 1, len(idx.Columns))
			assert.Equal(t, "", idx.ParentTable)
			assert.Equal(t, false, idx.IsUnique)
			assert.Equal(t, true, idx.IsNullFiltered)
			assert.Equal(t, false, idx.IsPrimaryKey)
			assert.Equal(t, "Name", idx.Columns[0].Name)
		} else if idx.Name == "InterleavedPK_ID_Name" {
			assert.Equal(t, 2, len(idx.Columns))
			assert.Equal(t, "SinglePK", idx.ParentTable)
			assert.Equal(t, true, idx.IsUnique)
			assert.Equal(t, false, idx.IsNullFiltered)
			assert.Equal(t, false, idx.IsPrimaryKey)
			assert.Equal(t, "ID", idx.Columns[0].Name)
			assert.Equal(t, "Name", idx.Columns[1].Name)
		} else if idx.Table == "SinglePK" && idx.IsPrimaryKey {
			assert.Equal(t, 1, len(idx.Columns))
			assert.Equal(t, "", idx.ParentTable)
			assert.Equal(t, true, idx.IsPrimaryKey)
			assert.Equal(t, "ID", idx.Columns[0].Name)
		} else if idx.Table == "CompositePK" && idx.IsPrimaryKey {
			assert.Equal(t, 2, len(idx.Columns))
			assert.Equal(t, "", idx.ParentTable)
			assert.Equal(t, true, idx.IsPrimaryKey)
			assert.Equal(t, "ID1", idx.Columns[0].Name)
			assert.Equal(t, "ID2", idx.Columns[1].Name)
		} else if idx.Table == "InterleavedPK" && idx.IsPrimaryKey {
			assert.Equal(t, 2, len(idx.Columns))
			assert.Equal(t, "", idx.ParentTable)
			assert.Equal(t, true, idx.IsPrimaryKey)
			assert.Equal(t, "ID", idx.Columns[0].Name)
			assert.Equal(t, "ChildID", idx.Columns[1].Name)
		}
	}
}

func TestGetInterleaveChildren(t *testing.T) {
	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, []string{`
CREATE TABLE Parent (
    ParentID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ParentID)
`, `
CREATE TABLE ChildA (
    ParentID STRING(36) NOT NULL,
	ChildAID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ParentID, ChildAID),
INTERLEAVE IN PARENT Parent ON DELETE CASCADE
`, `
CREATE TABLE ChildAA (
    ParentID STRING(36) NOT NULL,
	ChildAID STRING(36) NOT NULL,
	ChildAAID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ParentID, ChildAID, ChildAAID),
INTERLEAVE IN PARENT ChildA ON DELETE NO ACTION 
`, `
CREATE TABLE ChildB (
    ParentID STRING(36) NOT NULL,
	ChildBID STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
) PRIMARY KEY (ParentID, ChildBID),
INTERLEAVE IN PARENT Parent ON DELETE CASCADE
`}); err != nil {
		t.Fatal(err)
	}

	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		is, err := spankeys.GetInterleaveChildren(ctx, c, "Parent")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 2, len(is))
		assert.Equal(t, "ChildA", is[0].Table)
		assert.Equal(t, spankeys.OnDeleteCascade, is[0].OnDelete)
		assert.Equal(t, "ChildB", is[1].Table)
		assert.Equal(t, spankeys.OnDeleteCascade, is[1].OnDelete)
	}

	{
		is, err := spankeys.GetInterleaveChildren(ctx, c, "ChildA")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 1, len(is))
		assert.Equal(t, "ChildAA", is[0].Table)
		assert.Equal(t, spankeys.OnDeleteNoAction, is[0].OnDelete)
	}

	{
		is, err := spankeys.GetInterleaveChildren(ctx, c, "ChildB")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 0, len(is))
	}
}

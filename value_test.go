package spankeys_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/civil"

	"github.com/stretchr/testify/assert"

	"github.com/castaneai/spankeys"

	"github.com/castaneai/spankeys/testutils"

	"cloud.google.com/go/spanner"
)

func TestDecodeToInterface(t *testing.T) {
	ctx := context.Background()
	if err := testutils.PrepareDatabase(ctx, nil); err != nil {
		t.Fatal(err)
	}
	c, err := testutils.NewSpannerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// boolean
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, "select true", c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, true, v.(bool))
	}

	// int64
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, "select 12345", c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(12345), v.(int64))
	}

	// float64
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, "select 123.45", c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(123.45), v.(float64))
	}

	// string
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, `select "Hello"`, c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "Hello", v.(string))
	}

	// date
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, `select PARSE_DATE("%x", "12/25/08")`, c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		d, err := civil.ParseDate("2008-12-25")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, d, v.(civil.Date))
	}

	// timestamp
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, `select TIMESTAMP("2008-12-25T15:30:00+00:00")`, c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		ts, err := time.ParseInLocation(time.RFC3339, "2008-12-25T15:30:00+00:00", nil)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, ts, v.(time.Time))
	}

	// bytes
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, `select CODE_POINTS_TO_BYTES([65, 98, 67, 100])`, c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, []byte("AbCd"), v.([]byte))
	}

	// nullable values
	{
		var gcv spanner.GenericColumnValue
		if err := testutils.SelectOne(ctx, `select NULL`, c, &gcv); err != nil {
			t.Fatal(err)
		}
		var v interface{}
		if err := spankeys.DecodeToInterface(&gcv, &v); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, spanner.NullInt64{}, v)
	}

	// TODO: array, struct
}
